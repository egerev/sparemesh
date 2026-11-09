"""Package committed Git objects without checking out or executing repository code."""

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import unicodedata


MAX_ENTRIES = 10_000  # Files and directories, excluding the fixed archive root.
MAX_BLOB_BYTES = 16 * 1024 * 1024
MAX_SOURCE_BYTES = 128 * 1024 * 1024
MAX_ARCHIVE_BYTES = 192 * 1024 * 1024
MAX_PATH_BYTES = 4096
MAX_COMMIT_BYTES = 1024 * 1024
MAX_TREE_BYTES = 64 * 1024 * 1024
ROOT_PREFIX = "source/"


class SourceBundleError(Exception):
    """The requested committed source cannot be packaged safely."""


@dataclass(frozen=True)
class _Entry:
    path: str
    object_id: str
    mode: int


@contextmanager
def _git_process(repository: Path, *arguments: str, input_pipe: bool = False):
    # Ignore caller Git overrides, replacements and optional worktree locks. Object
    # reads never need hooks, filters, a working tree, or network object acquisition.
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update({
        "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_NO_REPLACE_OBJECTS": "1", "GIT_OPTIONAL_LOCKS": "0",
        "GIT_NO_LAZY_FETCH": "1", "GIT_TERMINAL_PROMPT": "0",
    })
    command = ["git", "--no-pager", "--no-replace-objects", "-c", "core.fsmonitor=false",
               "-c", "core.hooksPath=" + os.devnull, "-c", "protocol.allow=never",
               "-C", str(repository), *arguments]
    with tempfile.TemporaryFile() as errors:
        try:
            process = subprocess.Popen(command, stdin=subprocess.PIPE if input_pipe else subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=errors, env=environment)
        except OSError as error:
            raise SourceBundleError(f"cannot start git: {error}") from error
        try:
            yield process, errors
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdout.close()
            if process.stdin is not None:
                process.stdin.close()


def _finish_git(process, errors):
    try:
        returncode = process.wait(timeout=30)
    except subprocess.TimeoutExpired as error:
        raise SourceBundleError("git object read timed out") from error
    if returncode:
        errors.seek(0)
        detail = errors.read(2048).decode("utf-8", errors="replace").strip()
        raise SourceBundleError(f"cannot read repository objects: {detail or 'git failed'}")


def _git_value(repository: Path, *arguments: str) -> str:
    with _git_process(repository, *arguments) as (process, errors):
        value = process.stdout.read(4097)
        if len(value) > 4096:
            raise SourceBundleError("unexpectedly large git response")
        _finish_git(process, errors)
        return value.decode("ascii").strip()


def _object(process, object_id: str, kind: bytes, limit: int, object_format: str, limit_name: str):
    process.stdin.write(object_id.encode("ascii") + b"\n")
    process.stdin.flush()
    header = process.stdout.readline(256).split()
    if len(header) != 3 or header[0] != object_id.encode("ascii") or header[1] != kind:
        raise SourceBundleError(f"object ID must identify a {kind.decode()} object directly: {object_id}")
    try:
        size = int(header[2])
    except ValueError as error:
        raise SourceBundleError("invalid Git object size") from error
    if size < 0 or size > limit:
        raise SourceBundleError(f"{limit_name} exceeds {limit} bytes")
    data = process.stdout.read(size)
    if len(data) != size or process.stdout.read(1) != b"\n":
        raise SourceBundleError(f"incomplete Git object: {object_id}")
    digest = hashlib.new(object_format)
    digest.update(kind + b" " + str(size).encode("ascii") + b"\0")
    digest.update(data)
    if digest.hexdigest() != object_id:
        raise SourceBundleError(f"Git object hash mismatch: {object_id}")
    return data


def _safe_path(raw_path: bytes) -> str:
    if len(raw_path) > MAX_PATH_BYTES:
        raise SourceBundleError("repository path exceeds the path byte limit")
    try:
        path = raw_path.decode("utf-8")
    except UnicodeDecodeError as error:
        raise SourceBundleError("repository paths must be valid UTF-8") from error
    parts = path.split("/")
    if any(part in ("", ".", "..") or part.casefold() == ".git" or part.endswith((".", " "))
           for part in parts):
        raise SourceBundleError(f"unsafe repository path: {path!r}")
    if any(character in "\\:" or ord(character) < 32 or ord(character) == 127
           or unicodedata.category(character) == "Cf" for character in path):
        raise SourceBundleError(f"unsafe repository path: {path!r}")
    return path


def _entries(process, commit: str, object_format: str) -> list[_Entry]:
    commit_data = _object(process, commit, b"commit", MAX_COMMIT_BYTES, object_format, "commit metadata")
    tree_line = commit_data.split(b"\n", 1)[0]
    object_id_bytes = len(commit) // 2
    if re.fullmatch(rb"tree [0-9a-f]{" + str(len(commit)).encode() + rb"}", tree_line) is None:
        raise SourceBundleError("commit has an invalid root tree ID")
    pending = [(tree_line[5:].decode("ascii"), "")]
    entries = []
    seen = set()
    metadata_bytes = 0
    while pending:
        tree_id, prefix = pending.pop()
        data = _object(process, tree_id, b"tree", MAX_TREE_BYTES - metadata_bytes, object_format, "tree metadata")
        metadata_bytes += len(data)
        offset = 0
        subtrees = []
        while offset < len(data):
            if len(entries) >= MAX_ENTRIES:
                raise SourceBundleError(f"source exceeds {MAX_ENTRIES} entries")
            mode_end = data.find(b" ", offset)
            name_end = data.find(b"\0", mode_end + 1)
            object_end = name_end + 1 + object_id_bytes
            if mode_end < offset or name_end < 0 or object_end > len(data):
                raise SourceBundleError("invalid Git tree entry")
            raw_mode = data[offset:mode_end]
            raw_name = data[mode_end + 1:name_end]
            if len(raw_mode) > 6 or re.fullmatch(rb"[0-7]+", raw_mode) is None or b"/" in raw_name:
                raise SourceBundleError("invalid Git tree mode or path component")
            mode = int(raw_mode, 8)
            object_id = data[name_end + 1:object_end].hex()
            offset = object_end
            path = _safe_path(prefix.encode("utf-8") + raw_name)
            key = unicodedata.normalize("NFC", path).casefold()
            if key in seen:
                raise SourceBundleError(f"duplicate or case/Unicode-colliding repository path: {path!r}")
            seen.add(key)
            if mode == 0o120000:
                raise SourceBundleError(f"symlinks are unsupported: {path!r}")
            if mode == 0o160000:
                raise SourceBundleError(f"submodules are unsupported: {path!r}")
            if mode == 0o040000:
                subtrees.append((object_id, path + "/"))
            elif mode not in (0o100644, 0o100755):
                raise SourceBundleError(f"unsupported Git entry: {path!r}")
            entries.append(_Entry(path, object_id, mode))
        pending.extend(reversed(subtrees))
    return entries


class _LimitedArchive:
    def __init__(self, file):
        self.file = file
        self.size = 0

    def write(self, data):
        if self.size + len(data) > MAX_ARCHIVE_BYTES:
            raise SourceBundleError(f"archive exceeds {MAX_ARCHIVE_BYTES} bytes")
        written = self.file.write(data)
        self.size += written
        return written


def _write_archive(repository: Path, entries: list[_Entry], object_format: str, destination: Path):
    total_bytes = 0
    file_count = 0
    with destination.open("xb") as file, _git_process(repository, "cat-file", "--batch", input_pipe=True) as (process, errors):
        with tarfile.open(fileobj=_LimitedArchive(file), mode="w|", format=tarfile.PAX_FORMAT) as archive:
            root = tarfile.TarInfo(ROOT_PREFIX)
            root.type = tarfile.DIRTYPE
            root.mode = 0o755
            archive.addfile(root)
            for entry in entries:
                info = tarfile.TarInfo(ROOT_PREFIX + entry.path)
                if entry.mode == 0o040000:
                    info.type = tarfile.DIRTYPE
                    info.mode = 0o755
                    archive.addfile(info)
                else:
                    remaining = MAX_SOURCE_BYTES - total_bytes
                    limit = min(MAX_BLOB_BYTES, remaining)
                    limit_name = "blob" if MAX_BLOB_BYTES <= remaining else "total source"
                    data = _object(process, entry.object_id, b"blob", limit, object_format, limit_name)
                    if data.split(b"\n", 1)[0].rstrip(b"\r") == b"version https://git-lfs.github.com/spec/v1":
                        raise SourceBundleError(f"Git LFS pointers are unsupported: {entry.path!r}")
                    info.mode = 0o755 if entry.mode == 0o100755 else 0o644
                    info.size = len(data)
                    archive.addfile(info, io.BytesIO(data))
                    total_bytes += len(data)
                    file_count += 1
        process.stdin.close()
        _finish_git(process, errors)
        file.flush()
        os.fsync(file.fileno())
    return file_count, total_bytes


def _remove_owned(path: Path, identity):
    try:
        stat = path.lstat()
        if (stat.st_dev, stat.st_ino) == identity:
            path.unlink()
    except FileNotFoundError:
        pass


def package_source(repository: str | Path, commit: str, output: str | Path) -> dict:
    """Create a new bundle directory. A published manifest marks a complete bundle.

    The output's parent must exist. Existing output paths, including symlinks, are
    always rejected. A failure removes this call's files and empty output directory.
    """
    if not isinstance(commit, str) or re.fullmatch(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})", commit) is None:
        raise SourceBundleError("commit must be a full 40- or 64-character hexadecimal object ID")
    commit = commit.lower()
    repository = Path(repository).absolute()
    output = Path(output).absolute()
    published = []
    created = False
    try:
        object_format = _git_value(repository, "rev-parse", "--show-object-format")
        expected_length = {"sha1": 40, "sha256": 64}.get(object_format)
        if len(commit) != expected_length:
            raise SourceBundleError("commit length does not match the repository object format")
        with _git_process(repository, "cat-file", "--batch", input_pipe=True) as (process, errors):
            entries = _entries(process, commit, object_format)
            process.stdin.close()
            _finish_git(process, errors)
        try:
            output.mkdir(mode=0o700)
        except FileExistsError as error:
            raise SourceBundleError(f"output already exists: {output}") from error
        created = True
        with tempfile.TemporaryDirectory(prefix=".staging-", dir=output) as temporary:
            staging = Path(temporary)
            archive = staging / "source.tar"
            file_count, total_bytes = _write_archive(repository, entries, object_format, archive)
            with archive.open("rb") as file:
                digest = hashlib.file_digest(file, "sha256").hexdigest()
            manifest = {
                "schema_version": 1, "commit": commit, "object_format": object_format,
                "archive_name": "source.tar", "archive_format": "tar-pax", "root_prefix": ROOT_PREFIX,
                "archive_sha256": digest, "archive_size_bytes": archive.stat().st_size,
                "entry_count": len(entries),
                "file_count": file_count, "total_file_bytes": total_bytes,
            }
            manifest_path = staging / "manifest.json"
            with manifest_path.open("x", encoding="utf-8", newline="\n") as file:
                json.dump(manifest, file, indent=2, sort_keys=True, allow_nan=False)
                file.write("\n")
                file.flush()
                os.fsync(file.fileno())
            # Hard links publish complete files without replacing any existing path.
            # The manifest is last so consumers never observe a success marker first.
            for source in (archive, manifest_path):
                destination = output / source.name
                stat = source.stat()
                identity = (stat.st_dev, stat.st_ino)
                os.link(source, destination)
                published.append((destination, identity))
        return manifest
    except BaseException as error:
        for path, identity in reversed(published):
            _remove_owned(path, identity)
        if created:
            try:
                output.rmdir()
            except OSError:
                pass
        if isinstance(error, (OSError, UnicodeError, tarfile.TarError)):
            raise SourceBundleError(f"cannot create source bundle: {error}") from error
        raise
