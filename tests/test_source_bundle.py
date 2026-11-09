import hashlib
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
import zlib
from unittest.mock import patch

from sparemesh.source_bundle import SourceBundleError, package_source


class SourceBundleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repository with spaces"
        self.repo.mkdir()
        self.git("init", "-q")
        self.git("config", "user.name", "Bundle Tests")
        self.git("config", "user.email", "bundle@example.invalid")
        self.write("hello.txt", b"committed\n")
        self.commit = self.save()
        self.output = self.root / "output bundle"

    def git(self, *args, input=None):
        return subprocess.run(
            ["git", "-C", str(self.repo), *args], input=input, capture_output=True, check=True,
        ).stdout.strip().decode()

    def write(self, path, data):
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return target

    def save(self):
        self.git("add", "--all")
        self.git("commit", "-qm", "fixture")
        return self.git("rev-parse", "HEAD")

    def package(self, commit=None, output=None):
        return package_source(self.repo, commit or self.commit, output or self.output)

    def assert_rejected(self, message, commit=None):
        with self.assertRaisesRegex(SourceBundleError, message):
            self.package(commit)
        self.assertFalse(self.output.exists())

    def test_fixed_commit_excludes_dirty_and_untracked_files_and_preserves_index(self):
        self.write("hello.txt", b"staged change\n")
        self.git("add", "hello.txt")
        self.write("hello.txt", b"unstaged change\n")
        self.write("untracked.txt", b"untracked\n")
        before = self.git("status", "--porcelain=v1")
        manifest = self.package(self.commit.upper())
        with tarfile.open(self.output / "source.tar") as archive:
            self.assertEqual(archive.getnames(), ["source", "source/hello.txt"])
            self.assertEqual(archive.extractfile("source/hello.txt").read(), b"committed\n")
        self.assertEqual(self.git("status", "--porcelain=v1"), before)
        self.assertEqual(manifest["commit"], self.commit)
        self.assertEqual(manifest["file_count"], 1)
        self.assertEqual(manifest["total_file_bytes"], len(b"committed\n"))
        self.assertEqual(json.loads((self.output / "manifest.json").read_text()), manifest)
        data = (self.output / "source.tar").read_bytes()
        self.assertEqual(manifest["archive_sha256"], hashlib.sha256(data).hexdigest())
        self.assertEqual(manifest["archive_size_bytes"], len(data))
        self.assertNotIn(str(self.repo), json.dumps(manifest))

    def test_deterministic_archive_unicode_spaces_and_executable_modes(self):
        self.write("folder with spaces/тест.txt", "привет\n".encode())
        script = self.write("run.sh", b"#!/bin/sh\nexit 0\n")
        script.chmod(0o755)
        self.commit = self.save()
        first = self.package()
        second_output = self.root / "second"
        second = self.package(output=second_output)
        self.assertEqual(first, second)
        self.assertEqual((self.output / "source.tar").read_bytes(), (second_output / "source.tar").read_bytes())
        with tarfile.open(self.output / "source.tar") as archive:
            self.assertEqual(archive.getmember("source/run.sh").mode, 0o755)
            self.assertEqual(archive.getmember("source/hello.txt").mode, 0o644)
            self.assertEqual(archive.extractfile("source/folder with spaces/тест.txt").read(), "привет\n".encode())
            for member in archive:
                self.assertEqual((member.uid, member.gid, member.mtime), (0, 0, 0))
                self.assertEqual((member.uname, member.gname), ("", ""))

    def test_export_attributes_do_not_alter_committed_contents(self):
        self.write(".gitattributes", b"hello.txt export-ignore\nrevision.txt export-subst\n")
        self.write("revision.txt", b"$Format:%H$\n")
        self.commit = self.save()
        self.package()
        with tarfile.open(self.output / "source.tar") as archive:
            self.assertEqual(archive.extractfile("source/hello.txt").read(), b"committed\n")
            self.assertEqual(archive.extractfile("source/revision.txt").read(), b"$Format:%H$\n")
            self.assertIn("source/.gitattributes", archive.getnames())

    def test_rejects_refs_abbreviations_invalid_hex_and_missing_objects(self):
        for commit in ["HEAD", "main", self.commit[:12], "g" * 40, "1" * 40, "a" * 64]:
            with self.subTest(commit=commit):
                self.assert_rejected("commit|object|hex", commit)

    def test_rejects_blob_tree_and_annotated_tag_objects(self):
        self.git("tag", "-a", "release", "-m", "tag")
        for object_id in [self.git("rev-parse", "HEAD:hello.txt"), self.git("rev-parse", "HEAD^{tree}"),
                          self.git("rev-parse", "release")]:
            with self.subTest(object_id=object_id):
                self.assert_rejected("commit object", object_id)

    def test_replacement_objects_and_git_environment_do_not_change_the_commit(self):
        original = self.commit
        self.write("hello.txt", b"replacement\n")
        replacement = self.save()
        self.git("replace", original, replacement)
        with patch.dict(os.environ, {"GIT_DIR": "/missing", "GIT_WORK_TREE": "/missing"}):
            self.package(original)
        with tarfile.open(self.output / "source.tar") as archive:
            self.assertEqual(archive.extractfile("source/hello.txt").read(), b"committed\n")

    def test_rejects_symlink(self):
        (self.repo / "link").symlink_to("hello.txt")
        self.commit = self.save()
        self.assert_rejected("symlink")

    def test_rejects_submodule(self):
        self.git("update-index", "--add", "--cacheinfo", f"160000,{self.commit},vendor/module")
        self.git("commit", "-qm", "submodule")
        self.commit = self.git("rev-parse", "HEAD")
        self.assert_rejected("submodule")

    def test_rejects_lfs_pointer_and_cleans_output(self):
        for newline in [b"\n", b"\r\n"]:
            with self.subTest(newline=newline):
                self.write("large.bin", newline.join([
                    b"version https://git-lfs.github.com/spec/v1", b"oid sha256:" + b"a" * 64, b"size 123", b"",
                ]))
                self.commit = self.save()
                self.assert_rejected("LFS")

    def test_existing_outputs_including_dangling_symlinks_are_untouched(self):
        for kind in ["file", "directory", "symlink"]:
            with self.subTest(kind=kind):
                if kind == "file":
                    self.output.write_bytes(b"keep")
                elif kind == "directory":
                    self.output.mkdir()
                    (self.output / "manifest.json").write_bytes(b"keep")
                else:
                    self.output.symlink_to(self.root / "missing")
                with self.assertRaisesRegex(SourceBundleError, "already exists"):
                    self.package()
                if kind == "directory":
                    self.assertEqual((self.output / "manifest.json").read_bytes(), b"keep")
                    (self.output / "manifest.json").unlink()
                    self.output.rmdir()
                else:
                    if kind == "file":
                        self.assertEqual(self.output.read_bytes(), b"keep")
                    else:
                        self.assertTrue(self.output.is_symlink())
                    self.output.unlink()

    def test_limit_rejections(self):
        for setting, value, message in [("MAX_ENTRIES", 0, "entries"), ("MAX_BLOB_BYTES", 1, "blob"),
                                        ("MAX_SOURCE_BYTES", 1, "total"), ("MAX_ARCHIVE_BYTES", 1, "archive"),
                                        ("MAX_COMMIT_BYTES", 1, "commit metadata"), ("MAX_TREE_BYTES", 1, "tree metadata")]:
            with self.subTest(setting=setting), patch("sparemesh.source_bundle." + setting, value):
                self.assert_rejected(message)

    def test_publish_failure_removes_our_archive_and_temporary_files(self):
        original_link = os.link
        calls = []

        def fail_manifest(source, destination):
            calls.append(str(destination))
            if Path(destination).name == "manifest.json":
                raise OSError("injected publication failure")
            return original_link(source, destination)

        with patch("sparemesh.source_bundle.os.link", side_effect=fail_manifest):
            self.assert_rejected("publication failure")
        self.assertEqual(len(calls), 2)

    def test_concurrent_retry_never_overwrites_the_winner(self):
        def attempt():
            try:
                return self.package()
            except SourceBundleError as error:
                return error

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: attempt(), range(2)))
        successes = [value for value in results if isinstance(value, dict)]
        errors = [value for value in results if isinstance(value, SourceBundleError)]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(errors), 1)
        self.assertIn("already exists", str(errors[0]))
        self.assertEqual(json.loads((self.output / "manifest.json").read_text()), successes[0])
        self.assertEqual({path.name for path in self.output.iterdir()}, {"source.tar", "manifest.json"})

    def test_manifest_collision_keeps_the_existing_file_and_removes_our_archive(self):
        original_link = os.link

        def introduce_collision(source, destination):
            if Path(destination).name == "manifest.json":
                Path(destination).write_bytes(b"existing manifest")
            return original_link(source, destination)

        with patch("sparemesh.source_bundle.os.link", side_effect=introduce_collision):
            with self.assertRaisesRegex(SourceBundleError, "File exists"):
                self.package()
        self.assertEqual((self.output / "manifest.json").read_bytes(), b"existing manifest")
        self.assertEqual({path.name for path in self.output.iterdir()}, {"manifest.json"})

    def tree_commit(self, names, object_id=None):
        blob = object_id or self.git("rev-parse", "HEAD:hello.txt")
        data = b"".join(f"100644 blob {blob}\t{name}".encode() + b"\0" for name in names)
        tree = self.git("mktree", "-z", "--missing", input=data)
        return self.git("commit-tree", tree, input=b"tree fixture\n")

    def test_rejects_unsafe_committed_paths_and_case_collisions(self):
        for names in [[".git"], [".."], ["back\\slash"], ["drive:name"], ["line\nbreak"],
                      ["HELLO.txt", "hello.txt"], ["café", "cafe\u0301"]]:
            with self.subTest(names=names):
                self.assert_rejected("unsafe|colliding", self.tree_commit(names))

    def test_missing_blob_is_reported_and_does_not_create_output(self):
        self.assert_rejected("blob object", self.tree_commit(["missing.txt"], "a" * 40))

    def test_rejects_corrupt_blob_commit_and_tree_objects(self):
        self.write("nested/inside.txt", b"nested data\n")
        self.commit = self.save()
        root_tree = self.git("rev-parse", "HEAD^{tree}")
        child_tree = self.git("rev-parse", "HEAD:nested")
        blob = self.git("rev-parse", "HEAD:hello.txt")
        for kind, object_id in [("blob", blob), ("commit", self.commit), ("tree", root_tree), ("tree", child_tree)]:
            with self.subTest(kind=kind, object_id=object_id):
                data = subprocess.check_output(["git", "-C", str(self.repo), "cat-file", kind, object_id])
                changed = bytes([data[0] ^ 1]) + data[1:]
                object_path = self.repo / ".git" / "objects" / object_id[:2] / object_id[2:]
                original = object_path.read_bytes()
                object_path.chmod(0o600)
                object_path.write_bytes(zlib.compress(kind.encode() + b" " + str(len(changed)).encode() + b"\0" + changed))
                try:
                    self.assert_rejected("hash mismatch")
                finally:
                    object_path.write_bytes(original)

    def test_bare_sha256_repository_roundtrip(self):
        bare = self.root / "sha256 bare.git"
        initialized = subprocess.run(["git", "init", "--bare", "--object-format=sha256", str(bare)], capture_output=True)
        if initialized.returncode:
            self.skipTest("installed Git does not support SHA-256 repositories")

        def git(*arguments, input=None):
            return subprocess.run(["git", "-C", str(bare), *arguments], input=input,
                                  capture_output=True, check=True).stdout.strip().decode()

        git("config", "user.name", "Bundle Tests")
        git("config", "user.email", "bundle@example.invalid")
        blob = git("hash-object", "-w", "--stdin", input=b"sha256 source\n")
        tree = git("mktree", input=f"100644 blob {blob}\tfile.txt\n".encode())
        commit = git("commit-tree", tree, input=b"sha256 fixture\n")
        manifest = package_source(bare, commit, self.output)
        self.assertEqual(manifest["object_format"], "sha256")
        self.assertEqual(manifest["commit"], commit)
        self.assertEqual(len(commit), 64)
        with tarfile.open(self.output / "source.tar") as archive:
            self.assertEqual(archive.extractfile("source/file.txt").read(), b"sha256 source\n")

    def test_deep_tree_hits_path_limit_without_recursion(self):
        object_id = self.git("mktree", input=b"")
        for _ in range(2100):
            data = b"40000 d\0" + bytes.fromhex(object_id)
            raw_object = b"tree " + str(len(data)).encode() + b"\0" + data
            object_id = hashlib.sha1(raw_object).hexdigest()
            path = self.repo / ".git" / "objects" / object_id[:2] / object_id[2:]
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(zlib.compress(raw_object))
        commit = self.git("commit-tree", object_id, input=b"deep tree\n")
        self.assert_rejected("path byte limit", commit)

    def test_empty_commit_creates_a_root_only_archive(self):
        tree = self.git("mktree", input=b"")
        self.commit = self.git("commit-tree", tree, input=b"empty commit\n")
        manifest = self.package()
        self.assertEqual((manifest["entry_count"], manifest["file_count"], manifest["total_file_bytes"]), (0, 0, 0))
        with tarfile.open(self.output / "source.tar") as archive:
            self.assertEqual(archive.getnames(), ["source"])

    def test_invalid_repository_is_reported(self):
        with self.assertRaisesRegex(SourceBundleError, "repository"):
            package_source(self.root, self.commit, self.output)
        self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
