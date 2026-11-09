# Source bundle contract v1

The source packager reads one fixed commit from a local Git repository and creates an uncompressed PAX tar archive plus a JSON manifest. Python 3.11 or newer and the `git` executable are required.

## CLI

Run the module from the Sparemesh checkout, or make the checkout available through `PYTHONPATH`. Repository and output paths are resolved relative to the caller's working directory.

```sh
commit="$(git -C '/path/to/project' rev-parse HEAD)"
python3 -m sparemesh bundle-source \
  --repo '/path/to/project' \
  --commit "$commit" \
  --output '/path/to/bundles/run-001'
```

The output parent must already exist. The output directory must be a new path. Success prints the manifest as JSON and exits with status `0`. Packaging errors print `{"error":{"code":"SourceBundleError","message":"..."}}` to stderr and exit with status `2`. This command operates independently of the coordinator database.

The Python entry point returns the same manifest.

```python
from sparemesh.source_bundle import package_source

manifest = package_source("/path/to/project", commit, "/path/to/bundles/run-001")
```

## Snapshot semantics

The commit argument is a complete 40-character SHA-1 or 64-character SHA-256 object ID, matching the repository's storage format. Uppercase hexadecimal is normalized to lowercase. The object must directly identify a commit. Branches, abbreviated IDs, trees, blobs, and annotated tag objects are rejected.

The packager verifies the Git object hash of the requested commit, every traversed tree, and every included blob. Iterative traversal reads only that snapshot. Git tree and blob objects provide the source bytes. Committed `.gitattributes` files are included as source. `export-ignore`, `export-subst`, checkout filters, Git replacements, and caller-provided `GIT_*` overrides cannot change the snapshot. Worktree and index contents remain untouched. Repository code, hooks, and dependency installers are never run. All required objects must already be available locally.

The archive has one fixed `source/` root. All tree directories are represented, including committed empty trees. Ordinary files receive mode `0644` or `0755` according to the committed executable bit. Directories receive mode `0755`. Archive ownership IDs and timestamps are zero, and owner/group names are empty. Ordering follows deterministic traversal of the committed Git tree. Repeated packaging with the same implementation produces identical archive bytes and manifest values.

## Pilot limits

The following cases produce a packaging error.

- Symlinks, submodules, and files whose first line identifies the Git LFS v1 pointer format.
- Paths exceeding 4,096 UTF-8 bytes, invalid UTF-8, empty or dot components, a `.git` component in any letter case, trailing dots/spaces, backslashes, colons, ASCII controls, or Unicode format-control characters.
- Paths that collide after NFC Unicode normalization and case folding.
- More than 10,000 committed file/directory entries, excluding the fixed root.
- Commit metadata larger than 1 MiB or total traversed tree metadata larger than 64 MiB. Repeated references count for each traversal.
- Any blob larger than 16 MiB, total file contents larger than 128 MiB, or final archive larger than 192 MiB.

MiB means 1,048,576 bytes. Limits are checked before blob allocation. Metadata reads and individual blob reads are bounded. A rejected entry fails the whole bundle. Every accepted committed file is included.

## Manifest

A complete output directory contains `source.tar` and `manifest.json`. The manifest uses UTF-8 JSON and contains these fields.

| Field | v1 meaning |
| --- | --- |
| `schema_version` | Integer `1` |
| `commit` | Lowercase, complete commit object ID |
| `object_format` | `sha1` or `sha256` |
| `archive_name` | `source.tar` |
| `archive_format` | `tar-pax` |
| `root_prefix` | `source/` |
| `archive_sha256` | Lowercase SHA-256 of the final archive bytes |
| `archive_size_bytes` | Final archive length, including tar headers and padding |
| `entry_count` | Committed file and directory count, excluding the fixed root |
| `file_count` | Ordinary file count |
| `total_file_bytes` | Sum of ordinary file contents before tar overhead |

The manifest contains source identity and archive measurements. Local repository paths, remotes, and credentials are excluded.

## Publication and worker handoff

An exclusive directory creation reserves the requested output name. Existing files, directories, and symlinks are rejected. Both output files are built in a private staging directory. Exclusive hard links publish `source.tar` first and `manifest.json` last. Concurrent callers for the same output yield one successful publisher. Handled failures remove that call's temporary files and published files while preserving any colliding files owned by another caller.

The manifest is the completion marker. A process interruption can leave an incomplete output directory, which the operator must inspect and remove before reusing that name. Consumers require both files. Before extraction, the worker must accept exactly manifest schema v1, match `commit` to its assigned revision, verify the archive length and SHA-256, and apply its safe-extraction policy to the fixed `source/` root. The worker integration owns transport, extraction, execution policy, and cleanup.
