"""Update the vendored copy of mongomock in src/litemongo/_vendor/mongomock.

litemongo carries its own changes on top of mongomock (query indexes, codec options, etc.), so the
vendored copy can't simply be replaced.  Instead, this does a three-way merge per file:

* base: the mongomock commit that is currently vendored (recorded in UPSTREAM_FILE)
* ours: the vendored copy, including the litemongo changes
* theirs: the new mongomock commit

Both upstream versions are first converted to the vendored form (relative imports and a fixed
version string) so that only real changes get merged.  Files that only exist in litemongo (e.g.
indexing.py) are left alone.  Conflicts are left in the files with the usual markers.

Usage:
    python scripts/update_mongomock.py ~/src/mongomock <new-ref> [--base <ref>] [--version <v>]
"""

import argparse
import pathlib
import re
import shutil
import subprocess  # nosec B404
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
VENDOR_DIR = ROOT / "src" / "litemongo" / "_vendor" / "mongomock"
UPSTREAM_FILE = ROOT / "src" / "litemongo" / "_vendor" / "mongomock.UPSTREAM"
SOURCE_SUFFIXES = (".py", ".pyi")


def git(repo: pathlib.Path, *args: str) -> str:
    """Run a git command in the given repository and return its output"""
    return subprocess.run(  # nosec B603 B607
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def to_vendored(text: str) -> str:
    """Rewrite absolute mongomock imports to relative ones"""
    text = re.sub(
        r"^(\s*)import mongomock(\s*#.*)?$", r"\1from .. import mongomock\2", text, flags=re.M
    )
    text = re.sub(r"^(\s*)from mongomock import ", r"\1from . import ", text, flags=re.M)
    text = re.sub(r"^(\s*)from mongomock\.(\w+) import ", r"\1from .\2 import ", text, flags=re.M)
    return text


def export(repo: pathlib.Path, ref: str, version: str, dest: pathlib.Path) -> pathlib.Path:
    """Export the mongomock package at ref into dest, converted to the vendored form"""
    archive = subprocess.run(  # nosec B603 B607
        ["git", "-C", str(repo), "archive", ref, "mongomock", "LICENSE"],
        check=True,
        capture_output=True,
    ).stdout
    subprocess.run(["tar", "-x", "-C", str(dest)], input=archive, check=True)  # nosec B603 B607
    package = dest / "mongomock"
    for path in package.iterdir():
        if path.suffix in SOURCE_SUFFIXES:
            path.write_text(to_vendored(path.read_text()))
    (package / "__version__.py").write_text(f'__version__ = "{version}"\n')
    shutil.move(dest / "LICENSE", package / "LICENSE")
    return package


def merge(base: pathlib.Path, theirs: pathlib.Path) -> list:
    """Merge the upstream changes (base -> theirs) into the vendored copy, returning conflicts"""
    conflicts = []
    for new in sorted(theirs.iterdir()):
        old, ours = base / new.name, VENDOR_DIR / new.name
        if not ours.exists() or not old.exists() or new.name == "__version__.py":
            # Added upstream (or never vendored), or generated
            shutil.copy2(new, ours)
        elif new.suffix in SOURCE_SUFFIXES:
            result = subprocess.run(  # nosec B603 B607
                ["git", "merge-file", "-L", "litemongo", "-L", "base", "-L", "upstream"]
                + [str(ours), str(old), str(new)],
                check=False,
            )
            if result.returncode:
                conflicts.append(ours)
        else:
            shutil.copy2(new, ours)

    # Files removed upstream
    for old in base.iterdir():
        if not (theirs / old.name).exists() and (VENDOR_DIR / old.name).exists():
            (VENDOR_DIR / old.name).unlink()

    return conflicts


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("repo", type=pathlib.Path, help="path to a mongomock git checkout")
    parser.add_argument("ref", help="the mongomock commit, branch or tag to update to")
    parser.add_argument(
        "--base", help=f"the currently vendored commit (default: {UPSTREAM_FILE.name})"
    )
    parser.add_argument("--version", help="version to report (default: latest tag before ref)")
    args = parser.parse_args()

    repo = args.repo.expanduser()
    base_ref = args.base or UPSTREAM_FILE.read_text().strip()
    new_ref = git(repo, "rev-parse", f"{args.ref}^{{commit}}")
    version = args.version or git(repo, "describe", "--tags", "--abbrev=0", new_ref)

    with tempfile.TemporaryDirectory() as tmp:
        tmp = pathlib.Path(tmp)
        (tmp / "base").mkdir()
        (tmp / "theirs").mkdir()
        base = export(repo, base_ref, version, tmp / "base")
        theirs = export(repo, new_ref, version, tmp / "theirs")
        conflicts = merge(base, theirs)

    UPSTREAM_FILE.write_text(new_ref + "\n")
    print(f"Updated vendored mongomock from {base_ref[:12]} to {new_ref[:12]} ({version})")
    if conflicts:
        print("Resolve the conflicts in:", *conflicts, sep="\n  ")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
