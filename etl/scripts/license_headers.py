# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SPDX license-header stamper and verifier for the Healthpipe public tree.

Implements US 27067. See
docs/superpowers/specs/2026-06-17-license-header-script-design.md.
"""

from __future__ import annotations

import argparse
import difflib
import fnmatch
import subprocess
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

# --- Configuration -------------------------------------------------------------------

HOLDER = "Enversion Journl ApS"
PROJECT = "Healthpipe"
LICENSE_ID = "AGPL-3.0-or-later"
DEFAULT_YEAR = 2026

INCLUDE_ROOTS = [
    "etl",
    ".devcontainer",
    "pyproject.toml",
    "package.json",
    ".pre-commit-config.yaml",
    ".gitignore",
    ".gitattributes",
    ".env.template",
    # Public publish-output coverage: these ship via the publish transform's
    # whitelist (gitleaks/docker config, the self-build files, the CI workflows).
    # Keep in sync with healthpipe-infra/publish/config.py so the --check gate
    # covers every commentable file that ships to the public repo.
    ".gitleaks.toml",
    ".dockerignore",
    ".github",
    "infrastructure/containers/dockerfiles/code-server.Dockerfile",
    "infrastructure/containers/dockerfiles/code-server-base.Dockerfile",
    "infrastructure/devops/scripts/build-push.sh",
    "infrastructure/devops/scripts/check_hook_entries.py",
    "infrastructure/devops/scripts/check_ruff_pin.py",
]

# Exclude globs are fnmatch patterns over repo-root-relative POSIX paths.
# A leading "*/" matches at any depth (e.g. fixtures); a bare "name/*" is
# anchored to the top level (e.g. private top-level dirs). The three public
# self-build files under infrastructure/ (code-server Dockerfiles + build-push.sh)
# are listed in INCLUDE_ROOTS and intentionally stamped; the rest of a private
# top-level infrastructure/ is simply never enumerated (it is not an INCLUDE root).
EXCLUDE_GLOBS = [
    "LICENSE",
    "README.md",  # root public README — a foundation doc (carries its own license section)
    "CONTRIBUTING.md",
    "CODE_OF_CONDUCT.md",
    "SECURITY.md",
    "CHANGELOG.md",
    "*.lock",
    "package-lock.json",
    "uv.lock",
    "*/tests/fixtures/*",
    "*/__snapshots__/*",
    "notebooks/*",
    "Daniel/*",
]

PRECOMMIT_CONFIG = ".pre-commit-config.yaml"
_HOOKS_MARKER_START = "# >>> healthpipe-license-hooks (managed by license_headers.py) >>>"
_HOOKS_MARKER_END = "# <<< healthpipe-license-hooks <<<"
_LICENSE_HOOKS_BLOCK = """  - repo: local
    hooks:
      - id: license-headers-apply
        name: Insert missing SPDX license headers
        entry: python etl/scripts/license_headers.py --apply
        language: system
        pass_filenames: true
        require_serial: true
      - id: license-headers
        name: Verify SPDX license headers
        entry: python etl/scripts/license_headers.py --check
        language: system
        pass_filenames: true
        require_serial: true"""


def ensure_precommit_hooks(config_text: str) -> str:
    """Insert the marker-delimited license-header hooks after ``repos:``. Idempotent."""
    if _HOOKS_MARKER_START in config_text:
        return config_text
    block = f"{_HOOKS_MARKER_START}\n{_LICENSE_HOOKS_BLOCK}\n{_HOOKS_MARKER_END}\n"
    needle = "repos:\n"
    idx = config_text.find(needle)
    if idx == -1:
        raise ValueError("no 'repos:' key found in pre-commit config")
    insert_at = idx + len(needle)
    return config_text[:insert_at] + block + config_text[insert_at:]


def remove_precommit_hooks(config_text: str) -> str:
    """Remove the marker-delimited license-header hook block. Idempotent."""
    start = config_text.find(_HOOKS_MARKER_START)
    if start == -1:
        return config_text
    end = config_text.find(_HOOKS_MARKER_END, start)
    if end == -1:
        return config_text
    end += len(_HOOKS_MARKER_END)
    if end < len(config_text) and config_text[end] == "\n":
        end += 1
    return config_text[:start] + config_text[end:]


@dataclass(frozen=True)
class CommentStyle:
    line_prefix: str | None = None
    block: tuple[str, str, str] | None = None  # (open, indent, close)


HASH = CommentStyle(line_prefix="#")
DASH = CommentStyle(line_prefix="--")
SLASH = CommentStyle(line_prefix="//")
XML = CommentStyle(block=("<!--", "  ", "-->"))

_EXT_STYLES: dict[str, CommentStyle] = {
    ".py": HASH,
    ".sh": HASH,
    ".yml": HASH,
    ".yaml": HASH,
    ".tpl": HASH,
    ".toml": HASH,
    ".ini": HASH,
    ".cfg": HASH,
    ".template": HASH,
    ".example": HASH,
    ".sql": DASH,
    ".cs": SLASH,
    ".c": SLASH,
    ".h": SLASH,
    ".cpp": SLASH,
    ".js": SLASH,
    ".ts": SLASH,
    ".xml": XML,
    ".csproj": XML,
    ".html": XML,
    ".htm": XML,
    ".svg": XML,
    ".md": XML,
    ".markdown": XML,
    ".dockerfile": HASH,  # e.g. code-server.Dockerfile (suffix is lowercased)
}

_FILENAME_STYLES: dict[str, CommentStyle] = {
    "Dockerfile": HASH,
    "Makefile": HASH,
    ".gitignore": HASH,
    ".gitattributes": HASH,
    ".dockerignore": HASH,
}

_SKIP_EXTS = {".json", ".csv", ".lock"}


def comment_style_for(path: Path) -> CommentStyle | None:
    """Return the comment style for a file, or None if it should be skipped."""
    if path.name in _FILENAME_STYLES:
        return _FILENAME_STYLES[path.name]
    suffix = path.suffix.lower()
    if suffix in _SKIP_EXTS:
        return None
    return _EXT_STYLES.get(suffix)


@dataclass(frozen=True)
class HeaderConfig:
    year: int = DEFAULT_YEAR
    holder: str = HOLDER
    project: str = PROJECT
    license_id: str = LICENSE_ID


def _header_body_lines(cfg: HeaderConfig) -> list[str]:
    license_name = cfg.license_id.replace("-or-later", "").replace("-only", "")
    return [
        f"SPDX-FileCopyrightText: {cfg.year} {cfg.holder}",
        f"SPDX-License-Identifier: {cfg.license_id}",
        "",
        f"This file is part of {cfg.project}. See the LICENSE file in the project",
        f"root for the full {license_name} license text.",
    ]


def render_header(style: CommentStyle, cfg: HeaderConfig) -> str:
    """Render the SPDX header block (ending in a newline) for a comment style."""
    body = _header_body_lines(cfg)
    if style.line_prefix is not None:
        prefix = style.line_prefix
        rendered = [f"{prefix} {line}" if line else prefix for line in body]
        return "\n".join(rendered) + "\n"
    open_token, indent, close_token = style.block  # type: ignore[misc]
    inner = [f"{indent}{line}" if line else "" for line in body]
    return open_token + "\n" + "\n".join(inner) + "\n" + close_token + "\n"


def has_header(text: str, style: CommentStyle, max_lines: int = 15) -> bool:
    """True if an SPDX-License-Identifier appears in a comment near the top.

    Comment-context-aware: prose that merely mentions the token (e.g. a Markdown
    doc describing SPDX) is not mistaken for a real header.
    """
    lines = text.splitlines()[:max_lines]
    if style.line_prefix is not None:
        for line in lines:
            stripped = line.lstrip()
            if stripped.startswith(style.line_prefix) and "SPDX-License-Identifier:" in stripped:
                return True
        return False
    assert style.block is not None
    open_token, _, close_token = style.block
    in_block = False
    for line in lines:
        if open_token in line:
            in_block = True
        if in_block and "SPDX-License-Identifier:" in line:
            return True
        if close_token in line:
            in_block = False
    return False


_BOM = "﻿"


def is_generated(text: str, max_lines: int = 3) -> bool:
    """True if the first lines mark the file as generated ("do not edit").

    A header would change the bytes that the generator produces, so such files are skipped.
    """
    return "do not edit" in "\n".join(text.splitlines()[:max_lines]).lower()


def _is_coding_cookie(line: str) -> bool:
    stripped = line.lstrip()
    return stripped.startswith("#") and ("coding:" in stripped or "coding=" in stripped)


def _consume_declaration(
    lines: list[str], keep: int, opener: str, terminator: str, max_lookahead: int = 10
) -> int:
    """Advance past a markup declaration that starts at ``lines[keep]``.

    Only consume when the ``terminator`` is found within ``max_lookahead`` lines;
    an unterminated/malformed declaration is left untouched (the header goes above
    it) rather than swallowing the rest of the file. ``opener`` is matched
    case-insensitively against the line's stripped start.
    """
    if keep >= len(lines) or not lines[keep].lstrip().lower().startswith(opener):
        return keep
    for offset in range(min(max_lookahead, len(lines) - keep)):
        if terminator in lines[keep + offset]:
            return keep + offset + 1
    return keep


def insert_header(text: str, header: str) -> str:
    """Insert `header` at the top, after shebang/cookie/xml-decl/doctype/BOM.

    `header` is a rendered block ending in a newline. Leading blank lines in the
    original content are normalized to a single blank line after the header.
    """
    bom = ""
    if text.startswith(_BOM):
        bom, text = _BOM, text[len(_BOM) :]

    lines = text.split("\n")
    keep = 0
    if keep < len(lines) and lines[keep].startswith("#!"):
        keep += 1
    if keep < len(lines) and _is_coding_cookie(lines[keep]):
        keep += 1
    keep = _consume_declaration(lines, keep, "<?xml", "?>")
    keep = _consume_declaration(lines, keep, "<!doctype", ">")

    preserved = lines[:keep]
    rest = lines[keep:]
    while rest and rest[0].strip() == "":
        rest.pop(0)

    blocks: list[str] = []
    if preserved:
        blocks.append("\n".join(preserved))
    blocks.append(header.rstrip("\n"))
    if rest:
        blocks.append("\n".join(rest))

    result = bom + "\n\n".join(blocks)
    if not result.endswith("\n"):
        result += "\n"
    return result


def strip_header(text: str, header: str) -> str:
    """Remove a header block previously inserted by ``insert_header``.

    Exact inverse for canonical inputs: ``strip_header(insert_header(x, h), h) == x``
    when ``x`` has no blank lines immediately after its preamble (a blank line right
    after e.g. a shebang is collapsed by ``insert_header`` at stamp time and is not
    restored). Matching is shape-based on the rendered ``header`` (same preamble
    handling as insertion), so a file that merely mentions ``SPDX-License-Identifier``
    in prose is never touched. Idempotent: text without the exact block is returned
    unchanged.
    """
    bom = ""
    if text.startswith(_BOM):
        bom, text = _BOM, text[len(_BOM) :]

    lines = text.split("\n")
    keep = 0
    if keep < len(lines) and lines[keep].startswith("#!"):
        keep += 1
    if keep < len(lines) and _is_coding_cookie(lines[keep]):
        keep += 1
    keep = _consume_declaration(lines, keep, "<?xml", "?>")
    keep = _consume_declaration(lines, keep, "<!doctype", ">")

    preserved = lines[:keep]
    rest = lines[keep:]

    # insert_header places a single blank line before the header when a preamble exists.
    while rest and rest[0].strip() == "":
        rest.pop(0)

    header_lines = header.rstrip("\n").split("\n")
    if rest[: len(header_lines)] != header_lines:
        return bom + "\n".join(lines)  # not our block -> no-op (idempotent)

    rest = rest[len(header_lines) :]
    # insert_header places a single blank line after the header; drop it.
    if rest and rest[0].strip() == "":
        rest.pop(0)

    result = bom + "\n".join(preserved + rest)
    # Faithful inverse of insert_header's trailing-newline guard: a stamped
    # preamble-only file strips back to its preamble + newline, not a newline-less
    # tail. Only on the match path, so an unstamped file is still a pure no-op.
    if result and not result.endswith("\n"):
        result += "\n"
    return result


def _is_excluded(rel_path: str, globs: list[str]) -> bool:
    return any(fnmatch.fnmatch(rel_path, pattern) for pattern in globs)


def iter_target_files(
    roots: list[str], repo_root: Path, exclude_globs: list[str]
) -> Iterable[Path]:
    """Yield tracked files under `roots`, skipping `exclude_globs`."""
    result = subprocess.run(
        ["git", "ls-files", "-z", "--", *roots],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    for rel in result.stdout.split("\0"):
        if not rel or _is_excluded(rel, exclude_globs):
            continue
        yield repo_root / rel


def _repo_root() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(result.stdout.strip())


def _print_diff(path: Path, before: str, after: str) -> None:
    diff = difflib.unified_diff(
        before.splitlines(keepends=True),
        after.splitlines(keepends=True),
        fromfile=str(path),
        tofile=str(path),
    )
    sys.stdout.writelines(diff)


def _resolve_paths(paths: list[str], repo_root: Path) -> list[Path]:
    resolved = []
    for raw in paths:
        path = Path(raw)
        abs_path = path if path.is_absolute() else repo_root / path
        try:
            rel = abs_path.resolve().relative_to(repo_root.resolve()).as_posix()
        except ValueError:
            rel = path.as_posix()
        if _is_excluded(rel, EXCLUDE_GLOBS):
            continue
        resolved.append(abs_path)
    return resolved


def run(
    paths: list[str],
    repo_root: Path,
    cfg: HeaderConfig,
    mode: str,
    skip_empty: bool,
) -> tuple[list[Path], list[Path], list[Path]]:
    if paths:
        files = _resolve_paths(paths, repo_root)
    else:
        files = list(iter_target_files(INCLUDE_ROOTS, repo_root, EXCLUDE_GLOBS))

    missing: list[Path] = []
    stamped: list[Path] = []
    skipped: list[Path] = []
    for path in files:
        if not path.is_file():
            continue
        style = comment_style_for(path)
        if style is None:
            skipped.append(path)
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            skipped.append(path)
            continue
        if skip_empty and text.strip() == "":
            skipped.append(path)
            continue
        if mode != "strip" and is_generated(text):
            skipped.append(path)
            continue
        if mode == "strip":
            new_text = strip_header(text, render_header(style, cfg))
            if path.name == PRECOMMIT_CONFIG:
                new_text = remove_precommit_hooks(new_text)
            if new_text != text:
                path.write_text(new_text, encoding="utf-8")
                stamped.append(path)
            continue
        if has_header(text, style):
            continue
        if mode == "apply":
            path.write_text(insert_header(text, render_header(style, cfg)), encoding="utf-8")
            stamped.append(path)
        elif mode == "diff":
            _print_diff(path, text, insert_header(text, render_header(style, cfg)))
            missing.append(path)
        else:  # check
            missing.append(path)
    return missing, stamped, skipped


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stamp/verify SPDX license headers.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_const", const="check", dest="mode")
    mode.add_argument("--apply", action="store_const", const="apply", dest="mode")
    mode.add_argument("--diff", action="store_const", const="diff", dest="mode")
    mode.add_argument(
        "--ensure-precommit-hooks", action="store_const", const="ensure-hooks", dest="mode"
    )
    mode.add_argument("--strip", action="store_const", const="strip", dest="mode")
    parser.set_defaults(mode="check")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR)
    parser.add_argument("--skip-empty", action="store_true")
    parser.add_argument("paths", nargs="*", help="Specific files; default = configured tree")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    cfg = HeaderConfig(year=args.year)
    repo_root = _repo_root()

    if args.mode == "ensure-hooks":
        config_path = repo_root / PRECOMMIT_CONFIG
        text = config_path.read_text(encoding="utf-8")
        new_text = ensure_precommit_hooks(text)
        if new_text != text:
            config_path.write_text(new_text, encoding="utf-8")
            print("hooks=inserted")
        else:
            print("hooks=present")
        return 0

    missing, stamped, skipped = run(args.paths, repo_root, cfg, args.mode, args.skip_empty)

    if args.mode == "apply":
        print(f"stamped={len(stamped)} skipped={len(skipped)}")
        return 0
    if args.mode == "strip":
        print(f"stripped={len(stamped)} skipped={len(skipped)}")
        return 0
    if missing:
        if args.mode == "check":
            print("Missing license header(s):")
            for path in missing:
                print(f"  {path}")
        if args.mode == "diff":
            print(f"pending={len(missing)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
