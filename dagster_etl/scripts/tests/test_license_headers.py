# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import subprocess
from pathlib import Path

import license_headers as lh


def test_comment_style_by_extension():
    assert lh.comment_style_for(Path("a/b.py")).line_prefix == "#"
    assert lh.comment_style_for(Path("a/b.sql")).line_prefix == "--"
    assert lh.comment_style_for(Path("a/b.cs")).line_prefix == "//"
    assert lh.comment_style_for(Path("a/b.xml")).block == ("<!--", "  ", "-->")
    assert lh.comment_style_for(Path("a/b.md")).block == ("<!--", "  ", "-->")


def test_comment_style_uppercase_and_filenames():
    assert lh.comment_style_for(Path("README.MD")).block == ("<!--", "  ", "-->")
    assert lh.comment_style_for(Path("a/.gitignore")).line_prefix == "#"
    assert lh.comment_style_for(Path("a/Dockerfile")).line_prefix == "#"
    assert lh.comment_style_for(Path("a/.env.template")).line_prefix == "#"


def test_comment_style_skips_uncommentable_and_unknown():
    assert lh.comment_style_for(Path("a/b.json")) is None
    assert lh.comment_style_for(Path("a/b.csv")) is None
    assert lh.comment_style_for(Path("a/uv.lock")) is None
    assert lh.comment_style_for(Path("a/b.bin")) is None


def test_render_hash_header_content():
    out = lh.render_header(lh.HASH, lh.HeaderConfig(year=2026))
    lines = out.splitlines()
    assert lines[0] == "# SPDX-FileCopyrightText: 2026 Enversion Journl ApS"
    assert lines[1] == "# SPDX-License-Identifier: AGPL-3.0-or-later"
    assert lines[2] == "#"  # blank comment line carries NO trailing space
    assert "This file is part of Healthpipe." in out
    assert "LICENSE file" in out
    assert "full AGPL-3.0 license text." in out
    assert out.endswith("\n")


def test_render_dash_and_slash_prefixes():
    assert lh.render_header(lh.DASH, lh.HeaderConfig()).startswith("-- SPDX-FileCopyrightText:")
    assert lh.render_header(lh.SLASH, lh.HeaderConfig()).startswith("// SPDX-FileCopyrightText:")


def test_render_xml_block_header():
    out = lh.render_header(lh.XML, lh.HeaderConfig(year=2026))
    lines = out.splitlines()
    assert lines[0] == "<!--"
    assert lines[1] == "  SPDX-FileCopyrightText: 2026 Enversion Journl ApS"
    assert lines[2] == "  SPDX-License-Identifier: AGPL-3.0-or-later"
    assert lines[3] == ""  # blank inner line, no trailing whitespace
    assert lines[-1] == "-->"


def test_render_has_no_trailing_whitespace():
    for style in (lh.HASH, lh.DASH, lh.SLASH, lh.XML):
        for line in lh.render_header(style, lh.HeaderConfig()).splitlines():
            assert line == line.rstrip(), f"trailing whitespace in {style}: {line!r}"


def test_has_header_detects_spdx_line():
    text = "# SPDX-License-Identifier: AGPL-3.0-or-later\n\nprint('hi')\n"
    assert lh.has_header(text, lh.HASH) is True


def test_has_header_false_when_absent():
    assert lh.has_header("print('hi')\n", lh.HASH) is False


def test_has_header_ignores_spdx_far_down_the_file():
    text = "x = 1\n" * 50 + "# SPDX-License-Identifier: AGPL-3.0-or-later\n"
    assert lh.has_header(text, lh.HASH) is False


def test_has_header_ignores_spdx_mentioned_in_prose():
    md = "# Title\n\nWe stamp files with SPDX-License-Identifier: MIT in them.\n"
    assert lh.has_header(md, lh.XML) is False


def test_has_header_detects_xml_block_header():
    md = "<!--\n  SPDX-License-Identifier: AGPL-3.0-or-later\n-->\n# Title\n"
    assert lh.has_header(md, lh.XML) is True


HASH_HEADER = "# SPDX-License-Identifier: AGPL-3.0-or-later\n"


def test_insert_plain_file_goes_to_top_with_blank_line():
    out = lh.insert_header("import os\n", HASH_HEADER)
    assert out == "# SPDX-License-Identifier: AGPL-3.0-or-later\n\nimport os\n"


def test_insert_after_shebang():
    out = lh.insert_header("#!/bin/bash\nset -e\n", HASH_HEADER)
    assert out.startswith("#!/bin/bash\n")
    assert "SPDX-License-Identifier" in out.splitlines()[2]


def test_insert_after_python_encoding_cookie():
    out = lh.insert_header("# -*- coding: utf-8 -*-\nx = 1\n", HASH_HEADER)
    assert out.startswith("# -*- coding: utf-8 -*-\n")
    assert "SPDX-License-Identifier" in out


def test_insert_after_xml_declaration():
    xml_header = "<!--\n  SPDX-License-Identifier: AGPL-3.0-or-later\n-->\n"
    out = lh.insert_header('<?xml version="1.0" ?>\n<Root/>\n', xml_header)
    assert out.startswith('<?xml version="1.0" ?>\n')
    assert "SPDX-License-Identifier" in out
    assert out.rstrip().endswith("<Root/>")


def test_insert_after_html_doctype():
    xml_header = "<!--\n  SPDX-License-Identifier: AGPL-3.0-or-later\n-->\n"
    out = lh.insert_header("<!DOCTYPE html>\n<html></html>\n", xml_header)
    assert out.startswith("<!DOCTYPE html>\n")


def test_insert_preserves_bom():
    out = lh.insert_header("﻿<Project/>\n", "<!--\n  X\n-->\n")
    assert out.startswith("﻿")
    assert out.count("﻿") == 1


def test_insert_is_idempotent_via_has_header():
    once = lh.insert_header("import os\n", HASH_HEADER)
    assert lh.has_header(once, lh.HASH) is True  # real callers guard with has_header


def test_insert_into_empty_file():
    out = lh.insert_header("", HASH_HEADER)
    assert out == "# SPDX-License-Identifier: AGPL-3.0-or-later\n"


def _init_repo(root: Path, files: list[str]) -> None:
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    for rel in files:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)


def test_iter_target_files_respects_roots_and_excludes(tmp_path):
    _init_repo(
        tmp_path,
        [
            "dagster_etl/a.py",
            "dagster_etl/tests/fixtures/case.xml",  # excluded (fixture)
            "dagster_etl/data.json",  # included here; style filter drops it later
            "uv.lock",  # excluded glob
            "notebooks/explore.py",  # excluded tree
            "pyproject.toml",  # included root file
            "README.md",  # excluded (root foundation doc)
            "dagster_etl/docs/README.md",  # NOT excluded (nested README)
        ],
    )
    found = {
        p.relative_to(tmp_path).as_posix()
        for p in lh.iter_target_files(lh.INCLUDE_ROOTS, tmp_path, lh.EXCLUDE_GLOBS)
    }
    assert "dagster_etl/a.py" in found
    assert "pyproject.toml" in found
    assert "dagster_etl/docs/README.md" in found
    assert "dagster_etl/tests/fixtures/case.xml" not in found
    assert "uv.lock" not in found
    assert "notebooks/explore.py" not in found
    assert "README.md" not in found


def test_run_apply_then_check_roundtrip(tmp_path, monkeypatch):
    _init_repo(tmp_path, ["dagster_etl/a.py", "dagster_etl/b.sql"])
    monkeypatch.setattr(lh, "_repo_root", lambda: tmp_path)

    # check before apply -> 2 files missing -> exit 1
    assert lh.main(["--check"]) == 1

    # apply -> stamps both -> exit 0
    assert lh.main(["--apply"]) == 0
    assert lh.has_header((tmp_path / "dagster_etl/a.py").read_text(encoding="utf-8"), lh.HASH)
    assert (
        (tmp_path / "dagster_etl/b.sql")
        .read_text(encoding="utf-8")
        .startswith("-- SPDX-FileCopyrightText:")
    )

    # check after apply -> nothing missing -> exit 0
    assert lh.main(["--check"]) == 0

    # apply again is a no-op (idempotent): file content unchanged
    before = (tmp_path / "dagster_etl/a.py").read_text(encoding="utf-8")
    assert lh.main(["--apply"]) == 0
    assert (tmp_path / "dagster_etl/a.py").read_text(encoding="utf-8") == before


def test_run_skips_json_and_csv(tmp_path, monkeypatch):
    _init_repo(tmp_path, ["dagster_etl/data.json", "dagster_etl/data.csv"])
    monkeypatch.setattr(lh, "_repo_root", lambda: tmp_path)
    assert lh.main(["--check"]) == 0  # nothing stampable -> nothing missing


def test_diff_reports_pending_with_exit_1(tmp_path, monkeypatch, capsys):
    _init_repo(tmp_path, ["dagster_etl/a.py"])
    monkeypatch.setattr(lh, "_repo_root", lambda: tmp_path)
    assert lh.main(["--diff"]) == 1
    out = capsys.readouterr().out
    assert "SPDX-License-Identifier" in out
    # diff must not have written the file
    assert not lh.has_header((tmp_path / "dagster_etl/a.py").read_text(encoding="utf-8"), lh.HASH)


def test_prose_spdx_mention_gets_real_header_applied(tmp_path, monkeypatch):
    """A Markdown file that only mentions SPDX in prose must still receive a real header."""
    # Create a notes.md under dagster_etl/ so it's in INCLUDE_ROOTS and not excluded.
    notes = tmp_path / "dagster_etl" / "notes.md"
    notes.parent.mkdir(parents=True, exist_ok=True)
    notes.write_text(
        "# Notes\n\nFiles are stamped with SPDX-License-Identifier: AGPL-3.0-or-later.\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    monkeypatch.setattr(lh, "_repo_root", lambda: tmp_path)

    # prose mention alone must flag as missing
    assert lh.main(["--check"]) == 1

    # apply must insert a real XML block header
    assert lh.main(["--apply"]) == 0
    content_after = notes.read_text(encoding="utf-8")
    assert content_after.startswith(
        "<!--"
    ), f"Expected XML header block, got: {content_after[:80]!r}"

    # a second apply must be a no-op (idempotent)
    assert lh.main(["--apply"]) == 0
    assert notes.read_text(encoding="utf-8") == content_after


# --- Fix 1: explicit absolute paths must still be excluded ---------------------------


def test_explicit_absolute_paths_still_excluded(tmp_path, monkeypatch):
    """Passing absolute paths to --apply must still honour EXCLUDE_GLOBS."""
    _init_repo(tmp_path, ["LICENSE", "notebooks/x.py"])
    monkeypatch.setattr(lh, "_repo_root", lambda: tmp_path)

    license_path = tmp_path / "LICENSE"
    notebook_path = tmp_path / "notebooks" / "x.py"
    original_license = license_path.read_text(encoding="utf-8")
    original_notebook = notebook_path.read_text(encoding="utf-8")

    rc = lh.main(["--apply", str(license_path), str(notebook_path)])
    assert rc == 0
    assert (
        license_path.read_text(encoding="utf-8") == original_license
    ), "LICENSE must not be stamped"
    assert (
        notebook_path.read_text(encoding="utf-8") == original_notebook
    ), "notebooks/x.py must not be stamped"


# --- Fix 2: multi-line xml/doctype declarations ------------------------------------


def test_insert_after_multiline_xml_declaration():
    xml_header = lh.render_header(lh.XML, lh.HeaderConfig())
    text = '<?xml\n  version="1.0"\n?>\n<Root/>\n'
    out = lh.insert_header(text, xml_header)
    # Full 3-line declaration preserved at top
    assert out.startswith('<?xml\n  version="1.0"\n?>\n'), f"Unexpected start: {out[:60]!r}"
    # Header comes after ?>
    decl_end = out.index("?>") + len("?>")
    header_start = out.index("<!--", decl_end)
    assert header_start > decl_end, "Header must come after ?>"
    assert "SPDX-License-Identifier:" in out[header_start:]
    # Content preserved
    assert "<Root/>" in out


def test_insert_after_multiline_doctype():
    xml_header = lh.render_header(lh.XML, lh.HeaderConfig())
    text = '<!DOCTYPE html\n  PUBLIC "x">\n<html></html>\n'
    out = lh.insert_header(text, xml_header)
    # Full 2-line doctype preserved at top
    assert out.startswith('<!DOCTYPE html\n  PUBLIC "x">\n'), f"Unexpected start: {out[:60]!r}"
    assert "SPDX-License-Identifier:" in out
    assert "<html></html>" in out


def test_insert_unterminated_xml_decl_not_consumed():
    # A malformed <?xml that never closes must NOT swallow the file: the header
    # goes at the very top, before the declaration, not appended at EOF.
    xml_header = lh.render_header(lh.XML, lh.HeaderConfig())
    text = '<?xml version="1.0"\n<Root/>\n<Child/>\n'
    out = lh.insert_header(text, xml_header)
    assert out.startswith("<!--"), f"Header must be at top, got: {out[:40]!r}"
    assert out.index("SPDX-License-Identifier:") < out.index("<?xml")
    assert "<Root/>" in out and "<Child/>" in out


# --- Fix 3: inline single-line XML comment header ---------------------------------


def test_has_header_detects_inline_xml_header():
    assert (
        lh.has_header("<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->\n# T\n", lh.XML) is True
    )


# --- Fix 4: --diff prints pending= to stderr -------------------------------------


def test_diff_prints_pending_to_stderr(tmp_path, monkeypatch, capsys):
    _init_repo(tmp_path, ["dagster_etl/a.py"])
    monkeypatch.setattr(lh, "_repo_root", lambda: tmp_path)
    rc = lh.main(["--diff"])
    assert rc == 1
    captured = capsys.readouterr()
    assert "pending=1" in captured.err
    # summary must not appear on stdout (to avoid polluting piped diff output)
    assert "pending=" not in captured.out


# --- strip_header: exact inverse of insert_header ---------------------------------


def test_strip_inverse_all_styles():
    cases = {
        "py": (lh.HASH, "import os\n"),
        "sql": (lh.DASH, "SELECT 1\n"),
        "cs": (lh.SLASH, "class A {}\n"),
        "md": (lh.XML, "# Title\n\nbody\n"),
    }
    for _name, (style, original) in cases.items():
        h = lh.render_header(style, lh.HeaderConfig())
        assert lh.strip_header(lh.insert_header(original, h), h) == original


def test_strip_inverse_after_shebang():
    original = "#!/bin/bash\nset -e\n"
    h = lh.render_header(lh.HASH, lh.HeaderConfig())
    assert lh.strip_header(lh.insert_header(original, h), h) == original


def test_strip_inverse_after_coding_cookie():
    original = "# -*- coding: utf-8 -*-\nx = 1\n"
    h = lh.render_header(lh.HASH, lh.HeaderConfig())
    assert lh.strip_header(lh.insert_header(original, h), h) == original


def test_strip_inverse_after_xml_declaration():
    original = '<?xml version="1.0" ?>\n<Root/>\n'
    h = lh.render_header(lh.XML, lh.HeaderConfig())
    assert lh.strip_header(lh.insert_header(original, h), h) == original


def test_strip_inverse_preserves_bom():
    original = "﻿<Project/>\n"
    h = lh.render_header(lh.XML, lh.HeaderConfig())
    assert lh.strip_header(lh.insert_header(original, h), h) == original


def test_strip_idempotent_on_unstamped():
    text = "print('hi')\n"
    h = lh.render_header(lh.HASH, lh.HeaderConfig())
    assert lh.strip_header(text, h) == text
    assert lh.strip_header(lh.strip_header(text, h), h) == text


def test_strip_ignores_prose_spdx_mention():
    h = lh.render_header(lh.XML, lh.HeaderConfig())
    md = "# Notes\n\nWe use SPDX-License-Identifier: MIT here.\n"
    assert lh.strip_header(md, h) == md


# --- pre-commit hook injection ----------------------------------------------------


def test_ensure_precommit_hooks_inserts_after_repos():
    cfg = "repos:\n  - repo: foo\n    hooks: []\n"
    out = lh.ensure_precommit_hooks(cfg)
    assert lh._HOOKS_MARKER_START in out
    assert "license-headers-apply" in out
    assert out.index(lh._HOOKS_MARKER_START) < out.index("- repo: foo")


def test_ensure_precommit_hooks_idempotent():
    cfg = "repos:\n  - repo: foo\n    hooks: []\n"
    once = lh.ensure_precommit_hooks(cfg)
    assert lh.ensure_precommit_hooks(once) == once


def test_remove_precommit_hooks_is_inverse_of_ensure():
    cfg = "repos:\n  - repo: foo\n    hooks: []\n"
    assert lh.remove_precommit_hooks(lh.ensure_precommit_hooks(cfg)) == cfg


def test_remove_precommit_hooks_idempotent_when_absent():
    cfg = "repos:\n  - repo: foo\n"
    assert lh.remove_precommit_hooks(cfg) == cfg


def test_ensure_precommit_hooks_cli(tmp_path, monkeypatch):
    cfg = tmp_path / ".pre-commit-config.yaml"
    cfg.write_text("repos:\n  - repo: foo\n", encoding="utf-8")
    monkeypatch.setattr(lh, "_repo_root", lambda: tmp_path)
    assert lh.main(["--ensure-precommit-hooks"]) == 0
    assert "license-headers-apply" in cfg.read_text(encoding="utf-8")


# --- --strip CLI -----------------------------------------------------------------


def test_strip_cli_roundtrip(tmp_path, monkeypatch):
    _init_repo(tmp_path, ["dagster_etl/a.py", "dagster_etl/b.sql"])
    monkeypatch.setattr(lh, "_repo_root", lambda: tmp_path)
    assert lh.main(["--apply"]) == 0
    assert lh.main(["--check"]) == 0
    assert lh.main(["--strip"]) == 0
    assert lh.main(["--check"]) == 1
    assert "SPDX-License-Identifier" not in (tmp_path / "dagster_etl/a.py").read_text(
        encoding="utf-8"
    )


def test_strip_cli_prints_summary(tmp_path, monkeypatch, capsys):
    _init_repo(tmp_path, ["dagster_etl/a.py"])
    monkeypatch.setattr(lh, "_repo_root", lambda: tmp_path)
    lh.main(["--apply"])
    assert lh.main(["--strip"]) == 0
    out = capsys.readouterr().out
    assert "stripped=1" in out


def test_strip_removes_precommit_hook_block(tmp_path, monkeypatch):
    cfg = tmp_path / ".pre-commit-config.yaml"
    cfg.write_text(
        lh.ensure_precommit_hooks("repos:\n  - repo: foo\n    hooks: []\n"),
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    monkeypatch.setattr(lh, "_repo_root", lambda: tmp_path)
    assert lh.main(["--strip"]) == 0
    assert lh._HOOKS_MARKER_START not in cfg.read_text(encoding="utf-8")


def test_strip_inverse_shebang_with_body():
    original = "#!/bin/sh\necho hi\n"
    h = lh.render_header(lh.HASH, lh.HeaderConfig())
    assert lh.strip_header(lh.insert_header(original, h), h) == original


def test_strip_preamble_only_keeps_trailing_newline():
    original = "#!/bin/sh\n"
    h = lh.render_header(lh.HASH, lh.HeaderConfig())
    assert lh.strip_header(lh.insert_header(original, h), h) == original
