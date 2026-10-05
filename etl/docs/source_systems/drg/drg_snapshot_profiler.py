#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DRG snapshot profiler — stdlib-only profiling of DRG-grupperet LPR deliveries from SDS.

Profiles the tar.bz2 deliveries on the NGC share (see feature AB#27624) without
loading anything into a database. Built for the key questions behind US AB#29509:
which columns are stable natural keys, how deliveries differ across cadence (M/U)
and takstsystem, and whether the SDS export log's control totals hold.

Memory is deliberately capped: every uniqueness/join computation is bucketed to
disk (64 md5 buckets) so no pass holds more than a few hundred thousand entries
in memory at once. A full somatik delivery (~48M rows, ~16.5 GB CSV) profiles in
minutes on the code-server allocation. GDPR: CPR columns are never read, dumped,
logged or printed — only counts appear in output.

Subcommands
    inventory  Scan every Region_1083_*.tar.bz2 in a folder and print each
               delivery's SDP_ExportLog row counts (reads only the first few MB
               of each archive — metadata and export log precede the data files).
    profile    Profile ONE unpacked delivery folder (the T2x/ folder):
               control totals vs. export log, key uniqueness, empty rates,
               aktivitetsaar distribution, functional-dependency candidates for
               dim.DRG, forloeb identity via ind-kontakt UUID, diagnose composite
               key. Writes key dumps for later cross-delivery comparison.
    compare    Compare two `profile --dump-dir` outputs: key overlap, ID
               renumbering, attribute drift, forloeb re-keying. Works both for
               same-takstsystem (stability) and cross-takstsystem (overlap).

Typical run on the code-server allocation (mount: /mnt/ftproot/...):
    python3 drg_snapshot_profiler.py inventory --base-dir "$DRG_BASE_DIR"
    python3 drg_snapshot_profiler.py profile --dir /tmp/x/T26 --dump-dir /tmp/x/dump
    python3 drg_snapshot_profiler.py compare --a /tmp/a/dump --b /tmp/b/dump
"""

from __future__ import annotations

import argparse
import csv
import glob
import gzip
import hashlib
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
from collections import Counter

ENCODING = "cp1252"
DELIMITER = ";"
# 64 buckets holder hukommelsen pr. bucket-dict lav (25M raekker / 64 < 500K)
# uden at aabne flere filhaandtag end en typisk ulimit tillader, selv med
# flere samtidige writers i kontakt-passet.
N_BUCKETS = 64

# Region_1083_DRG_T26_U_20260824190546.tar.bz2 / Region_1083_DRGPSYK_T24_M_2025....
ARCHIVE_RE = re.compile(
    r"^Region_1083_(?P<leverancetype>DRG|DRGPSYK)_(?P<takstsystem>T\d{2})"
    r"(?:_(?P<kadence>[MU]))?_(?P<timestamp>\d{14})\.tar\.bz2$"
)

csv.field_size_limit(10**9)


def log(msg: str) -> None:
    print(msg, flush=True)


def iter_rows(path: str):
    with open(path, encoding=ENCODING, newline="") as fh:
        yield from csv.DictReader(fh, delimiter=DELIMITER)


def count_rows(path: str) -> int:
    """CSV-aware row count (quoted fields may in principle contain newlines).

    Blank records are skipped to match DictReader's behaviour, and an empty
    file counts as 0 rows rather than -1.
    """
    n = 0
    with open(path, encoding=ENCODING, newline="") as fh:
        for record in csv.reader(fh, delimiter=DELIMITER):
            if record:
                n += 1
    return max(n - 1, 0)  # header


def clean_field(value: str) -> str:
    """Make a field value safe for the tab/newline-delimited dump format.

    The source CSV quotes every field, so tabs and newlines are legal in the
    data even if machine-generated values make them unlikely.
    """
    if "\t" in value or "\n" in value or "\r" in value:
        return value.replace("\t", " ").replace("\n", " ").replace("\r", " ")
    return value


def find_one(directory: str, pattern: str) -> str | None:
    hits = sorted(glob.glob(os.path.join(directory, pattern)))
    return hits[0] if hits else None


def bucket_of(key: str) -> int:
    return (
        int(hashlib.md5(key.encode("utf-8", "replace"), usedforsecurity=False).hexdigest()[:2], 16)
        % N_BUCKETS
    )


class BucketWriter:
    """Append `key<TAB>value` lines to N_BUCKETS disk buckets keyed by md5(key)."""

    def __init__(self, work_dir: str, name: str):
        self.dir = os.path.join(work_dir, name)
        os.makedirs(self.dir, exist_ok=True)
        self._handles = [
            open(os.path.join(self.dir, f"{i:02x}"), "a", encoding="utf-8")
            for i in range(N_BUCKETS)
        ]

    def add(self, key: str, value: str = "") -> None:
        key = clean_field(key)
        self._handles[bucket_of(key)].write(f"{key}\t{value}\n")

    def close(self) -> None:
        for h in self._handles:
            h.close()

    def bucket_paths(self) -> list[str]:
        return [os.path.join(self.dir, f"{i:02x}") for i in range(N_BUCKETS)]

    def distinct(self) -> int:
        total = 0
        for p in self.bucket_paths():
            seen: set[str] = set()
            with open(p, encoding="utf-8") as fh:
                for line in fh:
                    seen.add(line.split("\t", 1)[0])
            total += len(seen)
        return total


def read_bucket(path: str) -> dict[str, str]:
    out: dict[str, str] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            key, _, value = line.rstrip("\n").partition("\t")
            out[key] = value
    return out


def uniq_line(label: str, distinct: int, total: int) -> str:
    dup = total - distinct
    verdict = "UNIK" if dup == 0 else f"{dup:,} dubletter"
    return f"  {label}: {distinct:,} distinkte af {total:,} -> {verdict}"


# ---------------------------------------------------------------------------
# inventory
# ---------------------------------------------------------------------------


def read_exportlog_from_archive(path: str) -> tuple[dict[str, int], list[str]]:
    """Stream-read an archive until the SDP_ExportLog member has been consumed.

    Metadata files and the export log alphabetically precede the data files in
    the tar, so this reads only the first few MB of a multi-GB archive.
    Returns ({data filename -> row count}, [member names seen]).
    """
    rows: dict[str, int] = {}
    seen: list[str] = []
    with tarfile.open(path, mode="r|bz2") as tar:
        for member in tar:
            base = os.path.basename(member.name)
            seen.append(base)
            if base.startswith("SDP_ExportLog"):
                fh = tar.extractfile(member)
                if fh is None:  # pragma: no cover - defensive
                    break
                text = io_text(fh.read())
                for rec in csv.DictReader(text.splitlines(), delimiter=DELIMITER):
                    rows[rec["FilNavn"]] = int(rec["AntalRaekker"])
                break
    return rows, seen


def io_text(raw: bytes) -> str:
    return raw.decode(ENCODING)


def cmd_inventory(args: argparse.Namespace) -> None:
    paths = sorted(glob.glob(os.path.join(args.base_dir, "Region_1083_*.tar.bz2")))
    if args.include_arkiv:
        paths += sorted(glob.glob(os.path.join(args.base_dir, "Arkiv", "Region_1083_*.tar.bz2")))
    if not paths:
        log(f"Ingen leverancer fundet i {args.base_dir}")
        return
    result = []
    for path in paths:
        name = os.path.basename(path)
        m = ARCHIVE_RE.match(name)
        if not m:
            log(f"SPRINGER OVER (uventet filnavn): {name}")
            continue
        try:
            rows, _ = read_exportlog_from_archive(path)
        except (tarfile.TarError, OSError, KeyError, ValueError) as exc:
            log(f"{name}: KUNNE IKKE LAESES ({exc})")
            continue
        data_rows = {f: n for f, n in rows.items() if not f.startswith("Metadata_")}
        entry = {
            "fil": name,
            "leverancetype": m["leverancetype"],
            "takstsystem": m["takstsystem"],
            "kadence": m["kadence"] or "-",
            "timestamp": m["timestamp"],
            "bytes": os.path.getsize(path),
            "datasets": data_rows,
        }
        result.append(entry)
        total = sum(data_rows.values())
        log(
            f"{name}  [{m['leverancetype']} {m['takstsystem']} {m['kadence'] or '-'}]  {len(data_rows)} datasaet, {total:,} raekker"
        )
        for f, n in sorted(data_rows.items()):
            log(f"    {n:>12,}  {f}")
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(result, fh, ensure_ascii=False, indent=1)
        log(f"Skrevet: {args.out}")


# ---------------------------------------------------------------------------
# profile
# ---------------------------------------------------------------------------


def verify_exportlog(directory: str) -> dict:
    """Count every data file with a CSV-aware reader and compare to SDP_ExportLog."""
    logpath = find_one(directory, "SDP_ExportLog_*.csv")
    if not logpath:
        return {"status": "SDP_ExportLog mangler"}
    expected = {}
    for rec in iter_rows(logpath):
        if not rec["FilNavn"].startswith("Metadata_"):
            expected[rec["FilNavn"]] = int(rec["AntalRaekker"])
    deviations = {}
    for fname, exp in sorted(expected.items()):
        fpath = os.path.join(directory, fname)
        if not os.path.exists(fpath):
            deviations[fname] = {"forventet": exp, "fundet": None}
            continue
        actual = count_rows(fpath)
        status = "OK" if actual == exp else "AFVIGER"
        log(f"  {status}: {fname}: exportlog {exp:,} / talt {actual:,}")
        if actual != exp:
            deviations[fname] = {"forventet": exp, "fundet": actual}
    on_disk = {
        os.path.basename(f)
        for f in glob.glob(os.path.join(directory, "T2*_*.csv"))
        if not os.path.basename(f).startswith("Metadata_")
    }
    unlisted = sorted(on_disk - set(expected))
    for fname in unlisted:
        log(f"  IKKE I EKSPORTLOG: {fname}")
    return {"filer": len(expected), "afvigelser": deviations, "ikke_i_eksportlog": unlisted}


class FdCollector:
    """Track, per DRG code, which candidate columns stay constant.

    Used to decide which kontakt attributes can move to dim.DRG (grain:
    takstsystem x DRG) without exploding its cardinality.
    """

    CAP = 25

    def __init__(self, columns: list[str]):
        self.columns = columns
        self.values: dict[str, dict[str, set[str]]] = {}

    def add(self, drg: str, row: dict) -> None:
        per_drg = self.values.setdefault(drg, {c: set() for c in self.columns})
        for c in self.columns:
            vals = per_drg[c]
            if len(vals) <= self.CAP:
                vals.add(row.get(c, ""))

    def report(self) -> dict:
        out = {}
        for c in self.columns:
            multi = sum(1 for per in self.values.values() if len(per[c]) > 1)
            capped = sum(1 for per in self.values.values() if len(per[c]) > self.CAP)
            out[c] = {
                "drg_koder": len(self.values),
                "med_flere_vaerdier": multi,
                "over_cap": capped,
            }
        return out


def profile_somatik(directory: str, dump_dir: str, work: str, skip_procedure: bool) -> dict:
    res: dict = {"leverancetype": "DRG"}
    kontakt_path = find_one(directory, "T2*_DRGKONTAKT_*.csv")
    forloeb_path = find_one(directory, "T2*_DRGSGHFORLOB_*.csv")
    diagnose_path = find_one(directory, "T2*_DIAGNOSER_1083_*.csv")
    procedure_path = find_one(directory, "T2*_PROCEDURE_1083_*.csv")
    info_path = find_one(directory, "T2*_LEVERANCEINFO_*.csv")
    missing_files = [
        navn
        for navn, sti in [
            ("DRGSGHFORLOB", forloeb_path),
            ("DIAGNOSER", diagnose_path),
            ("PROCEDURE", procedure_path),
        ]
        if sti is None
    ]
    if missing_files:
        sys.exit(f"Datafiler mangler i {directory}: {', '.join(missing_files)}")
    if info_path:
        info = next(iter_rows(info_path), None)
        res["leveranceinfo"] = info
        log(f"  LEVERANCEINFO: {info if info is not None else '(tom fil)'}")

    log("== DRGKONTAKT ==")
    # Skemaet varierer mellem takstsystemer (T25 mangler fx DW_EK_HelbredsForloeb
    # og FORLOB_REFERENCE_*) — de valgfrie kolonner behandles som tomme og
    # rapporteres, saa afvigelsen er synlig i output.
    with open(kontakt_path, encoding=ENCODING, newline="") as fh:
        header = next(csv.reader(fh, delimiter=DELIMITER))
    optional_missing = [
        c
        for c in ("DW_EK_HelbredsForloeb", "FORLOB_REFERENCE_VAERDI", "FORLOB_REFERENCE_TYPE")
        if c not in header
    ]
    if optional_missing:
        log(f"  OBS: kolonner findes ikke i dette takstsystems skema: {optional_missing}")
    res["kontakt_kolonner_mangler"] = optional_missing
    ids = BucketWriter(work, "k_ids")
    kvs = BucketWriter(work, "k_kv")
    fvs = BucketWriter(work, "k_fv")
    fv_hf = BucketWriter(work, "k_fv_hf")
    sf_fv = BucketWriter(work, "k_sf_fv")
    id2kv = BucketWriter(work, "k_id2kv")
    n = kv_empty = fv_empty = frv_empty = flag_uenige = mdc_mismatch = 0
    aar: Counter = Counter()
    frt: Counter = Counter()
    fd = FdCollector(["TRIMPUNKT", "PRIS_DRG", "MDC", "DRG_GROUPER", "LANGLIGGERPRIS"])
    with gzip.open(os.path.join(dump_dir, "kontakt.tsv.gz"), "wt", encoding="utf-8") as dump:
        for row in iter_rows(kontakt_path):
            n += 1
            rid = row["ID"]
            kv = row["KONTAKT_VAERDI"].strip()
            fv = row["FORLOB_VAERDI"].strip()
            hf = (row.get("DW_EK_HelbredsForloeb") or "").strip()
            drg = row["DRG"].strip()
            ids.add(rid)
            if kv:
                kvs.add(kv)
                dump.write(
                    "\t".join(
                        clean_field(f)
                        for f in [
                            kv,
                            rid,
                            row["AKTIVITETSAAR"],
                            drg,
                            row["TOTALPRIS_DRG"],
                            row["INDTIDSPUNKT_DRGKONTAKT"],
                            row["UDTIDSPUNKT_DRGKONTAKT"] or "",
                            hf,
                        ]
                    )
                    + "\n"
                )
            else:
                kv_empty += 1
            if fv:
                fvs.add(fv)
                fv_hf.add(f"{fv}|{hf}")
            else:
                fv_empty += 1
            if not (row.get("FORLOB_REFERENCE_VAERDI") or "").strip():
                frv_empty += 1
            frt[(row.get("FORLOB_REFERENCE_TYPE") or "").strip() or "(tom)"] += 1
            aar[row["AKTIVITETSAAR"]] += 1
            if row["TUNGESTE_DRGKONTAKT"] != row["UDFALDSGIVENDE_KONTAKT"]:
                flag_uenige += 1
            sfid = row["DRGSGHFORLOB_ID"].strip()
            if sfid:
                sf_fv.add(f"{sfid}|{fv}")
            id2kv.add(rid.strip(), kv)
            if drg and row["MDC"].strip() and drg[:2] != row["MDC"].strip():
                mdc_mismatch += 1
            if drg:
                fd.add(drg, row)
            if n % 2_000_000 == 0:
                log(f"  ... {n:,} raekker")
    for b in (ids, kvs, fvs, fv_hf, sf_fv, id2kv):
        b.close()
    log(f"raekker: {n:,}")
    log(uniq_line("ID", ids.distinct(), n))
    log(uniq_line("KONTAKT_VAERDI (ikke-tomme)", kvs.distinct(), n - kv_empty))
    log(
        f"  tomme: KONTAKT_VAERDI {kv_empty:,} | FORLOB_VAERDI {fv_empty:,} | FORLOB_REFERENCE_VAERDI {frv_empty:,}"
    )
    log(f"  AKTIVITETSAAR: {dict(sorted(aar.items()))}")
    log(f"  TUNGESTE != UDFALDSGIVENDE paa {flag_uenige:,} raekker")
    log(f"  FORLOB_REFERENCE_TYPE: {dict(frt.most_common(10))}")
    log(f"  MDC != DRG[0:2] paa {mdc_mismatch:,} raekker")
    n_fv, n_fvhf = fvs.distinct(), fv_hf.distinct()
    log(
        f"  distinkte FORLOB_VAERDI: {n_fv:,} | distinkte (FORLOB_VAERDI, DW_EK_HelbredsForloeb)-par: {n_fvhf:,}"
    )
    n_sffv = sf_fv.distinct()
    fd_rep = fd.report()
    log("  FD-analyse (kandidater til dim.DRG — antal DRG-koder hvor kolonnen IKKE er konstant):")
    for col, stat in fd_rep.items():
        log(
            f"    {col}: {stat['med_flere_vaerdier']:,} af {stat['drg_koder']:,} DRG-koder har >1 vaerdi"
        )
    res["kontakt"] = {
        "raekker": n,
        "id_distinkte": ids.distinct(),
        "kv_distinkte": kvs.distinct(),
        "kv_tomme": kv_empty,
        "fv_tomme": fv_empty,
        "frv_tomme": frv_empty,
        "aktivitetsaar": dict(aar),
        "flag_uenige": flag_uenige,
        "mdc_mismatch": mdc_mismatch,
        "forlob_reference_type": dict(frt),
        "fv_distinkte": n_fv,
        "fv_hf_par": n_fvhf,
        "sf_fv_par": n_sffv,
        "fd": fd_rep,
    }

    log("== DRGSGHFORLOB ==")
    f_ids = BucketWriter(work, "f_ids")
    f_by_ind = BucketWriter(work, "f_by_ind")
    n_f = 0
    for row in iter_rows(forloeb_path):
        n_f += 1
        f_ids.add(row["ID"])
        f_by_ind.add(
            row["DRGKONTAKT_ID_IND"].strip(),
            "\t".join(
                clean_field(f)
                for f in [
                    row["ID"],
                    row["DRG_DRGSGHF"],
                    row["ANTAL_I_FORLOB"],
                    row["TOTALPRIS_DRGSGHF"],
                    row["AKTIVITETSAAR"],
                    row["DRGKONTAKT_ID_UD"].strip(),
                ]
            ),
        )
        if n_f % 2_000_000 == 0:
            log(f"  ... {n_f:,} raekker")
    f_ids.close()
    f_by_ind.close()
    log(f"raekker: {n_f:,}")
    log(uniq_line("ID", f_ids.distinct(), n_f))
    log(
        f"  (DRGSGHFORLOB_ID, FORLOB_VAERDI)-par blandt kontakter: {n_sffv:,} vs. forloebsgrupper — se kontakt-sektionen"
    )

    # Join: forloeb.ind-kontakt-ID -> kontakt.KONTAKT_VAERDI => forloebsnoegle
    ind_uuid = BucketWriter(work, "f_ind_uuid")
    ind_missing = 0
    with gzip.open(os.path.join(dump_dir, "forloeb.tsv.gz"), "wt", encoding="utf-8") as dump:
        for kb, fb in zip(id2kv.bucket_paths(), f_by_ind.bucket_paths(), strict=True):
            lookup = read_bucket(kb)
            with open(fb, encoding="utf-8") as fh:
                for line in fh:
                    ind_id, _, rest = line.rstrip("\n").partition("\t")
                    kv = lookup.get(ind_id, "")
                    if kv:
                        ind_uuid.add(kv)
                        dump.write(f"{kv}\t{rest}\n")
                    else:
                        ind_missing += 1
    ind_uuid.close()
    n_ind_uuid = ind_uuid.distinct()
    log(uniq_line("forloebsnoegle = ind-kontaktens KONTAKT_VAERDI", n_ind_uuid, n_f - ind_missing))
    log(f"  ind-kontakt-ID uden match (eller med tom KONTAKT_VAERDI) i DRGKONTAKT: {ind_missing:,}")
    res["forloeb"] = {
        "raekker": n_f,
        "id_distinkte": f_ids.distinct(),
        "ind_uuid_distinkte": n_ind_uuid,
        "ind_uden_match": ind_missing,
    }

    log("== DIAGNOSER ==")
    comp = BucketWriter(work, "d_comp")
    n_d = dv_empty = 0
    art: Counter = Counter()
    for row in iter_rows(diagnose_path):
        n_d += 1
        if not row["DIAGNOSE_VAERDI"].strip():
            dv_empty += 1
        art[row["ART"].strip()] += 1
        comp.add(
            "|".join(
                [
                    row["DRGKONTAKT_ID"] or "",
                    row["DIAGNOSE"] or "",
                    row["ART"] or "",
                    row["SIDEANGIVELSE"] or "",
                ]
            )
        )
        if n_d % 4_000_000 == 0:
            log(f"  ... {n_d:,} raekker")
    comp.close()
    n_comp = comp.distinct()
    log(f"raekker: {n_d:,} | DIAGNOSE_VAERDI tomme: {dv_empty:,}")
    log(uniq_line("komposit (kontakt_id, diagnose, art, side)", n_comp, n_d))
    log(f"  ART: {dict(art.most_common(8))}")
    res["diagnoser"] = {
        "raekker": n_d,
        "vaerdi_tomme": dv_empty,
        "komposit_distinkte": n_comp,
        "art": dict(art),
    }

    if not skip_procedure:
        log("== PROCEDURE ==")
        pv = BucketWriter(work, "p_v")
        n_p = pv_empty = 0
        for row in iter_rows(procedure_path):
            n_p += 1
            v = row["PROCEDURE_VAERDI"].strip()
            if v:
                pv.add(v)
            else:
                pv_empty += 1
            if n_p % 5_000_000 == 0:
                log(f"  ... {n_p:,} raekker")
        pv.close()
        log(f"raekker: {n_p:,} | PROCEDURE_VAERDI tomme: {pv_empty:,}")
        log(uniq_line("PROCEDURE_VAERDI (ikke-tomme)", pv.distinct(), n_p - pv_empty))
        res["procedure"] = {
            "raekker": n_p,
            "vaerdi_tomme": pv_empty,
            "vaerdi_distinkte": pv.distinct(),
        }
    return res


def profile_psyk(directory: str, dump_dir: str, work: str) -> dict:
    res: dict = {"leverancetype": "DRGPSYK"}
    kontakt_path = find_one(directory, "T2*_Psyk_kontakt_*.csv")
    forloeb_path = find_one(directory, "T2*_Psyk_forloeb_*.csv")
    diagnose_path = find_one(directory, "T2*_Psyk_Diagnose_1083_*.csv")
    if forloeb_path is None or diagnose_path is None:
        sys.exit(f"Psyk_forloeb- eller Psyk_Diagnose-fil mangler i {directory}")

    log("== PSYK KONTAKT ==")
    kvs = BucketWriter(work, "pk_kv")
    dwek2kv = BucketWriter(work, "pk_dwek2kv")
    n = kv_empty = 0
    with gzip.open(os.path.join(dump_dir, "kontakt.tsv.gz"), "wt", encoding="utf-8") as dump:
        for row in iter_rows(kontakt_path):
            n += 1
            kv = row["KONTAKT_VAERDI"].strip()
            if not kv:
                kv_empty += 1
                continue
            kvs.add(kv)
            dwek2kv.add(row["DW_EK_Kontakt"].strip(), kv)
            dump.write(
                "\t".join(
                    clean_field(f)
                    for f in [
                        kv,
                        row["ID_INT"],
                        "",
                        "",
                        "",
                        row["INDTIDSPUNKT_KONTAKT"],
                        row["UDTIDSPUNKT_KONTAKT"] or "",
                        row["DW_EK_Helbredsforloeb"],
                    ]
                )
                + "\n"
            )
    kvs.close()
    dwek2kv.close()
    log(f"raekker: {n:,}")
    log(uniq_line("KONTAKT_VAERDI (ikke-tomme)", kvs.distinct(), n - kv_empty))
    res["kontakt"] = {"raekker": n, "kv_distinkte": kvs.distinct(), "kv_tomme": kv_empty}

    log("== PSYK FORLOEB ==")
    f_by_foerste = BucketWriter(work, "pf_by_foerste")
    n_f = 0
    hf = BucketWriter(work, "pf_hf")
    for row in iter_rows(forloeb_path):
        n_f += 1
        hf.add(row["DW_EK_Helbredsforloeb"])
        f_by_foerste.add(
            row["DW_EK_Kontakt_foerste"].strip(),
            "\t".join(
                clean_field(f)
                for f in [
                    row["PsykiatriForloeb_ID"],
                    row["FORLOEB_TYPE"],
                    row["ANTAL_I_FORLOEB"],
                    row["PRIS_PsykiatriForloeb"],
                    "",
                    "",
                ]
            ),
        )
    f_by_foerste.close()
    hf.close()
    first_uuid = BucketWriter(work, "pf_first_uuid")
    missing = 0
    with gzip.open(os.path.join(dump_dir, "forloeb.tsv.gz"), "wt", encoding="utf-8") as dump:
        for kb, fb in zip(dwek2kv.bucket_paths(), f_by_foerste.bucket_paths(), strict=True):
            lookup = read_bucket(kb)
            with open(fb, encoding="utf-8") as fh:
                for line in fh:
                    key, _, rest = line.rstrip("\n").partition("\t")
                    kv = lookup.get(key, "")
                    if kv:
                        first_uuid.add(kv)
                        dump.write(f"{kv}\t{rest}\n")
                    else:
                        missing += 1
    first_uuid.close()
    log(f"raekker: {n_f:,} | distinkte DW_EK_Helbredsforloeb: {hf.distinct():,}")
    log(
        uniq_line(
            "forloebsnoegle = foerste kontakts KONTAKT_VAERDI", first_uuid.distinct(), n_f - missing
        )
    )
    log(f"  DW_EK_Kontakt_foerste uden match i kontakttabellen: {missing:,}")
    res["forloeb"] = {
        "raekker": n_f,
        "hf_distinkte": hf.distinct(),
        "foerste_uuid_distinkte": first_uuid.distinct(),
        "foerste_uden_match": missing,
    }

    log("== PSYK DIAGNOSE ==")
    comp = BucketWriter(work, "pd_comp")
    n_d = 0
    for row in iter_rows(diagnose_path):
        n_d += 1
        comp.add(
            "|".join(
                [
                    row["Kontakt_ID_INT"] or "",
                    row["Diagnose"] or "",
                    row["ART"] or "",
                    row["SIDEANGIVELSE"] or "",
                ]
            )
        )
    comp.close()
    log(f"raekker: {n_d:,}")
    log(uniq_line("komposit (kontakt_id, diagnose, art, side)", comp.distinct(), n_d))
    res["diagnoser"] = {"raekker": n_d, "komposit_distinkte": comp.distinct()}
    return res


def cmd_profile(args: argparse.Namespace) -> None:
    directory = args.dir.rstrip("/")
    dump_dir = args.dump_dir or os.path.join(directory, "..", "dump")
    os.makedirs(dump_dir, exist_ok=True)
    for stale in ("kontakt.tsv.gz", "forloeb.tsv.gz", "profile.json"):
        try:
            os.remove(os.path.join(dump_dir, stale))
        except FileNotFoundError:
            pass
    work = tempfile.mkdtemp(prefix="drgprof_", dir=args.work_dir)
    try:
        if args.skip_exportlog:
            exportlog = {"status": "sprunget over (--skip-exportlog)"}
        else:
            log(f"=== EKSPORTLOG-VERIFIKATION: {directory} ===")
            exportlog = verify_exportlog(directory)
        if find_one(directory, "T2*_DRGKONTAKT_*.csv"):
            res = profile_somatik(directory, dump_dir, work, args.skip_procedure)
        elif find_one(directory, "T2*_Psyk_kontakt_*.csv"):
            res = profile_psyk(directory, dump_dir, work)
        else:
            sys.exit(f"Hverken DRGKONTAKT- eller Psyk_kontakt-fil fundet i {directory}")
        res["exportlog"] = exportlog
        res["dir"] = directory
        with open(os.path.join(dump_dir, "profile.json"), "w", encoding="utf-8") as fh:
            json.dump(res, fh, ensure_ascii=False, indent=1)
        log(f"Dumps + profile.json skrevet til {dump_dir}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ---------------------------------------------------------------------------
# compare
# ---------------------------------------------------------------------------


def split_dump(path: str, work: str, name: str) -> BucketWriter:
    bw = BucketWriter(work, name)
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            key, _, rest = line.rstrip("\n").partition("\t")
            bw.add(key, rest)
    bw.close()
    return bw


def compare_dumps(a_path: str, b_path: str, work: str, name: str, fields: list[str]) -> dict:
    """Bucket-join two key dumps; count overlap and per-field drift among common keys."""
    a = split_dump(a_path, work, f"{name}_a")
    b = split_dump(b_path, work, f"{name}_b")
    common = only_a = only_b = 0
    drift: Counter = Counter()
    only_a_keys = BucketWriter(work, f"{name}_onlya")
    for pa, pb in zip(a.bucket_paths(), b.bucket_paths(), strict=True):
        da, db = read_bucket(pa), read_bucket(pb)
        for key, va in da.items():
            vb = db.get(key)
            if vb is None:
                only_a += 1
                only_a_keys.add(key)
                continue
            common += 1
            fa, fb_ = va.split("\t"), vb.split("\t")
            for i, fname in enumerate(fields):
                if i < len(fa) and i < len(fb_) and fa[i] != fb_[i]:
                    drift[fname] += 1
        only_b += sum(1 for key in db if key not in da)
    only_a_keys.close()
    return {
        "faelles": common,
        "kun_a": only_a,
        "kun_b": only_b,
        "aendret": dict(drift),
        "_only_a_dir": only_a_keys.dir,
    }


def cmd_compare(args: argparse.Namespace) -> None:
    work = tempfile.mkdtemp(prefix="drgcmp_", dir=args.work_dir)
    try:
        label = args.label or f"{args.a} vs {args.b}"
        log(f"=== SAMMENLIGNING: {label} ===")
        kontakt_fields = [
            "ID",
            "AKTIVITETSAAR",
            "DRG",
            "TOTALPRIS",
            "INDTID",
            "UDTID",
            "DW_EK_HelbredsForloeb",
        ]
        k = compare_dumps(
            os.path.join(args.a, "kontakt.tsv.gz"),
            os.path.join(args.b, "kontakt.tsv.gz"),
            work,
            "k",
            kontakt_fields,
        )
        log("KONTAKT (noegle: KONTAKT_VAERDI)")
        log(f"  faelles: {k['faelles']:,} | kun A: {k['kun_a']:,} | kun B: {k['kun_b']:,}")
        for f, c in k["aendret"].items():
            log(f"  {f} aendret blandt faelles: {c:,}")

        forloeb_fields = [
            "FORLOB_ID",
            "DRG/TYPE",
            "ANTAL",
            "TOTALPRIS/PRIS",
            "AKTIVITETSAAR",
            "UD_KONTAKT_ID",
        ]
        f = compare_dumps(
            os.path.join(args.a, "forloeb.tsv.gz"),
            os.path.join(args.b, "forloeb.tsv.gz"),
            work,
            "f",
            forloeb_fields,
        )
        log("FORLOEB (noegle: ind-/foerste kontakts KONTAKT_VAERDI)")
        log(f"  faelles: {f['faelles']:,} | kun A: {f['kun_a']:,} | kun B: {f['kun_b']:,}")
        for fname, c in f["aendret"].items():
            log(f"  {fname} aendret blandt faelles: {c:,}")

        # Re-keying: forloeb only in A whose ind-kontakt still exists in B's kontakter.
        bk = split_dump(os.path.join(args.b, "kontakt.tsv.gz"), work, "bk_keys")
        rekeyed = 0
        for pa, pb in zip(
            sorted(glob.glob(os.path.join(f["_only_a_dir"], "*"))), bk.bucket_paths(), strict=True
        ):
            keys_b = set(read_bucket(pb))
            with open(pa, encoding="utf-8") as fh:
                rekeyed += sum(1 for line in fh if line.split("\t", 1)[0] in keys_b)
        log(
            f"  forloeb kun i A hvis ind-kontakt STADIG findes i B (re-keyet forloeb): {rekeyed:,} af {f['kun_a']:,}"
        )
        result = {"label": label, "kontakt": k, "forloeb": f, "forloeb_rekeyet": rekeyed}
        for section in (result["kontakt"], result["forloeb"]):
            section.pop("_only_a_dir", None)
        if args.out:
            with open(args.out, "w", encoding="utf-8") as fh:
                json.dump(result, fh, ensure_ascii=False, indent=1)
            log(f"Skrevet: {args.out}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_inv = sub.add_parser("inventory", help="Skan alle leverancer og udskriv eksportlog-indhold")
    p_inv.add_argument("--base-dir", required=True)
    p_inv.add_argument("--include-arkiv", action="store_true")
    p_inv.add_argument("--out", help="Skriv resultatet som JSON")
    p_inv.set_defaults(func=cmd_inventory)

    p_prof = sub.add_parser("profile", help="Profilér én udpakket leverance (T2x-mappen)")
    p_prof.add_argument(
        "--dir", required=True, help="Udpakket leverancemappe (indeholder CSV-filerne)"
    )
    p_prof.add_argument(
        "--dump-dir", help="Hvor noegledumps + profile.json skrives (default: <dir>/../dump)"
    )
    p_prof.add_argument(
        "--work-dir", default=None, help="Midlertidige bucket-filer (default: systemets tmp)"
    )
    p_prof.add_argument(
        "--skip-procedure",
        action="store_true",
        help="Spring PROCEDURE-analysen over (25M+ raekker)",
    )
    p_prof.add_argument(
        "--skip-exportlog",
        action="store_true",
        help="Spring eksportlog-verifikationen over (sparer et fuldt gennemlaes)",
    )
    p_prof.set_defaults(func=cmd_profile)

    p_cmp = sub.add_parser("compare", help="Sammenlign to profile-dumps (stabilitet/overlap)")
    p_cmp.add_argument(
        "--a", required=True, help="dump-dir for leverance A (den aeldste/reference)"
    )
    p_cmp.add_argument("--b", required=True, help="dump-dir for leverance B")
    p_cmp.add_argument("--label")
    p_cmp.add_argument("--work-dir", default=None)
    p_cmp.add_argument("--out", help="Skriv resultatet som JSON")
    p_cmp.set_defaults(func=cmd_compare)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
