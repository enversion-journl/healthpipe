# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Konvertering af talværdi-tillægskoder (uden Originalkode) til beregnede SKS-koder.

IFDB-operationsskemaer bærer tillægskoder uden kode i elementerne
``TillaegskodeHeltal``/``TillaegskodeKommatal`` (+ ``Optionel``-varianter): kun
``ElementVaerdi`` er udfyldt, med dansk decimalkomma. Værdien konverteres til en
beregnet SKS-kode:

- Heltal:   ``VPH`` (>= 0) / ``VNH`` (< 0), samlet længde 7, foranstillede
  nuller, afrunding til 4 cifre; klampes til 9999 ved overløb (99999 -> 9999).
- Kommatal: ``VPK`` (>= 0) / ``VNK`` (< 0), samlet længde 8, ``K`` som
  decimalseparator, foranstillede nuller så cifre + K fylder 5 tegn; heltal i
  kommatal-felter formateres som ,0 (10 -> VPK010K0-form: "010K0").

Reglerne følger referencens kravspecifikation for procedureregistrering
(afsnit 2.1.2.2). Formatet er verificeret mod produktionsdata:
ADM_PROCEDURE_SUPPL_CODES leverer allerede færdige koder (VPH4115, VPH0010).
"""

from decimal import ROUND_HALF_UP, Decimal

_HELTAL_ELEMENT_NAMES = frozenset(
    {
        "TillaegskodeHeltal",
        "TillaegskodeHeltalOptionel",
    }
)
_KOMMATAL_ELEMENT_NAMES = frozenset(
    {
        "TillaegskodeKommatal",
        "TillaegskodeKommatalOptionel",
    }
)

VALUE_TILLAEGSKODE_ELEMENT_NAMES = _HELTAL_ELEMENT_NAMES | _KOMMATAL_ELEMENT_NAMES


def is_value_tillaegskode_element(element_navn: str | None) -> bool:
    """True hvis elementet er en talværdi-tillægskode (skjult tillægskode)."""
    return element_navn in VALUE_TILLAEGSKODE_ELEMENT_NAMES


def _clean_value(raw: object) -> tuple[str, bool] | None:
    if raw is None:
        return None
    text = str(raw).strip().replace(" ", "").replace(" ", "")
    if not text:
        return None
    negative = text.startswith("-")
    if text[0] in "+-":
        text = text[1:]
    # Tolerér punktum som decimaltegn (kilden bruger dansk komma).
    text = text.replace(".", ",")
    if not text or text.count(",") > 1:
        return None
    if not all(ch.isdigit() or ch == "," for ch in text):
        return None
    if text.replace(",", "") == "":
        return None
    return text, negative


def _convert_heltal(text: str, negative: bool) -> str:
    if "," in text:
        int_part, dec_part = text.split(",")
        value = int(int_part or 0)
        if dec_part and int(dec_part[0]) >= 5:
            value += 1
        if value > 9999:
            value = 9999
    else:
        value = int(text)
    digits = str(value)
    if len(digits) > 4:
        divisor = 10 ** (len(digits) - 4)
        value = (value + divisor // 2) // divisor
        if value > 9999:
            value = 9999
    return ("VNH" if negative else "VPH") + str(value).zfill(4)


def _convert_kommatal(text: str, negative: bool) -> str | None:
    int_s, _, dec_s = text.partition(",")
    int_s = str(int(int_s or "0"))
    dec_s = dec_s.rstrip("0") or "0"

    max_dec = 4 - len(int_s)
    if max_dec < 1:
        # Heltalsdelen fylder alle fire cifferpladser — uden for kodeformatets
        # udfaldsrum (kravspec: -999,9..999,9). Hellere ingen kode end en
        # fabrikeret værdi i forkert størrelsesorden.
        return None
    if len(dec_s) > max_dec:
        quantum = Decimal(1).scaleb(-max_dec)
        rounded = Decimal(f"{int_s}.{dec_s}").quantize(quantum, rounding=ROUND_HALF_UP)
        int_s, _, dec_s = format(rounded, "f").partition(".")
        dec_s = (dec_s or "0").rstrip("0") or "0"
        if len(int_s) > 3:
            # ±999,99 afrundes til 1000,0 og droppes; PDF-facit er VPK999K9/VNK999K9.
            return None

    pad_int = 4 - len(dec_s)
    if len(int_s) > pad_int:
        return None
    body = int_s.zfill(pad_int) + "K" + dec_s
    return ("VNK" if negative else "VPK") + body


def convert_value_tillaegskode(element_navn: str | None, element_vaerdi: object) -> str | None:
    """Konvertér et talværdi-element til dets beregnede SKS-kode.

    Returnerer ``None`` for ukendte elementnavne, tomme/ikke-numeriske værdier
    og kommatal uden for formatets udfaldsrum — kaldestedet afgør håndteringen.
    """
    if element_navn not in VALUE_TILLAEGSKODE_ELEMENT_NAMES:
        return None
    cleaned = _clean_value(element_vaerdi)
    if cleaned is None:
        return None
    text, negative = cleaned
    if element_navn in _HELTAL_ELEMENT_NAMES:
        return _convert_heltal(text, negative)
    return _convert_kommatal(text, negative)
