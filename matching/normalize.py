"""Name and address normalization shared by the vendor master build and the matcher."""

import re

# Map long forms and punctuation variants to one canonical legal suffix.
_NAME_TOKENS = {
    "INCORPORATED": "INC",
    "CORPORATION": "CORP",
    "COMPANY": "CO",
    "LIMITED": "LTD",
    "LLC": "LLC",
    "L L C": "LLC",
    "L L P": "LLP",
    "AND": "&",
}

_ADDR_TOKENS = {
    "STREET": "ST", "AVENUE": "AVE", "ROAD": "RD", "DRIVE": "DR", "BOULEVARD": "BLVD",
    "LANE": "LN", "COURT": "CT", "PLACE": "PL", "PARKWAY": "PKWY", "HIGHWAY": "HWY",
    "SUITE": "STE", "BUILDING": "BLDG", "FLOOR": "FL", "ROOM": "RM", "APARTMENT": "APT",
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    "NORTHEAST": "NE", "NORTHWEST": "NW", "SOUTHEAST": "SE", "SOUTHWEST": "SW",
    "POST OFFICE BOX": "PO BOX", "P O BOX": "PO BOX",
}


def _clean(text: str | None) -> str:
    if text is None:
        return ""
    s = str(text).upper()
    s = s.replace(".", "")  # L.L.C. -> LLC, P.O. -> PO
    s = s.replace("&", " & ")
    s = re.sub(r"[^A-Z0-9& ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _replace_tokens(s: str, table: dict[str, str]) -> str:
    # Multi-word keys first so "L L C" wins over single letters.
    for long, short in sorted(table.items(), key=lambda kv: -len(kv[0])):
        s = re.sub(rf"\b{re.escape(long)}\b", short, s)
    return re.sub(r"\s+", " ", s).strip()


def normalize_name(name: str | None) -> str:
    s = _replace_tokens(_clean(name), _NAME_TOKENS)
    s = s.replace("&", " & ")
    s = re.sub(r"\s+", " ", s).strip()
    # "WHITESTONE GROUP INC THE" and "THE WHITESTONE GROUP INC" are the same name
    return re.sub(r"^THE |(?<= )THE$", "", s).strip()


def normalize_address(addr: str | None) -> str:
    s = _clean(addr).replace("&", " AND ")
    return _replace_tokens(s, _ADDR_TOKENS)


def zip5(z: str | None) -> str:
    digits = re.sub(r"\D", "", "" if z is None else str(z))
    return digits[:5] if len(digits) >= 5 else ""
