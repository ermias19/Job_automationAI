from __future__ import annotations

from pathlib import Path


def local_path_hyperlink(path_value: str, label: str) -> str:
    """Return a spreadsheet hyperlink formula for a local filesystem path."""
    if not path_value:
        return ""

    value = path_value.strip()
    if value.startswith("http://") or value.startswith("https://"):
        # For web links (e.g., Google Drive), plain URL is safest for Google Sheets.
        return value

    path = Path(value).expanduser()
    try:
        file_uri = path.resolve().as_uri()
    except Exception:
        return path_value

    safe_uri = file_uri.replace('"', "%22")
    safe_label = label.replace('"', '""')
    return f'=HYPERLINK("{safe_uri}","{safe_label}")'
