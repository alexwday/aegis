"""NAS/XML transcript source helpers for call_summary_editor.

This module isolates transcript discovery and XML parsing from the downstream
interactive processing pipeline so the source can later be swapped from NAS to
S3 without changing classification or HTML generation.
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from aegis.utils.logging import get_logger

logger = get_logger()

DEFAULT_NAS_DATA_PATH = (
    "Finance Data and Analytics/DSA/Earnings Call Transcripts/Outputs/Data"
)

try:
    from smb.SMBConnection import SMBConnection
except ImportError:  # pragma: no cover - exercised via runtime environment
    SMBConnection = Any  # type: ignore[misc,assignment]


@dataclass(frozen=True)
class TranscriptXmlResult:
    """Resolved transcript XML payload and source metadata."""

    file_path: str
    xml_bytes: bytes
    transcript_title: str = ""


def _require_smb_dependency() -> None:
    if SMBConnection is Any:
        raise RuntimeError(
            "pysmb is required for NAS transcript access. Install the dependency "
            "or switch to a different transcript source implementation."
        )


def _nas_share() -> str:
    return os.getenv("NAS_SHARE_NAME", "")


def _nas_full(relative: str) -> str:
    base = os.getenv("NAS_BASE_PATH", "").rstrip("/")
    return f"{base}/{relative}".lstrip("/")


def get_nas_connection() -> SMBConnection:
    """Create an SMB connection to the NAS using ETL environment variables."""
    _require_smb_dependency()

    required = [
        "NAS_USERNAME",
        "NAS_PASSWORD",
        "NAS_SERVER_IP",
        "NAS_SERVER_NAME",
        "NAS_SHARE_NAME",
    ]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing NAS environment variables: {', '.join(missing)}")

    conn = SMBConnection(
        os.getenv("NAS_USERNAME"),
        os.getenv("NAS_PASSWORD"),
        os.getenv("CLIENT_MACHINE_NAME", "AEGIS"),
        os.getenv("NAS_SERVER_NAME"),
        use_ntlm_v2=True,
        is_direct_tcp=True,
    )
    if not conn.connect(os.getenv("NAS_SERVER_IP"), int(os.getenv("NAS_PORT", "445"))):
        raise ConnectionError("NAS connection failed")
    return conn


def nas_list_files(conn: SMBConnection, path: str) -> List[Any]:
    """List entries in a NAS directory."""
    try:
        return conn.listPath(_nas_share(), _nas_full(path))
    except Exception as exc:
        logger.warning("etl.call_summary_editor.nas_list_failed", path=path, error=str(exc))
        return []


def nas_download_file(conn: SMBConnection, path: str) -> Optional[bytes]:
    """Download a NAS file into memory."""
    import io

    try:
        buf = io.BytesIO()
        conn.retrieveFile(_nas_share(), _nas_full(path), buf)
        return buf.getvalue()
    except Exception as exc:
        logger.warning(
            "etl.call_summary_editor.nas_download_failed",
            path=path,
            error=str(exc),
        )
        return None


def find_transcript_xml(
    conn: SMBConnection,
    institution: Dict[str, Any],
    fiscal_year: int,
    fiscal_quarter: str,
    data_path: Optional[str] = None,
) -> Optional[TranscriptXmlResult]:
    """Locate the preferred transcript XML for one bank/period on NAS."""
    base_data_path = data_path or os.getenv("CALL_SUMMARY_NAS_DATA_PATH") or DEFAULT_NAS_DATA_PATH
    folder = (
        f"{base_data_path}/{fiscal_year}/{fiscal_quarter}/"
        f"{institution['bank_type']}/{institution['path_safe_name']}"
    )

    files = nas_list_files(conn, folder)
    xml_files = [
        file_info
        for file_info in files
        if not getattr(file_info, "isDirectory", False)
        and getattr(file_info, "filename", "").endswith(".xml")
        and not getattr(file_info, "filename", "").startswith(".")
    ]
    if not xml_files:
        return None

    def parse_filename(filename: str) -> Optional[Dict[str, Any]]:
        try:
            parts = filename.removesuffix(".xml").split("_")
            if len(parts) < 6:
                return None
            return {
                "filename": filename,
                "transcript_type": parts[3],
                "version_id": int(parts[5]) if parts[5].isdigit() else 0,
            }
        except Exception:
            return None

    parsed = [candidate for candidate in (parse_filename(f.filename) for f in xml_files) if candidate]
    if not parsed:
        return None

    parsed.sort(
        key=lambda candidate: (
            0 if candidate["transcript_type"].upper() in ("E1", "EARNINGS") else 1,
            -candidate["version_id"],
        )
    )
    selected = parsed[0]
    file_path = f"{folder}/{selected['filename']}"
    xml_bytes = nas_download_file(conn, file_path)
    if not xml_bytes:
        return None

    return TranscriptXmlResult(file_path=file_path, xml_bytes=xml_bytes)


def _clean(text: str) -> str:
    if not text:
        return ""
    return text.strip().replace("\n", " ").replace("\r", " ").replace("\t", " ")


def parse_transcript_xml(xml_bytes: bytes) -> Optional[Dict[str, Any]]:
    """Parse FactSet XML into transcript title, participants, and section blocks."""
    try:
        root = ET.fromstring(xml_bytes)
        namespace = (root.tag.split("}")[0] + "}") if root.tag.startswith("{") else ""

        def find_element(parent: ET.Element, tag: str):
            return parent.find(f"{namespace}{tag}")

        def find_all(parent: ET.Element, tag: str):
            return parent.findall(f"{namespace}{tag}")

        meta = find_element(root, "meta")
        if meta is None:
            return None

        title_element = find_element(meta, "title")
        title = _clean(title_element.text) if title_element is not None and title_element.text else ""

        participants: Dict[str, Dict[str, str]] = {}
        participants_element = find_element(meta, "participants")
        if participants_element is not None:
            for participant in find_all(participants_element, "participant"):
                participant_id = participant.get("id")
                if not participant_id:
                    continue
                participants[participant_id] = {
                    "name": _clean(participant.get("name", "") or participant.text or "Unknown Speaker"),
                    "type": participant.get("type", ""),
                    "title": _clean(participant.get("title", "")),
                    "affiliation": _clean(participant.get("affiliation", "")),
                }

        body = find_element(root, "body")
        if body is None:
            return None

        sections = []
        for section in find_all(body, "section"):
            section_name = section.get("name", "")
            speakers = []
            for speaker in find_all(section, "speaker"):
                plist = find_element(speaker, "plist")
                paragraphs = []
                if plist is not None:
                    for paragraph in find_all(plist, "p"):
                        if paragraph.text:
                            paragraphs.append(_clean(paragraph.text))
                if paragraphs:
                    speakers.append(
                        {
                            "speaker_id": speaker.get("id", ""),
                            "speaker_type": speaker.get("type", ""),
                            "paragraphs": paragraphs,
                        }
                    )
            if speakers:
                sections.append({"name": section_name, "speakers": speakers})

        return {"title": title, "participants": participants, "sections": sections}
    except ET.ParseError as exc:
        logger.warning("etl.call_summary_editor.xml_parse_failed", error=str(exc))
        return None
    except Exception as exc:
        logger.warning("etl.call_summary_editor.xml_parse_error", error=str(exc))
        return None


def extract_raw_blocks(parsed: Dict[str, Any], ticker: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Extract MD and QA speaker blocks from parsed transcript XML."""
    participants = parsed.get("participants", {})
    md_blocks: List[Dict[str, Any]] = []
    qa_blocks: List[Dict[str, Any]] = []
    block_counter = 0

    for section in parsed.get("sections", []):
        section_name = section.get("name", "")
        is_md = "management discussion" in section_name.lower()
        is_qa = "question" in section_name.lower() or "q&a" in section_name.lower()

        for speaker in section.get("speakers", []):
            paragraphs = speaker.get("paragraphs", [])
            if not paragraphs:
                continue

            block_counter += 1
            participant = participants.get(speaker.get("speaker_id", ""), {"name": "Unknown Speaker"})
            record = {
                "id": f"{ticker}_{'MD' if is_md else 'QA'}_{block_counter}",
                "speaker": _clean(participant.get("name", "Unknown Speaker")),
                "speaker_title": _clean(participant.get("title", "")),
                "speaker_affiliation": _clean(participant.get("affiliation", "")),
                "speaker_type_hint": speaker.get("speaker_type", ""),
                "paragraphs": paragraphs,
            }
            if is_md:
                md_blocks.append(record)
            elif is_qa:
                qa_blocks.append(record)

    return md_blocks, qa_blocks
