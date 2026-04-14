"""Tests for NAS/XML transcript source helpers."""

from types import SimpleNamespace

from aegis.etls.call_summary_editor.nas_source import (
    extract_raw_blocks,
    find_transcript_xml,
    parse_transcript_xml,
)


class DummyConnection:
    def __init__(self, files):
        self._files = files

    def listPath(self, share_name, path):
        return self._files


def test_find_transcript_xml_prefers_earnings_and_highest_version(monkeypatch):
    files = [
        SimpleNamespace(filename="RY-CA_Q3_2024_P_123_1.xml", isDirectory=False),
        SimpleNamespace(filename="RY-CA_Q3_2024_E1_123_2.xml", isDirectory=False),
        SimpleNamespace(filename="RY-CA_Q3_2024_E1_123_5.xml", isDirectory=False),
    ]
    conn = DummyConnection(files)
    institution = {
        "bank_type": "Canadian_Banks",
        "path_safe_name": "RY-CA_Royal_Bank_of_Canada",
    }

    monkeypatch.setenv("NAS_SHARE_NAME", "share")
    monkeypatch.setenv("NAS_BASE_PATH", "")
    monkeypatch.setattr(
        "aegis.etls.call_summary_editor.nas_source.nas_download_file",
        lambda _conn, path: path.encode("utf-8"),
    )

    result = find_transcript_xml(conn, institution, 2024, "Q3", data_path="Data")

    assert result is not None
    assert result.file_path.endswith("RY-CA_Q3_2024_E1_123_5.xml")
    assert result.xml_bytes.endswith(b"RY-CA_Q3_2024_E1_123_5.xml")


def test_parse_transcript_xml_and_extract_raw_blocks():
    xml_bytes = b"""
    <transcript>
      <meta>
        <title>Royal Bank of Canada Q3 2024 Earnings Call</title>
        <participants>
          <participant id=\"p1\" title=\"Chief Executive Officer\" affiliation=\"Royal Bank of Canada\">Dave McKay</participant>
          <participant id=\"p2\" title=\"Analyst\" affiliation=\"Big Bank\">John Doe</participant>
        </participants>
      </meta>
      <body>
        <section name=\"Management Discussion Section\">
          <speaker id=\"p1\" type=\"a\">
            <plist>
              <p>Revenue grew 8% year-over-year.</p>
            </plist>
          </speaker>
        </section>
        <section name=\"Q&amp;A\">
          <speaker id=\"p2\" type=\"q\">
            <plist>
              <p>How should we think about margins?</p>
            </plist>
          </speaker>
          <speaker id=\"p1\" type=\"a\">
            <plist>
              <p>Margins remain resilient.</p>
            </plist>
          </speaker>
        </section>
      </body>
    </transcript>
    """

    parsed = parse_transcript_xml(xml_bytes)
    assert parsed is not None
    assert parsed["title"] == "Royal Bank of Canada Q3 2024 Earnings Call"

    md_blocks, qa_blocks = extract_raw_blocks(parsed, "RY-CA")

    assert len(md_blocks) == 1
    assert md_blocks[0]["speaker"] == "Dave McKay"
    assert md_blocks[0]["speaker_title"] == "Chief Executive Officer"
    assert len(qa_blocks) == 2
    assert qa_blocks[0]["speaker_type_hint"] == "q"
    assert qa_blocks[1]["speaker_affiliation"] == "Royal Bank of Canada"
