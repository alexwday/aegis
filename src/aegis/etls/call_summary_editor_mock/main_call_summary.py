"""
Call Summary Interactive HTML Report Generator — v2

Changes from v1:
  - LLM-based Q/A boundary detection (ID-based, no content rewriting)
  - spaCy sentence splitting (en_core_web_sm) — handles financial abbreviations
  - Per-paragraph LLM calls for MD (full speaker-block context carried forward)
  - Per-exchange LLM call for QA (full conversation as context)
  - Sentence-level category scores stored on every sentence
  - HTML: sentence spans + score popover; MD reassignment triggers immediate re-render

Usage:
    python -m aegis.etls.call_summary_editor_mock --year 2025 --quarter Q1 --banks RY-CA,TD-CA,BMO-CA
    python -m aegis.etls.call_summary_editor_mock --year 2025 --quarter Q1 --dev
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import logging
import os
import sys
import time
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field
from smb.SMBConnection import SMBConnection

# ── spaCy (loaded once; only senter needed for sentence splitting) ──────────
try:
    import spacy
    _NLP = spacy.load("en_core_web_sm", exclude=["ner", "attribute_ruler", "lemmatizer"])
except OSError:
    raise RuntimeError(
        "spaCy model not found.\n"
        "Run:  pip install spacy && python -m spacy download en_core_web_sm"
    )

# ============================================================
# CONSTANTS
# ============================================================

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"
DEFAULT_CATEGORIES_FILE = SCRIPT_DIR / "call_summary_categories.xlsx"
DEFAULT_BANNER_PATH = SCRIPT_DIR / "banner.svg" if (SCRIPT_DIR / "banner.svg").exists() else SCRIPT_DIR / "banner.png"
DEFAULT_CONFIG_PATH = SCRIPT_DIR / "config.yaml"
MONITORED_INSTITUTIONS_PATH = SCRIPT_DIR / "monitored_institutions.yaml"

SECTION_MD = "MANAGEMENT DISCUSSION SECTION"
SECTION_QA = "Q&A"
MIN_IMPORTANCE_SCORE = 4.0

# 15 colour pairs (bg_light, accent)
BUCKET_COLORS: List[Tuple[str, str]] = [
    ("#E3F2FD", "#1565C0"), ("#F3E5F5", "#6A1B9A"), ("#E8F5E9", "#2E7D32"),
    ("#FFF3E0", "#E65100"), ("#FCE4EC", "#880E4F"), ("#E0F7FA", "#00695C"),
    ("#FFF8E1", "#F57F17"), ("#E8EAF6", "#283593"), ("#F1F8E9", "#558B2F"),
    ("#FBE9E7", "#BF360C"), ("#E0F2F1", "#004D40"), ("#EDE7F6", "#4527A0"),
    ("#F9FBE7", "#827717"), ("#FCE4EC", "#AD1457"), ("#E8EAF6", "#1A237E"),
]
OTHER_COLOR: Tuple[str, str] = ("#FAFAFA", "#9E9E9E")

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)
execution_log: List[Dict] = []
error_log: List[Dict] = []


def log_info(msg: str, **kw) -> None:
    logger.info(msg)
    execution_log.append({"level": "info", "msg": msg, "ts": datetime.utcnow().isoformat(), **kw})


def log_warn(msg: str, **kw) -> None:
    logger.warning(msg)
    execution_log.append({"level": "warn", "msg": msg, "ts": datetime.utcnow().isoformat(), **kw})


def log_error(msg: str, **kw) -> None:
    logger.error(msg)
    error_log.append({"msg": msg, "ts": datetime.utcnow().isoformat(), **kw})


# ============================================================
# PYDANTIC MODELS
# ============================================================

class SentenceResult(BaseModel):
    index: int = Field(description="1-based sentence index matching S1, S2, ... in the prompt")
    scores: List[float] = Field(
        description="Relevance score 0-10 for each bucket in order, one per bucket starting at index 0"
    )
    importance_score: float = Field(description="IR quotability 0-10")
    condensed: str = Field(description="~70% length, filler removed, all facts kept")
    summary: str = Field(description="1-2 sentences capturing the essential point")
    paraphrase: str = Field(description="Third-person restatement beginning 'Management noted...' or similar")


class ParagraphClassification(BaseModel):
    sentences: List[SentenceResult]


class QAConversationGroup(BaseModel):
    conversation_id: str
    block_ids: List[str]


class QABoundaryResult(BaseModel):
    conversations: List[QAConversationGroup]


class QAExchangeClassification(BaseModel):
    primary_bucket_index: int = Field(
        description="0-based index of the best bucket for the whole exchange. -1 for Other."
    )
    question_scores: List[float] = Field(
        description="Relevance scores for the question, one per bucket in order starting at index 0"
    )
    question_importance: float
    answer_sentences: List[SentenceResult]


# ============================================================
# LLM TOOL DEFINITIONS
# ============================================================

_SENTENCE_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "index": {
            "type": "integer",
            "description": "1-based sentence index matching S1, S2, etc. in the prompt",
        },
        "scores": {
            "type": "array",
            "description": (
                "REQUIRED. Relevance score 0–10 for every bucket in order, one value per bucket "
                "starting from bucket 0. Must contain exactly one score per bucket. "
                "Example with 3 buckets: [8.5, 2.0, 0.0]"
            ),
            "items": {"type": "number"},
        },
        "importance_score": {
            "type": "number",
            "description": "IR quotability score 0–10",
        },
        "condensed": {"type": "string"},
        "summary": {"type": "string"},
        "paraphrase": {"type": "string"},
    },
    "required": ["index", "scores", "importance_score", "condensed", "summary", "paraphrase"],
}

TOOL_QA_BOUNDARY = {
    "type": "function",
    "function": {
        "name": "group_qa_conversations",
        "description": (
            "Group speaker block IDs into complete Q&A conversation exchanges. "
            "Return ONLY block IDs — do not rewrite any content."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "conversations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "conversation_id": {"type": "string"},
                            "block_ids": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["conversation_id", "block_ids"],
                    },
                }
            },
            "required": ["conversations"],
        },
    },
}

TOOL_MD_PARAGRAPH = {
    "type": "function",
    "function": {
        "name": "classify_paragraph_sentences",
        "description": "Classify each sentence in the current paragraph.",
        "parameters": {
            "type": "object",
            "properties": {
                "sentences": {"type": "array", "items": _SENTENCE_RESULT_SCHEMA}
            },
            "required": ["sentences"],
        },
    },
}

TOOL_QA_EXCHANGE = {
    "type": "function",
    "function": {
        "name": "classify_qa_exchange",
        "description": "Classify the Q&A exchange: whole-question scores and per-sentence answer classification.",
        "parameters": {
            "type": "object",
            "properties": {
                "primary_bucket_index": {"type": "integer"},
                "question_scores": {
                    "type": "array",
                    "description": (
                        "REQUIRED. Relevance score 0–10 for every bucket in order, one value "
                        "per bucket starting from bucket 0. Example: [7.0, 0.5, 0.0]"
                    ),
                    "items": {"type": "number"},
                },
                "question_importance": {"type": "number"},
                "answer_sentences": {"type": "array", "items": _SENTENCE_RESULT_SCHEMA},
            },
            "required": [
                "primary_bucket_index", "question_scores",
                "question_importance", "answer_sentences"
            ],
        },
    },
}

TOOL_HEADLINE = {
    "type": "function",
    "function": {
        "name": "set_headline",
        "description": "Set the headline for a report section.",
        "parameters": {
            "type": "object",
            "properties": {
                "headline": {
                    "type": "string",
                    "description": "A specific, factual 5-10 word headline capturing what management said.",
                },
            },
            "required": ["headline"],
        },
    },
}

# ============================================================
# GLOBALS
# ============================================================

config: Dict[str, Any] = {}
ssl_cert_path: Optional[str] = None
oauth_token: Optional[str] = None
llm_client: Optional[OpenAI] = None
total_llm_cost: float = 0.0

load_dotenv()

# ============================================================
# ENVIRONMENT & CONFIG
# ============================================================

def validate_environment() -> None:
    required = [
        "NAS_USERNAME", "NAS_PASSWORD", "NAS_SERVER_IP", "NAS_SERVER_NAME",
        "NAS_SHARE_NAME", "NAS_BASE_PATH", "NAS_PORT",
        "LLM_CLIENT_ID", "LLM_CLIENT_SECRET", "CLIENT_MACHINE_NAME",
    ]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Missing env vars: {', '.join(missing)}")


def load_local_config(path: str = str(DEFAULT_CONFIG_PATH)) -> Dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)

# ============================================================
# NAS UTILITIES
# ============================================================

def get_nas_connection() -> SMBConnection:
    conn = SMBConnection(
        os.getenv("NAS_USERNAME"), os.getenv("NAS_PASSWORD"),
        os.getenv("CLIENT_MACHINE_NAME", "CLIENT"), os.getenv("NAS_SERVER_NAME"),
        use_ntlm_v2=True,
    )
    if not conn.connect(os.getenv("NAS_SERVER_IP"), int(os.getenv("NAS_PORT", "445"))):
        raise ConnectionError("NAS connection failed")
    return conn


def _nas_share() -> str:
    return os.getenv("NAS_SHARE_NAME", "")


def _nas_full(relative: str) -> str:
    base = os.getenv("NAS_BASE_PATH", "").rstrip("/")
    return f"{base}/{relative}".lstrip("/")


def nas_list_files(conn: SMBConnection, path: str) -> List[Any]:
    try:
        return conn.listPath(_nas_share(), _nas_full(path))
    except Exception as e:
        log_warn(f"NAS listPath failed for {path}: {e}")
        return []


def nas_download_file(conn: SMBConnection, path: str) -> Optional[bytes]:
    buf = io.BytesIO()
    try:
        conn.retrieveFile(_nas_share(), _nas_full(path), buf)
        return buf.getvalue()
    except Exception as e:
        log_error(f"NAS download failed for {path}: {e}")
        return None

# ============================================================
# SSL / PROXY
# ============================================================

def setup_ssl_certificate(conn: SMBConnection) -> None:
    global ssl_cert_path
    cert_path = config.get("nas", {}).get("ssl_cert_path")
    if not cert_path:
        return
    data = nas_download_file(conn, cert_path)
    if not data:
        log_warn("SSL cert not found on NAS; proceeding without custom cert")
        return
    import tempfile
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".cer")
    tmp.write(data); tmp.close()
    ssl_cert_path = tmp.name
    log_info(f"SSL cert loaded: {ssl_cert_path}")


def setup_proxy() -> None:
    pu, pp, purl, pd_ = (
        os.getenv("PROXY_USER"), os.getenv("PROXY_PASSWORD"),
        os.getenv("PROXY_URL"), os.getenv("PROXY_DOMAIN", ""),
    )
    if pu and pp and purl:
        from urllib.parse import quote as uq
        enc = uq(pp, safe="")
        pfx = f"{pd_}\\{pu}" if pd_ else pu
        proxy = f"http://{pfx}:{enc}@{purl}"
        os.environ["HTTP_PROXY"] = os.environ["HTTPS_PROXY"] = proxy

# ============================================================
# LLM / OAUTH
# ============================================================

def get_oauth_token() -> Optional[str]:
    llm_cfg = config.get("llm", {})
    endpoint = llm_cfg.get("token_endpoint")
    if not endpoint:
        raise ValueError("llm.token_endpoint missing from config.yaml")
    verify = ssl_cert_path if ssl_cert_path else True
    max_r = llm_cfg.get("max_retries", 3)
    delay = llm_cfg.get("retry_delay", 1.0)
    for attempt in range(1, max_r + 1):
        try:
            r = requests.post(
                endpoint,
                data={
                    "grant_type": "client_credentials",
                    "client_id": os.getenv("LLM_CLIENT_ID"),
                    "client_secret": os.getenv("LLM_CLIENT_SECRET"),
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                verify=verify, timeout=30,
            )
            if r.status_code == 200:
                log_info("OAuth token acquired")
                return r.json().get("access_token")
            log_warn(f"OAuth attempt {attempt}: HTTP {r.status_code}")
        except Exception as e:
            log_warn(f"OAuth attempt {attempt} error: {e}")
        if attempt < max_r:
            time.sleep(delay * attempt)
    return None


def setup_llm_client(token: str) -> OpenAI:
    llm_cfg = config.get("llm", {})
    return OpenAI(
        api_key=token,
        base_url=llm_cfg.get("base_url", "https://api.openai.com/v1"),
        timeout=llm_cfg.get("timeout", 120),
    )


def refresh_llm_auth() -> None:
    global oauth_token, llm_client
    token = get_oauth_token()
    if not token:
        raise RuntimeError("OAuth refresh failed")
    oauth_token = token
    llm_client = setup_llm_client(token)


def accumulate_cost(usage) -> None:
    global total_llm_cost
    if not usage:
        return
    llm_cfg = config.get("llm", {})
    pr = llm_cfg.get("cost_per_1k_prompt_tokens", 0.0025)
    cr = llm_cfg.get("cost_per_1k_completion_tokens", 0.01)
    total_llm_cost += (usage.prompt_tokens / 1000 * pr) + (usage.completion_tokens / 1000 * cr)


def llm_call(messages: List[Dict], tool: Dict, label: str) -> Optional[Dict]:
    """Single LLM call with retry. Returns parsed tool-call arguments or None."""
    llm_cfg = config.get("llm", {})
    max_r = llm_cfg.get("max_retries", 3)
    for attempt in range(1, max_r + 1):
        try:
            resp = llm_client.chat.completions.create(
                model=llm_cfg.get("model", "gpt-4o-2024-08-06"),
                messages=messages,
                tools=[tool],
                tool_choice="required",
                max_completion_tokens=llm_cfg.get("max_tokens", 4096),
            )
            accumulate_cost(resp.usage)
            raw = resp.choices[0].message.tool_calls[0].function.arguments
            return json.loads(raw)
        except Exception as e:
            log_warn(f"LLM attempt {attempt}/{max_r} failed [{label}]: {e}")
            if attempt < max_r:
                time.sleep(2 ** attempt)
    log_error(f"All LLM retries failed [{label}]")
    return None

# ============================================================
# CATEGORIES
# ============================================================

def load_categories(xlsx_path: str) -> List[Dict[str, Any]]:
    df = pd.read_excel(xlsx_path, sheet_name=0)
    required = ["transcript_sections", "category_name", "category_description"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"XLSX missing columns: {missing}")
    for opt in ["report_section", "example_1", "example_2", "example_3"]:
        if opt not in df.columns:
            df[opt] = ""
    cats = []
    for idx, row in df.iterrows():
        sec = str(row["transcript_sections"]).strip().upper()
        if sec not in ("MD", "QA", "ALL"):
            raise ValueError(f"Invalid transcript_sections '{sec}' in row {idx+2}")
        cats.append({
            "transcript_sections": sec,
            "report_section": str(row["report_section"]).strip() if pd.notna(row["report_section"]) else "Results Summary",
            "category_name": str(row["category_name"]).strip(),
            "category_description": str(row["category_description"]).strip(),
            "example_1": str(row["example_1"]).strip() if pd.notna(row["example_1"]) else "",
            "example_2": str(row["example_2"]).strip() if pd.notna(row["example_2"]) else "",
            "example_3": str(row["example_3"]).strip() if pd.notna(row["example_3"]) else "",
        })
    log_info(f"Loaded {len(cats)} categories from {xlsx_path}")
    return cats


def format_categories_for_prompt(categories: List[Dict], section_filter: str = "ALL") -> str:
    """Format categories as XML for LLM prompts. section_filter: 'MD', 'QA', or 'ALL'."""
    parts = []
    for i, cat in enumerate(categories):
        sec = cat["transcript_sections"]
        if section_filter != "ALL" and sec not in ("ALL", section_filter):
            continue
        applies = {"MD": "Management Discussion only", "QA": "Q&A only", "ALL": "Both MD and Q&A"}[sec]
        lines = [
            f'<category index="{i}">',
            f'  <name>{cat["category_name"]}</name>',
            f'  <applies_to>{applies}</applies_to>',
            f'  <description>{cat["category_description"]}</description>',
        ]
        exs = [cat.get(f"example_{n}", "") for n in (1, 2, 3) if cat.get(f"example_{n}")]
        if exs:
            lines.append("  <examples>")
            for ex in exs:
                lines.append(f"    <example>{ex}</example>")
            lines.append("  </examples>")
        lines.append("</category>")
        parts.append("\n".join(lines))
    return "\n\n".join(parts)


def applicable_bucket_ids(categories: List[Dict], section: str) -> List[str]:
    """Return list of 'bucket_N' IDs applicable to a given section (MD or QA)."""
    return [
        f"bucket_{i}" for i, cat in enumerate(categories)
        if cat["transcript_sections"] in ("ALL", section)
    ]

# ============================================================
# MONITORED INSTITUTIONS
# ============================================================

def load_monitored_institutions() -> Dict[str, Dict]:
    if not MONITORED_INSTITUTIONS_PATH.exists():
        raise FileNotFoundError(f"Not found: {MONITORED_INSTITUTIONS_PATH}")
    with open(MONITORED_INSTITUTIONS_PATH) as f:
        return yaml.safe_load(f)


def resolve_bank_tickers(ticker_list: List[str], institutions: Dict) -> List[Dict]:
    results = []
    for ticker in ticker_list:
        ticker = ticker.strip().upper()
        if ticker in institutions:
            inst = institutions[ticker]
            results.append({
                "ticker": ticker,
                "id": inst["id"],
                "name": inst["name"],
                "type": inst["type"],
                "path_safe_name": inst["path_safe_name"],
            })
        else:
            log_warn(f"Ticker '{ticker}' not found in monitored_institutions.yaml — skipping")
    return results

# ============================================================
# SENTENCE SPLITTING (spaCy)
# ============================================================

def split_sentences(text: str) -> List[str]:
    """Split text into sentences using spaCy en_core_web_sm."""
    if not text or not text.strip():
        return []
    doc = _NLP(text.strip())
    sents = [s.text.strip() for s in doc.sents if s.text.strip() and len(s.text.strip()) > 3]
    return sents if sents else [text.strip()]

# ============================================================
# TRANSCRIPT DISCOVERY + XML PARSING
# ============================================================

def find_transcript_xml(
    conn: SMBConnection, institution: Dict, fiscal_year: str, fiscal_quarter: str
) -> Optional[Tuple[str, bytes]]:
    data_path = config.get("nas", {}).get(
        "data_path",
        "Finance Data and Analytics/DSA/Earnings Call Transcripts/Outputs/Data",
    )
    folder = (
        f"{data_path}/{fiscal_year}/{fiscal_quarter}"
        f"/{institution['type']}/{institution['path_safe_name']}"
    )
    files = nas_list_files(conn, folder)
    xml_files = [f for f in files if not f.isDirectory and f.filename.endswith(".xml") and not f.filename.startswith(".")]
    if not xml_files:
        log_warn(f"No XML files found for {institution['ticker']} at {folder}")
        return None

    def parse_fname(fname: str) -> Optional[Dict]:
        try:
            parts = fname.replace(".xml", "").split("_")
            if len(parts) < 6:
                return None
            return {
                "filename": fname,
                "transcript_type": parts[3],
                "version_id": int(parts[5]) if parts[5].isdigit() else 0,
            }
        except Exception:
            return None

    parsed = [p for p in (parse_fname(f.filename) for f in xml_files) if p]
    if not parsed:
        return None
    parsed.sort(key=lambda p: (0 if p["transcript_type"].upper() in ("E1", "EARNINGS") else 1, -p["version_id"]))
    best = parsed[0]
    file_path = f"{folder}/{best['filename']}"
    xml_bytes = nas_download_file(conn, file_path)
    if not xml_bytes:
        return None
    log_info(f"Found transcript: {best['filename']}")
    return file_path, xml_bytes


def _clean(text: str) -> str:
    if not text:
        return ""
    return text.strip().replace("\n", " ").replace("\r", " ").replace("\t", " ")


def parse_transcript_xml(xml_bytes: bytes) -> Optional[Dict[str, Any]]:
    """Parse FactSet XML → {title, participants, sections}."""
    try:
        root = ET.fromstring(xml_bytes)
        ns = (root.tag.split("}")[0] + "}") if root.tag.startswith("{") else ""

        def fe(parent, tag): return parent.find(f"{ns}{tag}")
        def fea(parent, tag): return parent.findall(f"{ns}{tag}")

        meta = fe(root, "meta")
        if meta is None:
            log_error("XML meta section missing"); return None
        title_el = fe(meta, "title")
        title = _clean(title_el.text) if title_el is not None and title_el.text else ""

        participants: Dict[str, Dict] = {}
        parts_el = fe(meta, "participants")
        if parts_el is not None:
            for p in fea(parts_el, "participant"):
                pid = p.get("id")
                if not pid: continue
                participants[pid] = {
                    "name": _clean(p.get("name", "") or p.text or "Unknown Speaker"),
                    "type": p.get("type", ""),
                    "title": _clean(p.get("title", "")),
                    "affiliation": _clean(p.get("affiliation", "")),
                }

        body = fe(root, "body")
        if body is None:
            log_error("XML body section missing"); return None

        sections = []
        for sec_el in fea(body, "section"):
            sec_name = sec_el.get("name", "")
            speakers_out = []
            for spk_el in fea(sec_el, "speaker"):
                spk_id = spk_el.get("id", "")
                spk_type = spk_el.get("type", "")
                plist = fe(spk_el, "plist")
                paras = []
                if plist is not None:
                    for p_el in fea(plist, "p"):
                        if p_el.text:
                            paras.append(_clean(p_el.text))
                if paras:
                    speakers_out.append({
                        "speaker_id": spk_id,
                        "speaker_type": spk_type,
                        "paragraphs": paras,
                    })
            if speakers_out:
                sections.append({"name": sec_name, "speakers": speakers_out})

        return {"title": title, "participants": participants, "sections": sections}

    except ET.ParseError as e:
        log_error(f"XML parse error: {e}"); return None
    except Exception as e:
        log_error(f"XML unexpected error: {e}"); return None


def extract_raw_blocks(parsed: Dict, ticker: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Extract raw MD speaker blocks and raw QA speaker blocks (pre-boundary detection).
    Returns (md_raw_blocks, qa_raw_blocks).
    Each block: {id, speaker, speaker_title, speaker_affiliation, speaker_type_hint, paragraphs}
    """
    participants = parsed.get("participants", {})
    md_blocks: List[Dict] = []
    qa_blocks: List[Dict] = []
    block_counter = 0

    for section in parsed.get("sections", []):
        sec_name = section.get("name", "")
        is_md = "management discussion" in sec_name.lower()
        is_qa = "question" in sec_name.lower() or "q&a" in sec_name.lower()

        for spk in section.get("speakers", []):
            paras = spk.get("paragraphs", [])
            if not paras:
                continue
            block_counter += 1
            part = participants.get(spk["speaker_id"], {"name": "Unknown Speaker"})
            record = {
                "id": f"{ticker}_{'MD' if is_md else 'QA'}_{block_counter}",
                "speaker": _clean(part.get("name", "Unknown Speaker")),
                "speaker_title": _clean(part.get("title", "")),
                "speaker_affiliation": _clean(part.get("affiliation", "")),
                "speaker_type_hint": spk.get("speaker_type", ""),  # 'q', 'a', or ''
                "paragraphs": paras,
            }
            if is_md:
                md_blocks.append(record)
            elif is_qa:
                qa_blocks.append(record)

    return md_blocks, qa_blocks

# ============================================================
# Q/A BOUNDARY DETECTION (LLM)
# ============================================================

def detect_qa_boundaries(qa_raw_blocks: List[Dict], categories_text_qa: str) -> List[List[Dict]]:
    """
    LLM groups raw QA speaker blocks into conversation exchanges.
    Returns list of conversations; each conversation is a list of block dicts.
    """
    if not qa_raw_blocks:
        return []

    # Format blocks with IDs and type hints — content not rewritten, just displayed
    block_lines = []
    for blk in qa_raw_blocks:
        hint = blk["speaker_type_hint"].upper() if blk["speaker_type_hint"] else "?"
        speaker_line = blk["speaker"]
        if blk["speaker_title"]:
            speaker_line += f", {blk['speaker_title']}"
        if blk["speaker_affiliation"]:
            speaker_line += f" ({blk['speaker_affiliation']})"
        # Show first 300 chars of first paragraph as preview
        preview = blk["paragraphs"][0][:300] if blk["paragraphs"] else ""
        if blk["paragraphs"] and len(blk["paragraphs"][0]) > 300:
            preview += "..."
        block_lines.append(
            f'[{blk["id"]}] type_hint={hint} | {speaker_line}\n  "{preview}"'
        )

    formatted = "\n\n".join(block_lines)

    system_prompt = (
        "You are grouping speaker blocks from an earnings call Q&A section into complete conversation exchanges.\n"
        "Each conversation begins with an analyst question and includes all executive responses that follow, "
        "until the next analyst question.\n"
        "Return ONLY block IDs grouped into conversations — do not rewrite any content.\n"
        "The type_hint field (q=question, a=answer) is a FactSet tag that may occasionally be inaccurate — "
        "use it as a guide, not ground truth. Use speaker affiliation to identify analysts vs executives."
    )
    user_prompt = (
        f"Group these Q&A speaker blocks into conversation exchanges:\n\n{formatted}\n\n"
        "Return block IDs grouped into conversations using the group_qa_conversations function."
    )

    raw = llm_call(
        messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
        tool=TOOL_QA_BOUNDARY,
        label="qa_boundary",
    )

    if not raw:
        log_warn("QA boundary detection failed; falling back to type-hint grouping")
        return _fallback_qa_grouping(qa_raw_blocks)

    try:
        result = QABoundaryResult.model_validate(raw)
    except Exception as e:
        log_warn(f"QA boundary validation error: {e}; using fallback")
        return _fallback_qa_grouping(qa_raw_blocks)

    # Map block IDs back to block dicts
    block_by_id = {blk["id"]: blk for blk in qa_raw_blocks}
    conversations = []
    for conv in result.conversations:
        blocks = [block_by_id[bid] for bid in conv.block_ids if bid in block_by_id]
        if blocks:
            conversations.append(blocks)

    log_info(f"  QA boundary: {len(qa_raw_blocks)} blocks → {len(conversations)} conversations")
    return conversations


def _fallback_qa_grouping(qa_raw_blocks: List[Dict]) -> List[List[Dict]]:
    """Fallback: group by consecutive q→a blocks using FactSet type hints."""
    conversations = []
    i = 0
    while i < len(qa_raw_blocks):
        blk = qa_raw_blocks[i]
        if blk["speaker_type_hint"] == "q":
            group = [blk]
            j = i + 1
            while j < len(qa_raw_blocks) and qa_raw_blocks[j]["speaker_type_hint"] == "a":
                group.append(qa_raw_blocks[j])
                j += 1
            conversations.append(group)
            i = j
        else:
            i += 1
    return conversations

# ============================================================
# MD SENTENCE CLASSIFICATION (LLM, per-paragraph)
# ============================================================

def _bucket_name(bucket_id: str, categories: List[Dict]) -> str:
    if bucket_id == "other":
        return "Other"
    try:
        idx = int(bucket_id.split("_")[1])
        return categories[idx]["category_name"]
    except Exception:
        return bucket_id


def _primary_from_scores(scores: Dict[str, float], applicable_ids: List[str]) -> str:
    """Pick the highest-scoring applicable bucket. Falls back to 'other'."""
    best_id, best_score = "other", 0.0
    for bid in applicable_ids:
        s = scores.get(bid, 0.0)
        if s > best_score:
            best_score = s
            best_id = bid
    return best_id if best_score >= 1.5 else "other"


def _normalise_scores(raw_scores, categories: List[Dict]) -> Dict[str, float]:
    """Convert LLM scores (list or dict) to 'bucket_0','bucket_1',... dict."""
    out = {f"bucket_{i}": 0.0 for i in range(len(categories))}
    if isinstance(raw_scores, list):
        for i, v in enumerate(raw_scores):
            if i < len(categories):
                out[f"bucket_{i}"] = round(float(v), 2)
    else:
        for k, v in raw_scores.items():
            key = k.replace("bucket_", "")
            if key.isdigit():
                out[f"bucket_{key}"] = round(float(v), 2)
    return out


def _make_sentence_record(
    sid: str,
    text: str,
    llm_result: Optional[SentenceResult],
    categories: List[Dict],
    applicable_ids: List[str],
) -> Dict:
    if llm_result is None:
        return {
            "sid": sid, "text": text,
            "primary": "other",
            "scores": {f"bucket_{i}": 0.0 for i in range(len(categories))},
            "importance_score": 2.0,
            "condensed": text, "summary": text, "paraphrase": text,
        }
    scores = _normalise_scores(llm_result.scores, categories)
    return {
        "sid": sid, "text": text,
        "primary": _primary_from_scores(scores, applicable_ids),
        "scores": scores,
        "importance_score": round(float(llm_result.importance_score), 1),
        "condensed": llm_result.condensed or text,
        "summary": llm_result.summary or text,
        "paraphrase": llm_result.paraphrase or text,
    }


def classify_md_block(
    block_raw: Dict,
    categories: List[Dict],
    categories_text_md: str,
    company_name: str,
    fiscal_year: str,
    fiscal_quarter: str,
) -> Dict:
    """
    Process one MD speaker block: split into sentences, classify paragraph-by-paragraph
    carrying forward context from already-classified paragraphs.
    Returns a block dict ready for the HTML state.
    """
    block_id = block_raw["id"]
    paragraphs = block_raw["paragraphs"]
    app_ids = applicable_bucket_ids(categories, "MD")

    speaker_line = block_raw["speaker"]
    if block_raw["speaker_title"]:
        speaker_line += f", {block_raw['speaker_title']}"
    if block_raw["speaker_affiliation"]:
        speaker_line += f" ({block_raw['speaker_affiliation']})"

    # Split all paragraphs into sentences upfront
    all_para_sentences: List[List[str]] = [split_sentences(p) for p in paragraphs]

    sentence_records: List[Dict] = []
    prior_para_summaries: List[str] = []  # context lines for subsequent LLM calls
    global_sent_idx = 0

    for para_idx, para_sents in enumerate(all_para_sentences):
        if not para_sents:
            continue

        # ── Build prompt ──
        # Context block: full speaker turn with prior paragraphs annotated
        ctx_lines = [f"SPEAKER: {speaker_line}\n"]
        for i, (para_text, para_sents_i) in enumerate(zip(paragraphs, all_para_sentences)):
            if i < para_idx:
                # Already classified — show annotation
                ctx_lines.append(f"[Paragraph {i+1} — previously classified]")
                if i < len(prior_para_summaries):
                    ctx_lines.append(prior_para_summaries[i])
                else:
                    ctx_lines.append(para_text[:200])
            elif i == para_idx:
                ctx_lines.append(f"\n[Paragraph {i+1} — CLASSIFY THESE SENTENCES:]")
                for j, s in enumerate(para_sents):
                    ctx_lines.append(f"  S{j+1}: \"{s}\"")
            else:
                ctx_lines.append(f"[Paragraph {i+1} — not yet processed]")
                ctx_lines.append(para_text[:150] + ("..." if len(para_text) > 150 else ""))
        context_text = "\n".join(ctx_lines)

        n_buckets = len(categories)
        system_prompt = (
            f"You are classifying sentences from an earnings call Management Discussion section "
            f"for {company_name}'s {fiscal_quarter} {fiscal_year} earnings call.\n\n"
            f"Available IR report buckets (MD-applicable):\n{categories_text_md}\n\n"
            f"For each sentence you MUST return ALL of these fields:\n"
            f"- scores: array of {n_buckets} numbers, one per bucket in order [bucket_0_score, bucket_1_score, ...]. "
            f"Example: [8.5, 0.0, 2.0, ...]. This field is REQUIRED.\n"
            f"- importance_score: 0–10 IR quotability. Use 0 for ceremonial/procedural content "
            f"(greetings, thank-yous, speaker introductions, handoffs like 'I'll turn it over to...', "
            f"one-word affirmations like 'Great', 'Okay'). Use 1-3 for transitional or low-value context. "
            f"Reserve 4+ for substantive financial commentary worth quoting in an IR report.\n"
            f"- condensed: ~70% length version\n"
            f"- summary: 1-2 sentence summary\n"
            f"- paraphrase: 3rd person rewrite\n"
            f"- index: must match the S-number shown (1-based)\n"
            f"Do NOT omit any field."
        )
        user_prompt = f"{context_text}\n\nClassify the sentences in Paragraph {para_idx+1} now."

        raw = llm_call(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            tool=TOOL_MD_PARAGRAPH,
            label=f"md_para {block_id} p{para_idx+1}",
        )

        # Parse results — validate per-sentence to salvage partial LLM responses
        llm_results_by_idx: Dict[int, SentenceResult] = {}
        if raw and "sentences" in raw:
            for sr_raw in raw["sentences"]:
                try:
                    sr = SentenceResult.model_validate(sr_raw)
                    llm_results_by_idx[sr.index] = sr
                except Exception:
                    # Salvage scores even if text fields are missing
                    idx = sr_raw.get("index")
                    if idx is not None and "scores" in sr_raw:
                        try:
                            sr = SentenceResult(
                                index=idx,
                                scores=sr_raw["scores"],
                                importance_score=sr_raw.get("importance_score", 3.0),
                                condensed=sr_raw.get("condensed", ""),
                                summary=sr_raw.get("summary", ""),
                                paraphrase=sr_raw.get("paraphrase", ""),
                            )
                            llm_results_by_idx[sr.index] = sr
                            log_warn(f"Salvaged partial sentence S{idx} [{block_id} p{para_idx+1}]")
                        except Exception as e2:
                            log_warn(f"Could not salvage sentence S{idx} [{block_id} p{para_idx+1}]: {e2}")
                    else:
                        log_warn(f"Sentence parse failed [{block_id} p{para_idx+1}]: missing index or scores")

        # Build sentence records
        para_labels = []
        for j, sent_text in enumerate(para_sents):
            sid = f"{block_id}_s{global_sent_idx}"
            llm_r = llm_results_by_idx.get(j + 1)
            rec = _make_sentence_record(sid, sent_text, llm_r, categories, app_ids)
            rec["para_idx"] = para_idx
            sentence_records.append(rec)
            para_labels.append(f"S{j+1}→{_bucket_name(rec['primary'], categories)}")
            global_sent_idx += 1

        prior_para_summaries.append(f"  [{', '.join(para_labels)}] {paragraphs[para_idx][:120]}...")

    return {
        "id": block_id,
        "speaker": block_raw["speaker"],
        "speaker_title": block_raw.get("speaker_title", ""),
        "speaker_affiliation": block_raw.get("speaker_affiliation", ""),
        "sentences": sentence_records,
    }

# ============================================================
# QA SENTENCE CLASSIFICATION (LLM, per conversation)
# ============================================================

def classify_qa_conversation(
    conv_idx: int,
    conv_blocks: List[Dict],
    ticker: str,
    categories: List[Dict],
    categories_text_qa: str,
    company_name: str,
    fiscal_year: str,
    fiscal_quarter: str,
) -> Dict:
    """
    Classify one Q/A conversation exchange at sentence level.
    Returns a conversation dict ready for the HTML state.
    """
    conv_id = f"{ticker}_QA_{conv_idx}"
    app_ids = applicable_bucket_ids(categories, "QA")

    # Separate question blocks from answer blocks
    q_blocks = [b for b in conv_blocks if b["speaker_type_hint"] == "q"]
    a_blocks = [b for b in conv_blocks if b["speaker_type_hint"] != "q"]

    # If no explicit split, treat first block as question
    if not q_blocks:
        q_blocks, a_blocks = conv_blocks[:1], conv_blocks[1:]

    # Analyst / executive info from first blocks
    analyst_name = q_blocks[0]["speaker"] if q_blocks else "Analyst"
    analyst_affiliation = q_blocks[0]["speaker_affiliation"] if q_blocks else ""
    exec_name = a_blocks[0]["speaker"] if a_blocks else "Executive"
    exec_title = a_blocks[0]["speaker_title"] if a_blocks else ""

    # Assemble full question text and split into sentences
    q_text = " ".join(
        para for blk in q_blocks for para in blk["paragraphs"]
    )
    q_sentences = split_sentences(q_text)

    # Assemble full answer text and split into sentences, tracking paragraph index
    a_sentences_raw: List[str] = []
    a_para_indices: List[int] = []
    _a_pidx = 0
    for blk in a_blocks:
        for para in blk["paragraphs"]:
            for s in split_sentences(para):
                a_sentences_raw.append(s)
                a_para_indices.append(_a_pidx)
            _a_pidx += 1

    # ── Build prompt ──
    q_formatted = "\n".join(f"QS{i+1}: \"{s}\"" for i, s in enumerate(q_sentences))
    a_formatted = "\n".join(f"AS{i+1}: \"{s}\"" for i, s in enumerate(a_sentences_raw))

    n_buckets = len(categories)
    system_prompt = (
        f"You are classifying a Q&A exchange from {company_name}'s {fiscal_quarter} {fiscal_year} earnings call.\n\n"
        f"Available IR report buckets (Q&A-applicable):\n{categories_text_qa}\n\n"
        f"Task:\n"
        f"1. primary_bucket_index: which single bucket best describes this whole exchange (-1 for Other)\n"
        f"2. question_scores: array of {n_buckets} numbers [bucket_0_score, ..., bucket_{n_buckets-1}_score]. REQUIRED.\n"
        f"3. question_importance: 0–10 IR quotability of the question. "
        f"Use 0 for ceremonial content (greetings, thanks). Reserve 4+ for substantive questions.\n"
        f"4. answer_sentences: for each AS-numbered answer sentence, ALL fields are REQUIRED:\n"
        f"   - index: must match the AS-number (1-based)\n"
        f"   - scores: array of {n_buckets} numbers, one per bucket in order. REQUIRED.\n"
        f"   - importance_score: 0–10 IR quotability. Use 0 for ceremonial/procedural content "
        f"(greetings, thank-yous, speaker introductions, handoffs, one-word affirmations). "
        f"Use 1-3 for transitional or low-value context. Reserve 4+ for substantive commentary.\n"
        f"   - condensed, summary, paraphrase: text fields\n"
        f"Do NOT omit any field."
    )
    user_prompt = (
        f"ANALYST ({analyst_name}"
        f"{', ' + analyst_affiliation if analyst_affiliation else ''}):\n{q_formatted}\n\n"
        f"EXECUTIVE ({exec_name}"
        f"{', ' + exec_title if exec_title else ''}):\n{a_formatted}\n\n"
        f"Classify this exchange using the classify_qa_exchange function."
    )

    raw = llm_call(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        tool=TOOL_QA_EXCHANGE,
        label=f"qa_conv {conv_id}",
    )

    # ── Parse results ──
    primary_bucket = "other"
    q_sent_records: List[Dict] = []
    a_sent_records: List[Dict] = []

    if raw:
        try:
            result = QAExchangeClassification.model_validate(raw)

            # Primary bucket for whole conversation
            idx = result.primary_bucket_index
            if 0 <= idx < len(categories):
                primary_bucket = f"bucket_{idx}"

            # Question sentences (treated as a unit — share question_scores)
            q_scores = _normalise_scores(result.question_scores, categories)
            q_primary = _primary_from_scores(q_scores, app_ids)
            for i, q_sent in enumerate(q_sentences):
                q_sent_records.append({
                    "sid": f"{conv_id}_qs{i}",
                    "text": q_sent,
                    "primary": q_primary,
                    "scores": q_scores,
                    "importance_score": round(float(result.question_importance), 1),
                    "condensed": q_sent,
                    "summary": q_sent,
                    "paraphrase": q_sent,
                })

            # Answer sentences (per-sentence from LLM)
            a_by_idx = {sr.index: sr for sr in result.answer_sentences}
            for i, a_sent in enumerate(a_sentences_raw):
                llm_r = a_by_idx.get(i + 1)
                sid = f"{conv_id}_as{i}"
                rec = _make_sentence_record(sid, a_sent, llm_r, categories, app_ids)
                rec["para_idx"] = a_para_indices[i] if i < len(a_para_indices) else 0
                a_sent_records.append(rec)

        except Exception as e:
            log_warn(f"QA classification parse error [{conv_id}]: {e}")

    # Fallback: if parse failed, build plain records
    if not q_sent_records:
        for i, s in enumerate(q_sentences):
            q_sent_records.append({
                "sid": f"{conv_id}_qs{i}", "text": s,
                "primary": "other",
                "scores": {f"bucket_{j}": 0.0 for j in range(len(categories))},
                "importance_score": 3.0,
                "condensed": s, "summary": s, "paraphrase": s,
            })
    if not a_sent_records:
        for i, s in enumerate(a_sentences_raw):
            a_sent_records.append({
                "sid": f"{conv_id}_as{i}", "text": s,
                "primary": "other",
                "scores": {f"bucket_{j}": 0.0 for j in range(len(categories))},
                "importance_score": 3.0,
                "condensed": s, "summary": s, "paraphrase": s,
                "para_idx": a_para_indices[i] if i < len(a_para_indices) else 0,
            })

    return {
        "id": conv_id,
        "primary_bucket": primary_bucket,
        "analyst_name": analyst_name,
        "analyst_affiliation": analyst_affiliation,
        "executive_name": exec_name,
        "executive_title": exec_title,
        "question_sentences": q_sent_records,
        "answer_sentences": a_sent_records,
    }

# ============================================================
# BANK PROCESSING
# ============================================================

def process_bank(
    conn: SMBConnection,
    institution: Dict,
    fiscal_year: str,
    fiscal_quarter: str,
    categories: List[Dict],
    dev_max_blocks: Optional[int] = None,
) -> Optional[Dict]:
    ticker = institution["ticker"]
    company_name = institution["name"]
    log_info(f"Processing {ticker} — {company_name}")

    refresh_llm_auth()

    result = find_transcript_xml(conn, institution, fiscal_year, fiscal_quarter)
    if result is None:
        log_warn(f"No transcript found for {ticker}")
        return None
    _, xml_bytes = result

    parsed = parse_transcript_xml(xml_bytes)
    if parsed is None:
        log_error(f"XML parse failed for {ticker}")
        return None

    md_raw_blocks, qa_raw_blocks = extract_raw_blocks(parsed, ticker)
    log_info(f"  {ticker}: {len(md_raw_blocks)} MD blocks, {len(qa_raw_blocks)} raw QA blocks")

    categories_text_md = format_categories_for_prompt(categories, "MD")
    categories_text_qa = format_categories_for_prompt(categories, "QA")

    # ── MD classification ──
    if dev_max_blocks:
        md_raw_blocks = md_raw_blocks[:dev_max_blocks]

    processed_md: List[Dict] = []
    for i, blk in enumerate(md_raw_blocks):
        log_info(f"  MD block {i+1}/{len(md_raw_blocks)}: {blk['id']}")
        processed_md.append(classify_md_block(
            blk, categories, categories_text_md,
            company_name, fiscal_year, fiscal_quarter,
        ))

    # ── QA boundary detection ──
    if dev_max_blocks:
        qa_raw_blocks = qa_raw_blocks[:dev_max_blocks * 3]  # approx ratio

    qa_conversations_raw = detect_qa_boundaries(qa_raw_blocks, categories_text_qa)

    if dev_max_blocks:
        qa_conversations_raw = qa_conversations_raw[:dev_max_blocks]

    # ── QA classification ──
    processed_qa: List[Dict] = []
    for i, conv_blocks in enumerate(qa_conversations_raw):
        log_info(f"  QA conv {i+1}/{len(qa_conversations_raw)}")
        processed_qa.append(classify_qa_conversation(
            i + 1, conv_blocks, ticker, categories,
            categories_text_qa, company_name, fiscal_year, fiscal_quarter,
        ))

    return {
        "ticker": ticker,
        "company_name": company_name,
        "transcript_title": parsed.get("title", f"{fiscal_quarter} {fiscal_year} Earnings Call"),
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "md_blocks": processed_md,
        "qa_conversations": processed_qa,
    }

# ============================================================
# HEADLINE GENERATION
# ============================================================

def generate_bucket_headline(bucket_name: str, samples: List[str]) -> str:
    if not samples:
        return ""
    sample_text = "\n\n---\n\n".join(samples[:8])
    system_prompt = (
        f"Generate a SPECIFIC, FACTUAL 5-10 word headline for the '{bucket_name}' "
        f"section of an IR earnings summary. Capture what management actually said.\n"
        f"Good: 'Banks Signal NII Headwinds as Rate Cuts Materialize'\n"
        f"Bad: 'Net Interest Income Discussion'\n"
        f"Return the headline using the set_headline function, no trailing punctuation."
    )
    raw = llm_call(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Sample content:\n\n{sample_text}"},
        ],
        tool=TOOL_HEADLINE,
        label=f"headline_{bucket_name}",
    )
    if raw and raw.get("headline"):
        return raw["headline"].strip().strip('"\'')
    log_warn(f"Headline generation returned no result for '{bucket_name}'")
    return ""

# ============================================================
# STATE BUILDING
# ============================================================

def build_report_state(
    banks_data: Dict[str, Dict],
    categories: List[Dict],
    fiscal_year: str,
    fiscal_quarter: str,
    min_importance: float,
) -> Dict:
    # Build bucket definitions
    buckets = []
    for i, cat in enumerate(categories):
        bg, accent = BUCKET_COLORS[i % len(BUCKET_COLORS)]
        buckets.append({
            "id": f"bucket_{i}",
            "name": cat["category_name"],
            "report_section": cat.get("report_section", "Results Summary"),
            "transcript_sections": cat["transcript_sections"],
            "description": cat["category_description"],
            "color_bg": bg,
            "color_accent": accent,
            "generated_headline": "",
        })
    buckets.append({
        "id": "other", "name": "Other",
        "report_section": "Other", "transcript_sections": "ALL",
        "description": "Quotes not strongly matching any defined bucket.",
        "color_bg": OTHER_COLOR[0], "color_accent": OTHER_COLOR[1],
        "generated_headline": "",
    })

    # Collect headline samples per bucket
    headline_samples: Dict[str, List[str]] = defaultdict(list)
    for bank_data in banks_data.values():
        for blk in bank_data["md_blocks"]:
            for sent in blk["sentences"]:
                if sent["importance_score"] >= min_importance and sent["primary"] != "other":
                    if len(headline_samples[sent["primary"]]) < 8:
                        headline_samples[sent["primary"]].append(sent["summary"])
        for conv in bank_data["qa_conversations"]:
            for sent in conv["answer_sentences"]:
                if sent["importance_score"] >= min_importance and sent["primary"] != "other":
                    pid = conv.get("primary_bucket", sent["primary"])
                    if len(headline_samples[pid]) < 8:
                        headline_samples[pid].append(sent["summary"])

    # Generate headlines
    log_info("Generating bucket headlines...")
    for bucket in buckets:
        if bucket["id"] == "other":
            continue
        samples = headline_samples.get(bucket["id"], [])
        if samples:
            hl = generate_bucket_headline(bucket["name"], samples)
            bucket["generated_headline"] = hl
            log_info(f"  '{bucket['name']}': {hl}")

    # Initial bank states — all empty; JS derives groupings from sentence data
    bank_states = {
        ticker: {
            "sentence_user_primary": {},
            "excluded_sentences": [],
            "subquote_bucket_overrides": {},
            "bucket_subquote_order": {},
            "subquote_formats": {},
        }
        for ticker in banks_data
    }

    return {
        "meta": {
            "generated_at": datetime.utcnow().isoformat(),
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
            "min_importance": min_importance,
            "version": "2.0",
        },
        "buckets": buckets,
        "banks": banks_data,
        "bank_states": bank_states,
        "current_bank": next(iter(banks_data)) if banks_data else None,
        "banner_visible": True,
        "banner_src": None,
        "bucket_user_titles": {},
    }

# ============================================================
# BANNER
# ============================================================

def load_banner_b64(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    with open(path, "rb") as f:
        data = base64.b64encode(f.read()).decode()
    ext = path.suffix.lower().lstrip(".")
    mime = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
            "svg": "image/svg+xml"}.get(ext, "image/png")
    return f"data:{mime};base64,{data}"

# ============================================================
# HTML TEMPLATE
# ============================================================

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Call Summary — __PERIOD__</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#F0F2F5;color:#1C1E21;font-size:14px;line-height:1.5;height:100vh;overflow:hidden}
#app{display:flex;flex-direction:column;height:100vh;overflow:hidden}

/* App banner */
#app-banner{flex-shrink:0;background:#1A2332;color:#fff;font-size:14px;font-weight:700;letter-spacing:.03em;padding:10px 20px;text-transform:uppercase}

/* PDF banner — only shown in print/PDF */
#banner-wrap{display:none}
#banner-wrap img{max-height:56px;max-width:400px;object-fit:contain}

/* Tab bar */
#tab-bar{flex-shrink:0;background:#fff;border-bottom:2px solid #E4E6EA;display:flex;align-items:stretch;padding:0 16px;gap:2px;overflow-x:auto;scrollbar-width:none}
#tab-bar::-webkit-scrollbar{display:none}
.tab-btn{padding:10px 18px;font-size:13px;font-weight:500;border:none;background:none;cursor:pointer;color:#65676B;border-bottom:3px solid transparent;white-space:nowrap}
.tab-btn.active{color:#1A2332;border-bottom-color:#1A2332;font-weight:600}
.tab-btn:hover:not(.active){color:#1A2332}

/* Content area */
#content-area{flex:1;display:flex;overflow:hidden;position:relative}

/* Transcript wrapper: rail + panel side by side */
#transcript-wrap{display:flex;flex-shrink:0;overflow:hidden;transition:width .2s ease}
#transcript-wrap.tp-collapsed{width:32px!important}

/* Transcript panel — flex row: content + rail */
#transcript-panel{flex:1;display:flex;flex-direction:row;background:#fff;overflow:hidden;min-width:0}

/* Transcript content column */
#tp-content{flex:1;display:flex;flex-direction:column;overflow:hidden;min-width:0;transition:opacity .15s,flex .2s}
.tp-collapsed #tp-content{opacity:0;pointer-events:none;overflow:hidden;flex:0}

/* Vertical rail — always visible, on the inside (right edge of transcript) */
#tp-rail{width:32px;flex-shrink:0;background:#2C3E50;cursor:pointer;display:flex;align-items:center;justify-content:center;user-select:none;z-index:2}
#tp-rail:hover{background:#34495E}
#tp-rail-label{writing-mode:vertical-lr;font-size:10px;font-weight:600;color:rgba(255,255,255,.8);letter-spacing:.06em;text-transform:uppercase;white-space:nowrap}

/* Resize handle — hidden when collapsed */
#tp-resize{width:5px;flex-shrink:0;cursor:col-resize;background:#E4E6EA;transition:background .15s}
#tp-resize:hover,#tp-resize.active{background:#0A66C2}
#tp-resize.tp-hidden{display:none}

/* View mode toggle */
#tp-view-bar{padding:5px 10px;background:#F8F9FB;border-bottom:1px solid #E4E6EA;flex-shrink:0;display:flex;align-items:center;justify-content:center;gap:4px}
.tp-view-label{font-size:9px;font-weight:600;color:#8A8D91;text-transform:uppercase;letter-spacing:.03em;margin-right:2px;white-space:nowrap}
.tp-view-btn{font-size:9px;padding:3px 8px;border:1px solid transparent;border-radius:3px;background:none;cursor:pointer;color:#65676B;font-weight:600;text-transform:uppercase;letter-spacing:.03em;white-space:nowrap}
.tp-view-btn.active{background:#1A2332;color:#fff;border-color:#1A2332}
.tp-view-btn:hover:not(.active){background:#E4E6EA}
.tp-view-help{width:18px;height:18px;border-radius:50%;border:1px solid #CDD0D5;background:none;cursor:pointer;font-size:10px;font-weight:700;color:#8A8D91;display:flex;align-items:center;justify-content:center;margin-left:4px;flex-shrink:0}
.tp-view-help:hover{background:#E4E6EA;color:#1A2332}

/* View help tooltip */
#tp-help-tip{display:none;position:absolute;z-index:100;background:#fff;border:1px solid #E4E6EA;border-radius:6px;box-shadow:0 4px 16px rgba(0,0,0,.12);padding:12px 14px;width:280px;font-size:11px;line-height:1.5;color:#333}
#tp-help-tip.visible{display:block}
#tp-help-tip h4{font-size:10px;font-weight:700;text-transform:uppercase;color:#1A2332;margin:0 0 6px;letter-spacing:.04em}
#tp-help-tip ul{margin:0;padding:0 0 0 14px}
#tp-help-tip li{margin-bottom:6px}
#tp-help-tip li:last-child{margin-bottom:0}
#tp-help-tip strong{color:#1A2332}

#transcript-body{flex:1;overflow-y:auto;padding:4px 0}

/* Transcript section header */
.t-section-header{padding:8px 12px;background:linear-gradient(135deg,#1A2332 0%,#2C3E50 100%);font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#fff;border-top:none;border-bottom:2px solid #E4E6EA;margin-bottom:4px}

/* MD speaker block — condensed */
.t-block{margin:4px 6px;padding:6px 8px;border-left:3px solid #D0D4DA;border-radius:0 4px 4px 0;background:#FAFBFC}
.t-block-header{display:flex;flex-wrap:wrap;align-items:center;gap:4px;margin-bottom:4px}
.t-speaker-name{font-weight:600;font-size:10px;color:#1A2332}
.t-speaker-title{font-size:9px;color:#65676B}
.t-block-text{font-size:10.5px;line-height:1.45;color:#444}
.t-para-break{display:block;height:6px}

/* Classification sub-group within a block */
.t-class-group{margin:1px 0;padding:3px 5px;border-left:2px solid transparent;border-radius:0 3px 3px 0}
.t-class-label{font-size:8px;font-weight:700;text-transform:uppercase;letter-spacing:.04em;color:#888;margin-bottom:1px}

/* Category view bucket header */
.t-cat-header{padding:5px 10px;background:#F0F2F5;font-size:9px;font-weight:700;color:#555;border-bottom:1px solid #E4E6EA;display:flex;align-items:center;gap:6px;position:sticky;top:0;z-index:2;border-left:3px solid #CDD0D5}
.t-cat-count{font-size:8px;color:#8A8D91;background:#fff;padding:1px 5px;border-radius:8px;margin-left:auto}

/* QA group — condensed */
.t-qa-group{margin:4px 6px;border-left:3px solid #B8CCE4;border-radius:0 4px 4px 0;overflow:hidden;background:#F0F5FA}
.t-qa-q{padding:5px 8px;background:#E8F0F8}
.t-qa-a{padding:5px 8px;border-top:1px solid #D0DCE8}
.qa-lbl{display:inline-block;font-size:8px;font-weight:800;padding:1px 4px;border-radius:2px;margin-right:4px;vertical-align:middle}
.qa-lbl.q{background:#0A66C2;color:#fff}
.qa-lbl.a{background:#0F7B4E;color:#fff}
.qa-person{font-weight:600;font-size:10px;color:#1A2332}
.qa-affil{font-size:9px;color:#65676B}
.t-qa-text{font-size:10.5px;line-height:1.45;color:#444;margin-top:2px}
.t-qa-footer{padding:3px 7px;background:#fff;border-top:1px solid #E4E6EA;display:flex;align-items:center;gap:6px}
.bucket-pill{font-size:9px;padding:1px 6px;border-radius:8px;color:#fff;font-weight:600;white-space:nowrap}
.importance-chip{font-size:9px;color:#8A8D91;background:#F0F2F5;padding:1px 5px;border-radius:8px}

/* Sentence tokens */
.s-tok{border-radius:2px;padding:0px 1px;cursor:pointer;border-bottom:1.5px solid transparent;transition:opacity .1s;font-size:inherit;line-height:inherit}
.s-tok:hover{opacity:.75}
.s-tok.qa-sent{cursor:default}

/* Transcript: included vs excluded sentence styling */
.s-tok.s-incl{background:rgba(46,125,50,.08);border-bottom-color:rgba(46,125,50,.35);color:#1C1E21}
.s-tok.s-excl{background:transparent;border-bottom-color:transparent;color:#B0B3B8}
.s-tok.s-excl:hover{opacity:.55}

/* Group bullet separator */
.t-gbullet{display:inline;color:#A0A4A8;font-size:11px;margin:0 1px;user-select:none;vertical-align:middle}

/* QA numbered container */
.t-qa-num-label{font-size:9px;font-weight:700;color:#65676B;padding:5px 7px 2px;text-transform:uppercase;letter-spacing:.03em}

/* Cross-highlight: flash sentence in report panel */
.s-tok.s-highlight{outline:2px solid #FF9800;outline-offset:1px;background:rgba(255,152,0,.15)!important;border-radius:3px;animation:s-flash .6s ease-out}
@keyframes s-flash{0%{outline-color:#FF9800;background:rgba(255,152,0,.25)}100%{outline-color:#FF9800;background:rgba(255,152,0,.15)}}
.q-card.q-card-highlight{box-shadow:0 0 0 2px #FF9800;transition:box-shadow .3s}

/* By Category: sentence group within a bucket */
.t-cat-sent-group{margin:1px 6px;padding:4px 7px;background:#fff;border-left:3px solid #E4E6EA;border-radius:0 4px 4px 0}
.t-cat-sent-group+.t-cat-sent-group{margin-top:0;border-top:1px dotted #E4E6EA}
.t-cat-attrib{text-align:right;font-size:9px;color:#8A8D91;font-style:italic;margin-top:2px}

/* Classified view: excluded group dimming */
.t-class-group.t-grp-excl{opacity:.45}
.t-class-group.t-grp-excl .t-class-label{color:#B0B3B8!important}

/* Tab icons */
.tp-view-icon{font-size:10px;margin-right:2px;opacity:.7}
.tp-view-btn.active .tp-view-icon{opacity:1}

/* Sentence popover */
#s-popover{position:fixed;z-index:9999;background:#fff;border:1px solid #E4E6EA;border-radius:10px;box-shadow:0 8px 28px rgba(0,0,0,.14);width:280px;display:none;overflow:hidden}
#s-pop-text{padding:10px 14px;font-size:12px;color:#3C3F44;border-bottom:1px solid #F0F2F5;line-height:1.55;font-style:italic;max-height:80px;overflow:hidden;text-overflow:ellipsis}
#s-pop-title{padding:6px 14px 4px;font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:#8A8D91;background:#F8F9FB;border-bottom:1px solid #F0F2F5}
#s-pop-scores{max-height:220px;overflow-y:auto}
.s-score-row{display:flex;align-items:center;gap:8px;padding:6px 14px;cursor:pointer;transition:background .1s}
.s-score-row:hover{background:#F0F2F5}
.s-score-row.current{background:#EEF3FA}
.s-score-row.readonly{cursor:default}
.s-score-row.readonly:hover{background:transparent}
.s-score-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.s-score-name{flex:1;font-size:12px;color:#1A2332;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.s-score-val{font-size:11px;font-weight:700;color:#65676B;min-width:28px;text-align:right}
.s-score-check{font-size:12px;color:#1565C0;margin-left:2px;min-width:12px}
#s-pop-footer{padding:8px 14px;border-top:1px solid #F0F2F5;background:#F8F9FB}
.s-pop-note{font-size:10px;color:#8A8D91;font-style:italic}

/* Report panel */
#report-panel{flex:1;display:flex;flex-direction:column;background:#4A4A4A;overflow:hidden;min-width:0}
#report-header{flex-shrink:0;background:#3A3A3A;border-bottom:1px solid #333;padding:10px 18px;display:flex;align-items:center;justify-content:space-between}
.report-meta-title{font-size:14px;font-weight:700;color:rgba(255,255,255,.85)}
.report-actions{display:flex;gap:8px}
.report-actions .btn{color:rgba(255,255,255,.75);border-color:rgba(255,255,255,.25);background:transparent}
.report-actions .btn:hover{background:rgba(255,255,255,.1);color:#fff}
#report-body{flex:1;overflow-y:auto;padding:24px 32px}
#report-page{background:#fff;min-height:100%;border-radius:2px;box-shadow:0 2px 12px rgba(0,0,0,.3);padding:32px 36px;max-width:800px;margin:0 auto}

/* Bucket section */
/* Report: L1 section heading */
.rpt-l1{font-size:16px;font-weight:800;color:#1A2332;padding:10px 0 4px;margin:0;border-bottom:2px solid #1A2332;margin-bottom:6px;text-transform:uppercase;letter-spacing:.04em}
.rpt-l1:not(:first-child){margin-top:14px}

/* Report: L2 bucket heading + L3 headline */
.bkt-section{margin-bottom:6px;overflow:hidden}
.bkt-header{display:flex;align-items:center;gap:8px;padding:4px 0;cursor:pointer;user-select:none}
.bkt-bar{width:3px;min-height:24px;border-radius:2px;flex-shrink:0}
.bkt-info{flex:1;min-width:0}
.bkt-name-input{font-size:13px;font-weight:700;color:#1A2332;background:transparent;border:none;outline:none;width:100%;padding:0;cursor:text}
.bkt-name-input:focus{background:#F8F9FB;border-radius:3px;padding:1px 4px;margin:-1px -4px}
.bkt-headline{font-size:10px;color:#65676B;margin-top:1px;font-style:italic}
.bkt-count{font-size:10px;background:#F0F2F5;color:#65676B;padding:1px 7px;border-radius:12px;font-weight:600;white-space:nowrap}
.bkt-chevron{color:#C4C9D0;font-size:12px;transition:transform .15s;flex-shrink:0}
.bkt-chevron.open{transform:rotate(0deg)}
.bkt-chevron.closed{transform:rotate(-90deg)}
.bkt-quotes{padding:2px 0 2px 6px}

/* Quote card (sub-quote) */
.q-card{background:#fff;border:1px solid #E4E6EA;border-radius:6px;margin-bottom:4px;overflow:hidden}
.q-card.sortable-ghost{opacity:.4;background:#E8F4FD}
.q-card.sortable-chosen{box-shadow:0 4px 16px rgba(0,0,0,.14)}
.q-card-header{padding:4px 8px;display:flex;align-items:center;gap:5px;background:#F8F9FB;border-bottom:1px solid #F0F2F5}
.drag-handle{color:#C4C9D0;cursor:grab;font-size:13px;flex-shrink:0;user-select:none}
.drag-handle:active{cursor:grabbing}
.q-attrib{text-align:right;font-size:9px;color:#8A8D91;font-style:italic;padding:0 10px 6px}
.attrib-off .q-attrib{display:none}
.q-bank-badge{font-size:9px;font-weight:600;padding:1px 5px;border-radius:3px;background:#E4E6EA;color:#555}
.q-sec-badge{font-size:9px;padding:1px 5px;border-radius:3px;font-weight:600}
.q-sec-badge.md{background:#E8F4EC;color:#1B6B38}
.q-sec-badge.qa{background:#E8F0FC;color:#1657A8}
.q-imp{font-size:9px;color:#8A8D91}
.q-hdr-spacer{flex:1}
.q-del{background:none;border:none;color:#C4C9D0;cursor:pointer;font-size:15px;padding:0 2px;line-height:1;flex-shrink:0}
.q-del:hover{color:#E02020}
/* Format dropdown button */
.fmt-select{font-size:9px;padding:2px 6px;border:1px solid #CDD0D5;border-radius:3px;background:#fff;cursor:pointer;color:#1A2332;font-weight:600;white-space:nowrap;appearance:none;-webkit-appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='8' height='5'%3E%3Cpath d='M0 0l4 5 4-5z' fill='%238A8D91'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 4px center;padding-right:14px}
.fmt-select:hover{border-color:#0A66C2}
.q-text{padding:6px 10px;font-size:11px;line-height:1.65;color:#3C3F44;max-height:200px;overflow-y:auto}
.q-text p{margin-bottom:5px}
.q-text p:last-child{margin-bottom:0}
.q-expand-btn{font-size:11px;color:#0A66C2;cursor:pointer;padding:3px 12px;display:block;text-align:center;background:#F8F9FB;border:none;width:100%;border-top:1px solid #F0F2F5}
/* QA conversation card extras */
.qa-card-q{padding:8px 12px 4px;background:#FAFBFC;border-bottom:1px solid #F0F2F5;font-size:11px;color:#65676B}
.qa-card-q-text{font-size:12px;color:#444;margin-top:4px;line-height:1.6}

/* Buttons */
.btn{font-size:12px;padding:6px 14px;border-radius:5px;border:1px solid;cursor:pointer;font-weight:600;transition:opacity .1s;white-space:nowrap}
.btn:hover{opacity:.88}
.btn-primary{background:#1A2332;color:#fff;border-color:#1A2332}
.btn-secondary{background:#fff;color:#444;border-color:#CDD0D5}
.btn-secondary:hover{background:#F8F9FB}
.btn-sm{font-size:11px;padding:3px 9px}
.btn-ghost{background:transparent;color:rgba(255,255,255,.75);border-color:rgba(255,255,255,.3)}
.btn-ghost:hover{background:rgba(255,255,255,.1);opacity:1}

/* Add-to-report menu */
.atr-menu{position:fixed;background:#fff;border:1px solid #E4E6EA;border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,.12);z-index:9999;padding:4px 0;min-width:200px}
.atr-item{padding:8px 16px;cursor:pointer;font-size:13px;display:flex;align-items:center;gap:8px}
.atr-item:hover{background:#F0F2F5}
.atr-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}

.empty-bucket{padding:20px 16px;text-align:center;color:#8A8D91;font-size:12px;font-style:italic}
.in-report-tag{font-size:11px;color:#2D9E5F;font-weight:600}
.t-block-actions{margin-top:8px;display:flex;gap:6px;align-items:center}

/* Print */
@media print{
  /* Hide app chrome */
  #app-banner{display:none!important}
  #tab-bar{display:none!important}
  #transcript-wrap{display:none!important}
  #tp-resize{display:none!important}
  #report-header{display:none!important}
  #s-popover{display:none!important}

  /* PDF banner */
  #banner-wrap{display:block!important;text-align:center;padding:8px 0 4px;border:none;margin:0}
  #banner-wrap.banner-off{display:none!important}

  /* Layout */
  #content-area{display:block!important}
  #report-panel{overflow:visible!important;display:block;background:#fff!important}
  #report-body{overflow:visible!important;height:auto;padding:0!important}
  #report-page{box-shadow:none!important;padding:0!important;max-width:none!important}
  body{overflow:visible!important;height:auto}
  #app{height:auto;overflow:visible}

  /* L1 headings */
  .rpt-l1{font-size:18px;padding:16px 0 8px;margin-bottom:12px;page-break-after:avoid}

  /* L2 bucket: just name + headline, no bar/count/chevron */
  .bkt-section{margin-bottom:12px;page-break-inside:avoid}
  .bkt-header{padding:4px 0;cursor:default}
  .bkt-bar{display:none!important}
  .bkt-count{display:none!important}
  .bkt-chevron{display:none!important}
  .bkt-name-input{font-size:14px;cursor:default}
  .bkt-headline{font-size:11px}
  .bkt-quotes{padding:2px 0 2px 8px;display:block!important}

  /* Quote cards: clean bullet points */
  .q-card{background:none!important;border:none!important;border-radius:0!important;margin-bottom:4px;page-break-inside:avoid;padding-left:12px;position:relative}
  .q-card::before{content:"\2022";position:absolute;left:0;top:2px;font-size:11px;line-height:1.5;color:#333}
  .q-card-header{display:none!important}
  .qa-card-q{display:none!important}
  .q-attrib{font-size:9px;padding:0;text-align:right}
  .attrib-off .q-attrib{display:none!important}
  .q-text{padding:2px 0;max-height:none!important;overflow:visible!important;font-size:11px;line-height:1.5}
  .q-text .s-tok{background:none!important;border-bottom:none!important;color:#333!important;padding:0!important;cursor:default}
  .q-expand-btn{display:none!important}
  .empty-bucket{display:none!important}
}

::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:#CDD0D5;border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:#A8ADB5}
</style>
</head>
<body>
<div id="app">
  <div id="app-banner">Investor Relations PM Call Summary</div>
  <div id="banner-wrap">
    <img id="banner-img" src="" alt="Banner">
  </div>
  <div id="tab-bar"></div>
  <div id="content-area">
    <div id="transcript-wrap" class="tp-collapsed" style="width:40%">
      <div id="transcript-panel">
        <div id="tp-content">
          <div id="tp-view-bar" style="position:relative">
            <span class="tp-view-label">View:</span>
            <button class="tp-view-btn active" data-view="blocks" onclick="setTranscriptView('blocks')"><span class="tp-view-icon">&#8801;</span>Blocks</button>
            <button class="tp-view-btn" data-view="classified" onclick="setTranscriptView('classified')"><span class="tp-view-icon">&#9881;</span>Classified</button>
            <button class="tp-view-btn" data-view="category" onclick="setTranscriptView('category')"><span class="tp-view-icon">&#9638;</span>By Category</button>
            <button class="tp-view-help" onclick="toggleViewHelp(event)" title="About view modes">?</button>
            <div id="tp-help-tip">
              <h4>Transcript View Modes</h4>
              <ul>
                <li><strong>Blocks</strong> &mdash; Original transcript layout grouped by speaker. Sentences highlighted green are included in the report draft.</li>
                <li><strong>Classified</strong> &mdash; Speaker blocks with sentences sub-grouped by their assigned financial category. See at a glance how each speaker's remarks break down.</li>
                <li><strong>By Category</strong> &mdash; All sentences across the transcript grouped under their category headers. Useful for seeing everything said about a topic in one place.</li>
              </ul>
            </div>
          </div>
          <div id="transcript-body"></div>
        </div>
        <div id="tp-rail" onclick="toggleTranscript()"><span id="tp-rail-label">Earnings Call Transcript</span></div>
      </div>
    </div>
    <div id="tp-resize" class="tp-hidden"></div>
    <div id="report-panel">
      <div id="report-header">
        <div class="report-meta">
          <div class="report-meta-title">Report Draft</div>
        </div>
        <div class="report-actions">
          <button class="btn btn-secondary" id="attrib-toggle-btn" onclick="toggleAttrib()">Names: Off</button>
          <button class="btn btn-secondary" id="banner-toggle-btn" onclick="toggleBanner()">Banner: On</button>
          <button class="btn btn-secondary" onclick="saveReport()">&#128190; Save</button>
          <button class="btn btn-secondary" onclick="window.print()">&#128196; PDF</button>
        </div>
      </div>
      <div id="report-body"><div id="report-page" class="attrib-off"></div></div>
    </div>
  </div>
</div>

<!-- Sentence score popover (single DOM element, repositioned on click) -->
<div id="s-popover">
  <div id="s-pop-text"></div>
  <div id="s-pop-title">Category Scores</div>
  <div id="s-pop-scores"></div>
  <div id="s-pop-footer"><span class="s-pop-note" id="s-pop-note"></span></div>
</div>

<script src="https://cdn.jsdelivr.net/npm/sortablejs@1.15.6/Sortable.min.js"></script>

<script type="application/json" id="state-data">
/* __BEGIN_STATE__ */
__STATE_JSON__
/* __END_STATE__ */
</script>

<script>
// ============================================================
// BOOT
// ============================================================
const MIN_IMPORTANCE = __MIN_IMPORTANCE__;
let __HTML_TPL__ = null;
let state = null;
let sortableInstances = [];
let _activeSid = null; // sentence currently shown in popover
let _transcriptView = 'blocks'; // 'blocks' | 'classified' | 'category'

document.addEventListener('DOMContentLoaded', () => {
  __HTML_TPL__ = document.documentElement.outerHTML;
  state = JSON.parse(
    document.getElementById('state-data').textContent
      .replace(/\/\*\s*__BEGIN_STATE__\s*\*\//,'')
      .replace(/\/\*\s*__END_STATE__\s*\*\//, '')
      .trim()
  );
  document.addEventListener('mousedown', maybeClosePopover);
  renderApp();
});

function renderApp() {
  renderBanner();
  renderTabs();
  renderCurrentBank();
}

// ============================================================
// BANNER (PDF only — toggle on/off for print output)
// ============================================================
function renderBanner() {
  const wrap = document.getElementById('banner-wrap');
  const img = document.getElementById('banner-img');
  const btn = document.getElementById('banner-toggle-btn');
  if (state.banner_src) { img.src = state.banner_src; }
  if (state.banner_visible) {
    wrap.classList.remove('banner-off');
    if (btn) btn.textContent = 'Banner: On';
  } else {
    wrap.classList.add('banner-off');
    if (btn) btn.textContent = 'Banner: Off';
  }
}
function toggleBanner() { state.banner_visible = !state.banner_visible; renderBanner(); }

let _attribVisible = false;
function toggleAttrib() {
  _attribVisible = !_attribVisible;
  document.getElementById('report-page').classList.toggle('attrib-off', !_attribVisible);
  document.getElementById('attrib-toggle-btn').textContent = _attribVisible ? 'Names: On' : 'Names: Off';
}

// ============================================================
// TABS
// ============================================================
function renderTabs() {
  const bar = document.getElementById('tab-bar');
  const fq = state.meta && state.meta.fiscal_quarter || '';
  const fy = state.meta && state.meta.fiscal_year || '';
  const shortYear = fy.length === 4 ? "'" + fy.slice(2) : fy;
  const period = fq && fy ? ` ${fq}${shortYear}` : '';
  bar.innerHTML = Object.entries(state.banks).map(([id, bank]) =>
    `<button class="tab-btn${id === state.current_bank ? ' active' : ''}" onclick="selectBank('${id}')">
       ${esc(bank.company_name)}${period ? `<span style="font-weight:400;color:#65676B;margin-left:4px;font-size:11px">${esc(period)}</span>` : ''}</button>`
  ).join('');
}
function selectBank(id) {
  state.current_bank = id; renderTabs(); renderCurrentBank();
}
function renderCurrentBank() {
  renderTranscriptPanel(); renderReportPanel();
}

// ============================================================
// TRANSCRIPT PANEL — COLLAPSE / VIEW MODES
// ============================================================
let _tpSavedWidth = null;
function toggleTranscript() {
  const wrap = document.getElementById('transcript-wrap');
  const resize = document.getElementById('tp-resize');
  const isCollapsed = wrap.classList.contains('tp-collapsed');
  if (isCollapsed) {
    wrap.classList.remove('tp-collapsed');
    wrap.style.width = _tpSavedWidth || '40%';
    resize.classList.remove('tp-hidden');
  } else {
    _tpSavedWidth = wrap.style.width || '40%';
    wrap.classList.add('tp-collapsed');
    wrap.style.width = '';
    resize.classList.add('tp-hidden');
  }
}
function setTranscriptView(mode) {
  _transcriptView = mode;
  document.querySelectorAll('.tp-view-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.view === mode));
  renderTranscriptBody();
}
function toggleViewHelp(evt) {
  evt.stopPropagation();
  const tip = document.getElementById('tp-help-tip');
  tip.classList.toggle('visible');
  if (tip.classList.contains('visible')) {
    const btn = evt.currentTarget;
    const bar = btn.closest('#tp-view-bar');
    const barRect = bar.getBoundingClientRect();
    const btnRect = btn.getBoundingClientRect();
    tip.style.top = (btnRect.bottom - barRect.top + 4) + 'px';
    tip.style.right = '4px';
    const close = e => { if (!tip.contains(e.target) && e.target !== btn) { tip.classList.remove('visible'); document.removeEventListener('mousedown', close); } };
    setTimeout(() => document.addEventListener('mousedown', close), 0);
  }
}

// ── Resize handle for transcript panel ──
(function initResize() {
  document.addEventListener('DOMContentLoaded', () => {
    const handle = document.getElementById('tp-resize');
    const wrap = document.getElementById('transcript-wrap');
    if (!handle || !wrap) return;
    let startX, startW;
    handle.addEventListener('mousedown', e => {
      e.preventDefault();
      startX = e.clientX;
      startW = wrap.offsetWidth;
      handle.classList.add('active');
      const onMove = ev => {
        const delta = ev.clientX - startX;
        const newW = Math.max(200, Math.min(startW + delta, window.innerWidth * 0.7));
        wrap.style.width = newW + 'px';
      };
      const onUp = () => {
        handle.classList.remove('active');
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
      };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  });
})();

// ============================================================
// TRANSCRIPT PANEL — RENDER
// ============================================================
function renderTranscriptPanel() {
  renderTranscriptBody();
}

function getBucket(bid) {
  return state.buckets.find(b => b.id === bid) ||
         {id:'other',name:'Other',color_bg:'#FAFAFA',color_accent:'#9E9E9E'};
}

function effectivePrimary(sid) {
  const bs = getBankState(state.current_bank);
  return (bs.sentence_user_primary && bs.sentence_user_primary[sid]) ||
         findSentence(sid)?.primary || 'other';
}

function renderTranscriptBody() {
  if (_transcriptView === 'classified') return renderTranscriptBodyClassified();
  if (_transcriptView === 'category') return renderTranscriptBodyByCategory();
  renderTranscriptBodyBlocks();
}

// ── Helpers: inclusion check + consecutive-category grouping ──
function computeIncludedSids(bankId) {
  const inc = new Set();
  const all = getAllSubquotes(bankId);
  for (const sq of all) {
    if (getSubquoteImportance(sq) < MIN_IMPORTANCE) continue;
    const sents = sq.type === 'md' ? sq.sentences
      : [...(sq.question_sentences||[]), ...(sq.answer_sentences||[])];
    for (const s of sents) inc.add(s.sid);
  }
  return inc;
}
function groupByCat(sentences) {
  const groups = []; let cur = null;
  for (const s of sentences) {
    const ep = effectivePrimary(s.sid);
    if (!cur || cur.bid !== ep) { if (cur) groups.push(cur); cur = {bid:ep, sents:[s]}; }
    else cur.sents.push(s);
  }
  if (cur) groups.push(cur);
  return groups;
}
function renderGroupedSpans(groups, btype, inc) {
  return groups.map((g, i) => {
    const bullet = i > 0 ? '<span class="t-gbullet">&bull;</span>' : '';
    return bullet + g.sents.map(s => tSpan(s, btype, inc)).join(' ');
  }).join(' ');
}

// ── Render sentences with paragraph breaks ──
function tSpansWithParas(sentences, btype, inc) {
  let html = '', lastPara = -1;
  sentences.forEach(s => {
    const pi = s.para_idx != null ? s.para_idx : 0;
    if (lastPara >= 0 && pi !== lastPara) html += '<span class="t-para-break"></span>';
    lastPara = pi;
    html += tSpan(s, btype, inc) + ' ';
  });
  return html;
}

// ── Transcript sentence span (included/excluded styling) ──
function tSpan(sent, blockType, inc) {
  const bid = effectivePrimary(sent.sid);
  const isQa = blockType === 'qa';
  const cls = inc && inc.has(sent.sid) ? 's-incl' : 's-excl';
  const scores64 = btoa(unescape(encodeURIComponent(JSON.stringify(sent.scores))));
  return `<span class="s-tok ${cls}${isQa?' qa-sent':''}"
    data-sid="${sent.sid}" data-btype="${blockType}" data-primary="${bid}" data-scores="${scores64}"
    onclick="showSentencePopover(event,this)">${esc(sent.text)}</span>`;
}
// ── Report-panel sentence span (per-bucket color styling) ──
function sentenceSpan(sent, blockType) {
  const bid = effectivePrimary(sent.sid);
  const bkt = getBucket(bid);
  const isQa = blockType === 'qa';
  const scores64 = btoa(unescape(encodeURIComponent(JSON.stringify(sent.scores))));
  return `<span class="s-tok${isQa?' qa-sent':''}"
    data-sid="${sent.sid}" data-btype="${blockType}" data-primary="${bid}" data-scores="${scores64}"
    style="background:${bkt.color_bg};border-bottom-color:${bkt.color_accent}"
    onclick="showSentencePopover(event,this)">${esc(sent.text)}</span>`;
}

// ── View: Blocks ──
function renderTranscriptBodyBlocks() {
  const bank = state.banks[state.current_bank];
  const inc = computeIncludedSids(state.current_bank);
  let html = '';
  if (bank.md_blocks && bank.md_blocks.length) {
    html += `<div class="t-section-header">Management Discussion</div>`;
    bank.md_blocks.forEach(blk => {
      const sentHtml = tSpansWithParas(blk.sentences, 'md', inc);
      html += `<div class="t-block">
        <div class="t-block-header"><span class="t-speaker-name">${esc(blk.speaker)}</span>
        ${blk.speaker_title ? `<span class="t-speaker-title">${esc(blk.speaker_title)}</span>` : ''}</div>
        <div class="t-block-text">${sentHtml}</div></div>`;
    });
  }
  if (bank.qa_conversations && bank.qa_conversations.length) {
    html += `<div class="t-section-header">Questions &amp; Answers</div>`;
    bank.qa_conversations.forEach((conv, ci) => {
      const qSentHtml = tSpansWithParas(conv.question_sentences, 'qa', inc);
      const aSentHtml = tSpansWithParas(conv.answer_sentences, 'qa', inc);
      const qLabel = conv.analyst_name + (conv.analyst_affiliation ? ` (${conv.analyst_affiliation})` : '');
      const aLabel = conv.executive_name + (conv.executive_title ? `, ${conv.executive_title}` : '');
      html += `<div class="t-qa-group">
        <div class="t-qa-num-label">Question ${ci+1}</div>
        <div class="t-qa-q"><span class="qa-lbl q">Q</span><span class="qa-person">${esc(qLabel)}</span>
          <div class="t-qa-text">${qSentHtml}</div></div>
        <div class="t-qa-a"><span class="qa-lbl a">A</span><span class="qa-person">${esc(aLabel)}</span>
          <div class="t-qa-text">${aSentHtml}</div></div></div>`;
    });
  }
  document.getElementById('transcript-body').innerHTML = html;
}

// ── View: Classified (sub-grouped by category within each block) ──
function renderTranscriptBodyClassified() {
  const bank = state.banks[state.current_bank];
  const inc = computeIncludedSids(state.current_bank);
  let html = '';
  if (bank.md_blocks && bank.md_blocks.length) {
    html += `<div class="t-section-header">Management Discussion</div>`;
    bank.md_blocks.forEach(blk => { html += renderMDBlockClassified(blk, inc); });
  }
  if (bank.qa_conversations && bank.qa_conversations.length) {
    html += `<div class="t-section-header">Questions &amp; Answers</div>`;
    bank.qa_conversations.forEach((conv, ci) => { html += renderQAConvClassified(conv, inc, ci); });
  }
  document.getElementById('transcript-body').innerHTML = html;
}
function renderMDBlockClassified(blk, inc) {
  const groups = groupByCat(blk.sentences);
  const groupsHtml = groups.map(g => {
    const bkt = getBucket(g.bid);
    const grpInc = g.sents.some(s => inc.has(s.sid));
    const cls = grpInc ? '' : ' t-grp-excl';
    const spans = tSpansWithParas(g.sents, 'md', inc);
    return `<div class="t-class-group${cls}" style="border-left-color:${bkt.color_accent};background:${grpInc?bkt.color_bg:'#FAFAFA'}">
      <div class="t-class-label" style="color:${bkt.color_accent}"><span class="t-gbullet">&bull;</span> ${esc(bkt.name)}</div>
      <div class="t-block-text">${spans}</div></div>`;
  }).join('');
  return `<div class="t-block">
    <div class="t-block-header"><span class="t-speaker-name">${esc(blk.speaker)}</span>
    ${blk.speaker_title ? `<span class="t-speaker-title">${esc(blk.speaker_title)}</span>` : ''}</div>
    ${groupsHtml}</div>`;
}
function renderQAConvClassified(conv, inc, convIdx) {
  const qSpans = tSpansWithParas(conv.question_sentences, 'qa', inc);
  const aGroups = groupByCat(conv.answer_sentences);
  const aHtml = aGroups.map(g => {
    const bkt = getBucket(g.bid);
    const grpInc = g.sents.some(s => inc.has(s.sid));
    const cls = grpInc ? '' : ' t-grp-excl';
    const spans = tSpansWithParas(g.sents, 'qa', inc);
    return `<div class="t-class-group${cls}" style="border-left-color:${bkt.color_accent};background:${grpInc?bkt.color_bg:'#FAFAFA'}">
      <div class="t-class-label" style="color:${bkt.color_accent}"><span class="t-gbullet">&bull;</span> ${esc(bkt.name)}</div>
      <div class="t-block-text">${spans}</div></div>`;
  }).join('');
  const qLabel = conv.analyst_name + (conv.analyst_affiliation ? ` (${conv.analyst_affiliation})` : '');
  const aLabel = conv.executive_name + (conv.executive_title ? `, ${conv.executive_title}` : '');
  return `<div class="t-qa-group">
    <div class="t-qa-num-label">Question ${convIdx+1}</div>
    <div class="t-qa-q"><span class="qa-lbl q">Q</span><span class="qa-person">${esc(qLabel)}</span>
      <div class="t-qa-text">${qSpans}</div></div>
    <div class="t-qa-a"><span class="qa-lbl a">A</span><span class="qa-person">${esc(aLabel)}</span>
      ${aHtml}</div></div>`;
}

// ── View: By Category ──
function renderTranscriptBodyByCategory() {
  const bank = state.banks[state.current_bank];
  const inc = computeIncludedSids(state.current_bank);
  const allSents = [];
  (bank.md_blocks || []).forEach(blk => {
    blk.sentences.forEach(s => allSents.push({sent:s, type:'md', speaker:blk.speaker, speakerTitle:blk.speaker_title||'', blkId:blk.id}));
  });
  (bank.qa_conversations || []).forEach(conv => {
    conv.question_sentences.forEach(s => allSents.push({sent:s, type:'qa-q', speaker:conv.analyst_name, speakerTitle:conv.analyst_affiliation||'', blkId:conv.id}));
    conv.answer_sentences.forEach(s => allSents.push({sent:s, type:'qa-a', speaker:conv.executive_name, speakerTitle:conv.executive_title||'', blkId:conv.id}));
  });
  const byBucket = {};
  allSents.forEach(item => { const bid = effectivePrimary(item.sent.sid); if (!byBucket[bid]) byBucket[bid]=[]; byBucket[bid].push(item); });

  let html = '';
  state.buckets.forEach(bucket => {
    const items = byBucket[bucket.id];
    if (!items || !items.length) return;
    const incCount = items.filter(i => inc.has(i.sent.sid)).length;
    html += `<div class="t-cat-header"><strong>${esc(bucket.name)}</strong><span class="t-cat-count">${incCount}/${items.length}</span></div>`;
    // Group consecutive sentences from same speaker+block into bullet groups
    const groups = [];
    let cur = null;
    items.forEach(item => {
      if (!cur || cur.speaker !== item.speaker || cur.blkId !== item.blkId) {
        if (cur) groups.push(cur);
        cur = {speaker:item.speaker, speakerTitle:item.speakerTitle, type:item.type, blkId:item.blkId, sents:[item]};
      } else { cur.sents.push(item); }
    });
    if (cur) groups.push(cur);
    groups.forEach(g => {
      const btype = g.type.startsWith('qa') ? 'qa' : 'md';
      const spans = g.sents.map(i => tSpan(i.sent, btype, inc)).join(' ');
      const attrib = g.speakerTitle ? `${g.speaker}, ${g.speakerTitle}` : g.speaker;
      html += `<div class="t-cat-sent-group">
        <div class="t-block-text"><span class="t-gbullet">&bull;</span> ${spans}</div>
        <div class="t-cat-attrib">&mdash; ${esc(attrib)}</div></div>`;
    });
  });
  document.getElementById('transcript-body').innerHTML = html;
}

// ============================================================
// SENTENCE POPOVER
// ============================================================
function showSentencePopover(evt, el) {
  evt.stopPropagation();
  const sid    = el.dataset.sid;
  const btype  = el.dataset.btype;
  const scores = JSON.parse(decodeURIComponent(escape(atob(el.dataset.scores))));
  const sent   = findSentence(sid);
  if (!sent) return;

  _activeSid = sid;

  // Cross-highlight: find this sentence in the report panel and scroll to it
  highlightInReport(sid);

  // Populate text preview
  document.getElementById('s-pop-text').textContent = `"${sent.text}"`;

  // Note line
  const readonly = btype === 'qa';
  document.getElementById('s-pop-note').textContent = readonly
    ? 'QA: reassign whole conversation via drag-drop'
    : 'Click a category to reassign this sentence';

  // Score rows — show all buckets scoring > 0.5, sorted desc; always show current
  const curPrimary = effectivePrimary(sid);
  const rows = state.buckets
    .filter(b => b.id !== 'other')
    .map(b => ({ b, score: scores[b.id] || 0 }))
    .filter(({b, score}) => score > 0.5 || b.id === curPrimary)
    .sort((a, b) => b.score - a.score);

  // Also add Other if it's current
  if (curPrimary === 'other') {
    rows.push({ b: getBucket('other'), score: 0 });
  }

  const scoresHtml = rows.map(({b, score}) => {
    const isCur = b.id === curPrimary;
    const cls   = `s-score-row${isCur?' current':''}${readonly?' readonly':''}`;
    const onclick = readonly ? '' : `onclick="reassignSentence('${sid}','${b.id}')"`;
    return `<div class="${cls}" ${onclick}>
      <div class="s-score-dot" style="background:${b.color_accent}"></div>
      <div class="s-score-name">${esc(b.name)}</div>
      <div class="s-score-val">${score.toFixed(1)}</div>
      <div class="s-score-check">${isCur ? '&#10003;' : ''}</div>
    </div>`;
  }).join('');

  // If not readonly, add Other option at bottom
  const otherRow = !readonly && curPrimary !== 'other' ? `
    <div class="s-score-row" onclick="reassignSentence('${sid}','other')">
      <div class="s-score-dot" style="background:#9E9E9E"></div>
      <div class="s-score-name">Other</div>
      <div class="s-score-val">—</div>
      <div class="s-score-check"></div>
    </div>` : '';

  document.getElementById('s-pop-scores').innerHTML = scoresHtml + otherRow;

  // Position near click
  const pop = document.getElementById('s-popover');
  pop.style.display = 'block';
  const pw = 280, ph_est = 320;
  let left = evt.clientX + 8;
  let top  = evt.clientY + 8;
  if (left + pw > window.innerWidth - 8)  left = evt.clientX - pw - 8;
  if (top + ph_est > window.innerHeight - 8) top = evt.clientY - ph_est - 8;
  pop.style.left = Math.max(8, left) + 'px';
  pop.style.top  = Math.max(8, top)  + 'px';
}

function highlightInReport(sid) {
  // Clear previous highlights
  document.querySelectorAll('#report-body .s-highlight').forEach(el => el.classList.remove('s-highlight'));
  document.querySelectorAll('#report-body .q-card-highlight').forEach(el => el.classList.remove('q-card-highlight'));
  // Find matching sentence span in the report panel
  const match = document.querySelector(`#report-body .s-tok[data-sid="${sid}"]`);
  if (match) {
    match.classList.add('s-highlight');
    const card = match.closest('.q-card');
    if (card) card.classList.add('q-card-highlight');
    // Scroll the report panel to show the match
    match.scrollIntoView({behavior:'smooth', block:'center'});
  }
}

function maybeClosePopover(evt) {
  const pop = document.getElementById('s-popover');
  if (pop.style.display !== 'none' && !pop.contains(evt.target)) {
    pop.style.display = 'none';
    _activeSid = null;
    // Clear report highlights when popover closes
    document.querySelectorAll('#report-body .s-highlight').forEach(el => el.classList.remove('s-highlight'));
    document.querySelectorAll('#report-body .q-card-highlight').forEach(el => el.classList.remove('q-card-highlight'));
  }
}

// ============================================================
// SENTENCE REASSIGNMENT (MD only) — Option A: immediate full re-render
// ============================================================
function reassignSentence(sid, newBucketId) {
  const bs = getBankState(state.current_bank);
  if (!bs.sentence_user_primary) bs.sentence_user_primary = {};
  bs.sentence_user_primary[sid] = newBucketId;
  document.getElementById('s-popover').style.display = 'none';
  _activeSid = null;
  renderCurrentBank(); // full re-render: transcript colors + report groupings
}

// ============================================================
// SUB-QUOTE COMPUTATION (programmatic from sentence data)
// ============================================================

/**
 * Group consecutive sentences with the same effective primary into sub-quotes.
 * Sub-quote ID = "SQ_" + first sentence SID.
 */
function computeMDSubquotes(blk, bs) {
  const excluded = new Set(bs.excluded_sentences || []);
  const groups = [];
  let cur = null;

  for (const sent of blk.sentences) {
    if (excluded.has(sent.sid)) {
      if (cur) { groups.push(cur); cur = null; }
      continue;
    }
    const ep = (bs.sentence_user_primary && bs.sentence_user_primary[sent.sid]) || sent.primary;
    // Check for whole-subquote bucket override (from drag-drop)
    const sqid = cur ? cur.id : `SQ_${sent.sid}`;
    const bucketOverride = bs.subquote_bucket_overrides && bs.subquote_bucket_overrides[sqid];
    const effectiveBucket = bucketOverride || ep;

    if (!cur || cur.effective_bucket !== effectiveBucket) {
      if (cur) groups.push(cur);
      cur = {
        id: `SQ_${sent.sid}`,
        type: 'md',
        block_id: blk.id,
        speaker: blk.speaker,
        speaker_title: blk.speaker_title || '',
        effective_bucket: effectiveBucket,
        sentences: [sent],
      };
    } else {
      cur.sentences.push(sent);
    }
  }
  if (cur) groups.push(cur);
  return groups;
}

/**
 * For QA conversations, the entire conversation is one sub-quote unit.
 * The effective bucket = whole-conv override || primary_bucket.
 */
function computeQASubquotes(conv, bs) {
  const excluded = new Set(bs.excluded_sentences || []);
  // If ALL answer sentences are excluded, skip this conversation
  const hasAnswers = conv.answer_sentences.some(s => !excluded.has(s.sid));
  if (!hasAnswers) return [];

  const sqid = `SQ_${conv.id}`;
  const override = bs.subquote_bucket_overrides && bs.subquote_bucket_overrides[sqid];
  const effectiveBucket = override || conv.primary_bucket || 'other';

  return [{
    id: sqid,
    type: 'qa',
    conv_id: conv.id,
    analyst_name: conv.analyst_name,
    analyst_affiliation: conv.analyst_affiliation,
    executive_name: conv.executive_name,
    executive_title: conv.executive_title,
    effective_bucket: effectiveBucket,
    question_sentences: conv.question_sentences,
    answer_sentences: conv.answer_sentences.filter(s => !excluded.has(s.sid)),
  }];
}

function getAllSubquotes(bankId) {
  const bank = state.banks[bankId];
  const bs   = getBankState(bankId);
  const result = [];
  (bank.md_blocks || []).forEach(blk => result.push(...computeMDSubquotes(blk, bs)));
  (bank.qa_conversations || []).forEach(conv => result.push(...computeQASubquotes(conv, bs)));
  return result;
}

function getSubquoteImportance(sq) {
  const sents = sq.type === 'md' ? sq.sentences : sq.answer_sentences;
  if (!sents || !sents.length) return 0;
  return Math.max(...sents.map(s => s.importance_score || 0));
}

function getSubquoteFormats(sq, fmt) {
  const sents = sq.type === 'md' ? sq.sentences : sq.answer_sentences;
  if (!sents) return '';
  const key = fmt === 'verbatim' ? 'text' : fmt;
  return sents.map(s => s[key] || s.text || '').join(' ').trim();
}

function getReportSubquotes(bankId, bucketId, sourceType) {
  const bs  = getBankState(bankId);
  const all = getAllSubquotes(bankId);

  let eligible = all.filter(sq =>
    sq.effective_bucket === bucketId &&
    getSubquoteImportance(sq) >= MIN_IMPORTANCE
  );

  // Filter by source type (md or qa) when specified
  if (sourceType) {
    eligible = eligible.filter(sq => sq.type === sourceType);
  }

  const order = bs.bucket_subquote_order && bs.bucket_subquote_order[bucketId];
  if (order && order.length) {
    const ordered = [];
    order.forEach(id => {
      const sq = eligible.find(x => x.id === id);
      if (sq) ordered.push(sq);
    });
    eligible.forEach(sq => { if (!order.includes(sq.id)) ordered.push(sq); });
    return ordered;
  }
  return [...eligible].sort((a, b) => getSubquoteImportance(b) - getSubquoteImportance(a));
}

// ============================================================
// REPORT PANEL
// ============================================================
function renderReportPanel() {
  renderReportSections();
}

function renderReportSections() {
  destroySortables();
  let html = '';

  // Fixed L1 sections: MD content under Results Summary, QA content under Earnings Call Q&A
  const reportSections = [
    { name: 'Results Summary', sourceType: 'md' },
    { name: 'Earnings Call Q&A', sourceType: 'qa' },
  ];

  reportSections.forEach(({name: secName, sourceType}) => {
    const hasContent = state.buckets.some(b =>
      getReportSubquotes(state.current_bank, b.id, sourceType).length > 0
    );
    if (!hasContent) return;

    html += `<div class="rpt-l1">${secName}</div>`;

    state.buckets.forEach(bucket => {
      const quotes = getReportSubquotes(state.current_bank, bucket.id, sourceType);
      if (quotes.length === 0) return;

      const compositeId = `${bucket.id}__${sourceType}`;
      const userTitle = (state.bucket_user_titles || {})[bucket.id];
      const titleVal  = userTitle != null ? userTitle : bucket.name;

      html += `
<div class="bkt-section" id="bs_${compositeId}">
  <div class="bkt-header" onclick="toggleBucket('${compositeId}')">
    <div class="bkt-bar" style="background:${bucket.color_accent}"></div>
    <div class="bkt-info">
      <input class="bkt-name-input" value="${escAttr(titleVal)}"
             onclick="event.stopPropagation()"
             onblur="saveBucketTitle('${bucket.id}',this.value)"
             onkeydown="if(event.key==='Enter')this.blur()">
      ${bucket.generated_headline ? `<div class="bkt-headline">${esc(bucket.generated_headline)}</div>` : ''}
    </div>
    <span class="bkt-count" id="bkc_${compositeId}">${quotes.length}</span>
    <span class="bkt-chevron open" id="bkv_${compositeId}">&#9662;</span>
  </div>
  <div class="bkt-quotes" id="bq_${compositeId}" data-bucket-id="${compositeId}">
    ${quotes.map(sq => renderSubquoteCard(sq, bucket)).join('')}
  </div>
</div>`;
    });
  });

  document.getElementById('report-page').innerHTML = html;
  initSortables();
}

function renderSubquoteCard(sq, bucket) {
  const bankId = state.current_bank;
  const bank   = state.banks[bankId];
  const bs     = getBankState(bankId);
  const fmt    = (bs.subquote_formats && bs.subquote_formats[sq.id]) || 'verbatim';
  const imp    = getSubquoteImportance(sq).toFixed(1);
  const isMD   = sq.type === 'md';

  const secBadge = isMD
    ? `<span class="q-sec-badge md">MD</span>`
    : `<span class="q-sec-badge qa">Q&amp;A</span>`;

  const textContent = renderSubquoteText(sq, fmt, bs);
  const isLong = getSubquoteFormats(sq, 'verbatim').length > 600;
  const expandBtn = isLong
    ? `<button class="q-expand-btn" onclick="expandCard(this)">Show more &#9662;</button>`
    : '';

  // Attribution line
  const attribName = isMD ? sq.speaker : (sq.executive_name || 'Executive');
  const attribTitle = isMD ? (sq.speaker_title || '') : (sq.executive_title || '');
  const attribLine = attribTitle ? `${attribName}, ${attribTitle}` : attribName;

  // QA card shows question as context above answer text
  const qaContextHtml = !isMD ? `
    <div class="qa-card-q">
      <span class="qa-lbl q">Q</span>
      <strong>${esc(sq.analyst_name||'Analyst')}</strong>
      ${sq.analyst_affiliation ? `<span style="color:#65676B"> (${esc(sq.analyst_affiliation)})</span>` : ''}
      <div class="qa-card-q-text">${sq.question_sentences.map(s=>esc(s.text)).join(' ')}</div>
    </div>` : '';

  return `
<div class="q-card" data-subquote-id="${sq.id}">
  <div class="q-card-header">
    <span class="drag-handle">&#8942;</span>
    <select class="fmt-select" onchange="switchFmt('${sq.id}',this.value)">
      ${['verbatim','condensed','summary','paraphrase'].map(f =>
        `<option value="${f}"${fmt===f?' selected':''}>${fmtLabel(f)}</option>`
      ).join('')}
    </select>
    <span class="q-bank-badge">${esc(bank.ticker)}</span>
    ${secBadge}
    <span class="q-imp">&#9733; ${imp}</span>
    <span class="q-hdr-spacer"></span>
    <button class="q-del" onclick="deleteSubquote('${sq.id}')" title="Remove from report">&#215;</button>
  </div>
  ${qaContextHtml}
  <div class="q-text">${textContent}</div>
  ${expandBtn}
  <div class="q-attrib">&mdash; ${esc(attribLine)}</div>
</div>`;
}

function renderSubquoteText(sq, fmt, bs) {
  const useSpans = fmt === 'verbatim' || fmt === 'condensed';
  const sents = sq.type === 'md' ? sq.sentences : sq.answer_sentences;
  const isQA = sq.type === 'qa';

  if (!sents) return '';

  if (useSpans) {
    // Show sentence spans (clickable for MD, read-only for QA)
    return `<p>${sents.map(s => {
      const bid = isQA ? (s.primary) : effectivePrimary(s.sid);
      const bkt = getBucket(bid);
      const text = fmt === 'condensed' ? (s.condensed || s.text) : s.text;
      const scores64 = btoa(unescape(encodeURIComponent(JSON.stringify(s.scores))));
      return `<span class="s-tok${isQA?' qa-sent':''}"
        data-sid="${s.sid}" data-btype="${isQA?'qa':'md'}"
        data-primary="${bid}" data-scores="${scores64}"
        style="background:${bkt.color_bg};border-bottom-color:${bkt.color_accent}"
        onclick="showSentencePopover(event,this)">${esc(text)}</span>`;
    }).join(' ')}</p>`;
  }

  // summary / paraphrase — plain text, no spans
  const text = sents.map(s => s[fmt] || s.text || '').join(' ').trim();
  return text.split(/\n\n+/).filter(Boolean).map(p => `<p>${esc(p.trim())}</p>`).join('');
}

function fmtLabel(f) {
  return {verbatim:'Verbatim',condensed:'Condensed',summary:'Summary',paraphrase:'Paraphrase'}[f]||f;
}
function fmtIcon(f) {
  return {verbatim:'&#8220;',condensed:'&#8796;',summary:'&#9776;',paraphrase:'&#8634;'}[f]||'?';
}
function fmtTitle(f) {
  return {verbatim:'Verbatim — original text',condensed:'Condensed — shortened',summary:'Summary — key points',paraphrase:'Paraphrase — rewritten'}[f]||f;
}

function expandCard(btn) {
  btn.closest('.q-card').querySelector('.q-text').style.maxHeight = 'none';
  btn.remove();
}

// ============================================================
// DRAG & DROP (Sortable.js — subquote level)
// ============================================================
function destroySortables() {
  sortableInstances.forEach(s => s.destroy());
  sortableInstances = [];
}
function initSortables() {
  if (typeof Sortable === 'undefined') return;
  document.querySelectorAll('.bkt-quotes').forEach(container => {
    const compositeId = container.dataset.bucketId || '';
    const sourceType = compositeId.includes('__qa') ? 'qa' : 'md';
    sortableInstances.push(Sortable.create(container, {
      group: `report-sq-${sourceType}`,
      animation: 150,
      handle: '.drag-handle',
      ghostClass: 'sortable-ghost',
      chosenClass: 'sortable-chosen',
      onEnd(evt) {
        const sqid = evt.item.dataset.subquoteId;
        const from = evt.from.dataset.bucketId;
        const to   = evt.to.dataset.bucketId;
        handleSubquoteDrop(sqid, from, to);
      },
    }));
  });
}

function handleSubquoteDrop(sqid, fromComposite, toComposite) {
  // Composite IDs are "bucket_N__md" or "bucket_N__qa" — extract the real bucket ID
  const fromBid = fromComposite.replace(/__(?:md|qa)$/, '');
  const toBid   = toComposite.replace(/__(?:md|qa)$/, '');
  const bs = getBankState(state.current_bank);

  if (fromBid !== toBid) {
    // Override bucket for this entire sub-quote
    if (!bs.subquote_bucket_overrides) bs.subquote_bucket_overrides = {};
    bs.subquote_bucket_overrides[sqid] = toBid;

    // Also set sentence_user_primary for all sentences in the sub-quote
    // so transcript coloring stays in sync
    const sq = getAllSubquotes(state.current_bank).find(x => x.id === sqid);
    if (sq) {
      if (!bs.sentence_user_primary) bs.sentence_user_primary = {};
      const sents = sq.type === 'md' ? sq.sentences : sq.answer_sentences;
      (sents || []).forEach(s => { bs.sentence_user_primary[s.sid] = toBid; });
    }
  }

  // Capture DOM order for target bucket (using composite ID for DOM lookup)
  const targetEl = document.getElementById(`bq_${toComposite}`);
  if (targetEl) {
    const ids = [...targetEl.querySelectorAll('[data-subquote-id]')].map(el => el.dataset.subquoteId);
    if (!bs.bucket_subquote_order) bs.bucket_subquote_order = {};
    bs.bucket_subquote_order[toBid] = ids;
  }
  if (fromBid !== toBid) {
    const fromEl = document.getElementById(`bq_${fromComposite}`);
    if (fromEl) {
      const ids = [...fromEl.querySelectorAll('[data-subquote-id]')].map(el => el.dataset.subquoteId);
      if (!bs.bucket_subquote_order) bs.bucket_subquote_order = {};
      bs.bucket_subquote_order[fromBid] = ids;
    }
    updateBucketCount(fromComposite);
  }
  updateBucketCount(toComposite);

  // Update transcript panel for moved sentences
  renderTranscriptBody();
}

// ============================================================
// QUOTE CARD ACTIONS
// ============================================================
function switchFmt(sqid, fmt) {
  const bs = getBankState(state.current_bank);
  if (!bs.subquote_formats) bs.subquote_formats = {};
  bs.subquote_formats[sqid] = fmt;

  const card = document.querySelector(`[data-subquote-id="${sqid}"]`);
  if (!card) return;
  const sq  = getAllSubquotes(state.current_bank).find(x => x.id === sqid);
  if (!sq) return;

  const textDiv = card.querySelector('.q-text');
  textDiv.innerHTML = renderSubquoteText(sq, fmt, bs);
}

function deleteSubquote(sqid) {
  const bs = getBankState(state.current_bank);
  const sq = getAllSubquotes(state.current_bank).find(x => x.id === sqid);
  if (!sq) return;

  if (!bs.excluded_sentences) bs.excluded_sentences = [];
  const sents = sq.type === 'md' ? sq.sentences : sq.answer_sentences;
  (sents || []).forEach(s => {
    if (!bs.excluded_sentences.includes(s.sid)) bs.excluded_sentences.push(s.sid);
  });

  // Surgical DOM removal to avoid full re-render
  const card = document.querySelector(`[data-subquote-id="${sqid}"]`);
  if (card) {
    const bucketId = card.closest('[data-bucket-id]')?.dataset.bucketId;
    card.remove();
    if (bucketId) updateBucketCount(bucketId);
  }
  // Update transcript block colors
  renderTranscriptBody();
}

function showAddMenu(convId, anchor) {
  document.querySelectorAll('.atr-menu').forEach(m => m.remove());
  const menu = document.createElement('div');
  menu.className = 'atr-menu';
  state.buckets.filter(b => b.id !== 'other').forEach(b => {
    const item = document.createElement('div');
    item.className = 'atr-item';
    item.innerHTML = `<span class="atr-dot" style="background:${b.color_accent}"></span>${esc(b.name)}`;
    item.onclick = () => { addConvToReport(convId, b.id); menu.remove(); };
    menu.appendChild(item);
  });
  const other = document.createElement('div');
  other.className = 'atr-item';
  other.innerHTML = `<span class="atr-dot" style="background:#9E9E9E"></span>Other`;
  other.onclick = () => { addConvToReport(convId, 'other'); menu.remove(); };
  menu.appendChild(other);

  const rect = anchor.getBoundingClientRect();
  menu.style.top  = (rect.bottom + 4) + 'px';
  menu.style.left = Math.min(rect.left, window.innerWidth - 220) + 'px';
  document.body.appendChild(menu);
  setTimeout(() => {
    const close = e => { if (!menu.contains(e.target)) { menu.remove(); document.removeEventListener('mousedown', close); } };
    document.addEventListener('mousedown', close);
  }, 50);
}

function addConvToReport(convId, bucketId) {
  const bs = getBankState(state.current_bank);
  const bank = state.banks[state.current_bank];
  const conv = (bank.qa_conversations || []).find(c => c.id === convId);
  if (!conv) return;

  // Un-exclude answer sentences
  if (!bs.excluded_sentences) bs.excluded_sentences = [];
  bs.excluded_sentences = bs.excluded_sentences.filter(
    sid => !conv.answer_sentences.some(s => s.sid === sid)
  );

  // Override bucket
  const sqid = `SQ_${convId}`;
  if (!bs.subquote_bucket_overrides) bs.subquote_bucket_overrides = {};
  bs.subquote_bucket_overrides[sqid] = bucketId;

  renderReportSections();
  renderTranscriptBody();
}

// ============================================================
// BUCKET ACTIONS
// ============================================================
function toggleBucket(id) {
  const el   = document.getElementById(`bq_${id}`);
  const chev = document.getElementById(`bkv_${id}`);
  if (!el) return;
  const hidden = el.style.display === 'none';
  el.style.display = hidden ? '' : 'none';
  if (chev) chev.className = `bkt-chevron ${hidden ? 'open' : 'closed'}`;
}
function saveBucketTitle(id, val) {
  if (!state.bucket_user_titles) state.bucket_user_titles = {};
  state.bucket_user_titles[id] = val.trim();
}
function updateBucketCount(compositeId) {
  const el = document.getElementById(`bkc_${compositeId}`);
  if (!el) return;
  const parts = compositeId.match(/^(.+)__(\w+)$/);
  if (parts) {
    el.textContent = getReportSubquotes(state.current_bank, parts[1], parts[2]).length;
  } else {
    el.textContent = getReportSubquotes(state.current_bank, compositeId).length;
  }
}

// ============================================================
// STATE HELPERS
// ============================================================
function getBankState(bankId) {
  if (!state.bank_states) state.bank_states = {};
  if (!state.bank_states[bankId]) {
    state.bank_states[bankId] = {
      sentence_user_primary: {}, excluded_sentences: [],
      subquote_bucket_overrides: {}, bucket_subquote_order: {}, subquote_formats: {},
    };
  }
  return state.bank_states[bankId];
}

function findSentence(sid) {
  for (const bank of Object.values(state.banks)) {
    for (const blk of (bank.md_blocks || [])) {
      const s = blk.sentences.find(x => x.sid === sid);
      if (s) return s;
    }
    for (const conv of (bank.qa_conversations || [])) {
      const s = [...conv.question_sentences, ...conv.answer_sentences].find(x => x.sid === sid);
      if (s) return s;
    }
  }
  return null;
}

// ============================================================
// SAVE
// ============================================================
function saveReport() {
  const ts = new Date().toISOString().replace(/[:.]/g,'').slice(0,15);
  const stateJson = JSON.stringify(state, null, 2).replace(/<\/script>/gi, '<\\/script>');
  const newHtml = __HTML_TPL__.replace(
    /\/\* __BEGIN_STATE__ \*\/[\s\S]*?\/\* __END_STATE__ \*\//,
    `/* __BEGIN_STATE__ */\n${stateJson}\n/* __END_STATE__ */`
  );
  const blob = new Blob([newHtml], {type:'text/html;charset=utf-8'});
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href = url; a.download = `call_summary_${ts}.html`; a.click();
  URL.revokeObjectURL(url);
}

// ============================================================
// UTILITIES
// ============================================================
function esc(s) {
  if (s == null) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
                  .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
function escAttr(s) { return esc(s); }
</script>
</body>
</html>"""


# ============================================================
# HTML GENERATION
# ============================================================

def generate_html(state: Dict, fiscal_year: str, fiscal_quarter: str) -> str:
    min_imp = config.get("processing", {}).get("min_importance_score", MIN_IMPORTANCE_SCORE)
    state_json = json.dumps(state, ensure_ascii=False, indent=2)
    state_json_safe = state_json.replace("</script>", "<\\/script>")
    period = f"{fiscal_quarter} {fiscal_year}"

    html = HTML_TEMPLATE
    html = html.replace("__PERIOD__", period)
    html = html.replace("__MIN_IMPORTANCE__", str(min_imp))
    html = html.replace("__STATE_JSON__", state_json_safe)

    banner_b64 = load_banner_b64(DEFAULT_BANNER_PATH)
    if banner_b64 and '"banner_src": null' in html:
        html = html.replace('"banner_src": null', f'"banner_src": "{banner_b64}"')

    return html


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    global config, ssl_cert_path, oauth_token, llm_client

    parser = argparse.ArgumentParser(
        description="Generate an interactive Call Summary HTML from raw NAS earnings transcripts."
    )
    parser.add_argument("--year",      required=True, help="Fiscal year e.g. 2025")
    parser.add_argument("--quarter",   required=True, help="Fiscal quarter e.g. Q1")
    parser.add_argument("--banks",     default="",    help="Comma-separated tickers e.g. RY-CA,TD-CA")
    parser.add_argument("--categories-file", default=str(DEFAULT_CATEGORIES_FILE))
    parser.add_argument("--config",    default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--dev", action="store_true", help="Dev mode: limit processing")
    args = parser.parse_args()

    config = load_local_config(args.config)
    proc   = config.get("processing", {})
    min_imp      = float(proc.get("min_importance_score", MIN_IMPORTANCE_SCORE))
    dev_mode     = args.dev or proc.get("dev_mode", False)
    dev_max      = int(proc.get("dev_max_banks", 2)) if dev_mode else None
    dev_max_blk  = int(proc.get("dev_max_quotes_per_bank", 5)) if dev_mode else None

    validate_environment()
    setup_proxy()

    fiscal_year    = args.year
    fiscal_quarter = args.quarter.upper()
    out_dir        = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    log_info(f"Call Summary v2 — {fiscal_quarter} {fiscal_year} | dev={dev_mode}")

    categories     = load_categories(args.categories_file)
    institutions   = load_monitored_institutions()

    if args.banks:
        ticker_list = [t.strip() for t in args.banks.split(",") if t.strip()]
    else:
        ticker_list = [t for t, inst in institutions.items() if inst.get("type") == "Canadian_Banks"]
        log_info(f"No --banks specified; defaulting to Canadian Banks: {ticker_list}")

    resolved = resolve_bank_tickers(ticker_list, institutions)
    if not resolved:
        print("ERROR: No valid banks resolved. Check --banks argument.")
        sys.exit(1)
    if dev_max:
        resolved = resolved[:dev_max]

    log_info("Connecting to NAS...")
    conn = get_nas_connection()
    log_info("NAS connected")
    setup_ssl_certificate(conn)

    log_info("Acquiring OAuth token...")
    token = get_oauth_token()
    if not token:
        print("ERROR: OAuth token acquisition failed.")
        sys.exit(1)
    oauth_token = token
    llm_client  = setup_llm_client(token)
    log_info("LLM client ready")

    banks_data: Dict[str, Dict] = {}
    for inst in resolved:
        try:
            result = process_bank(
                conn, inst, fiscal_year, fiscal_quarter,
                categories, dev_max_blocks=dev_max_blk,
            )
            if result:
                banks_data[inst["ticker"]] = result
        except Exception as e:
            log_error(f"Failed to process {inst['ticker']}: {e}")
            traceback.print_exc()

    if not banks_data:
        print("ERROR: No banks processed successfully.")
        sys.exit(1)

    log_info(f"Processed {len(banks_data)} banks. LLM cost: ${total_llm_cost:.4f}")

    log_info("Building report state...")
    report_state = build_report_state(banks_data, categories, fiscal_year, fiscal_quarter, min_imp)

    log_info("Generating HTML...")
    html_content = generate_html(report_state, fiscal_year, fiscal_quarter)

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"call_summary_{fiscal_year}_{fiscal_quarter}_{ts}.html"
    out_path.write_text(html_content, encoding="utf-8")

    print(f"\n✓ Call Summary generated: {out_path}")
    print(f"  Banks processed : {list(banks_data.keys())}")
    print(f"  Total LLM cost  : ${total_llm_cost:.4f}")
    print(f"  File size       : {out_path.stat().st_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
