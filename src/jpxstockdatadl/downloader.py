from __future__ import annotations

import asyncio
import html
import importlib.resources
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, Iterable
from zipfile import BadZipFile, ZipFile

from edinet_xbrl import (
    DocumentMetadata,
    EdinetApiError,
    EdinetClient,
    ParsedFiling,
    XBRLParser,
)
from lxml.etree import XMLSyntaxError

from jpxstockdatadl.helper import pick_xbrl_member

ANNUAL_DOC_TYPE_CODE = "120"
SEMIANNUAL_DOC_TYPE_CODE = "160"
TARGET_DOC_TYPE_CODES = frozenset({ANNUAL_DOC_TYPE_CODE, SEMIANNUAL_DOC_TYPE_CODE})
MANIFEST_FILE_NAME = ".manifest.json"
FINANCE_OVERVIEW_MARKDOWN_FILE_NAME = "finance_overview.md"
LEGACY_FINANCIALS_MARKDOWN_FILE_NAME = "financials.md"
DEFAULT_API_KEY_ENV = "EDINET_API_KEY"
INVALID_FILENAME_CHARS_RE = re.compile(r'[\\/:*?"<>|\r\n]+')
WHITESPACE_RE = re.compile(r"\s+")
NUMERIC_TEXT_RE = re.compile(r"^\d+(?:\.\d+)?$")
HTML_TAG_RE = re.compile(r"<[^>]+>")
FILING_JSON_NAME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}_(?:annual|semiannual)\.json$")
LIST_BATCH_SIZE = 30
MANIFEST_VERSION = 2
SESSION_CACHE_TTL = timedelta(days=14)
FILING_TYPE_BY_DOC_TYPE = {
    "120": "annual",
    "160": "semiannual",
}
FILING_TYPE_BY_PERIOD_CODE = {
    "FY": "annual",
    "Q1": "quarterly",
    "Q2": "semiannual",
    "Q3": "quarterly",
    "HY": "semiannual",
}
PERIOD_END_KEYS = (
    "CurrentPeriodEndDateDEI",
    "CurrentQuarterEndDateDEI",
    "CurrentSemiAnnualAccountingPeriodEndDateDEI",
    "CurrentFiscalYearEndDateDEI",
)
LABEL_SUFFIXES = (
    " Summary Of Business Results",
    " Key Financial Data",
    " Cover Page",
    " Text Block",
    " DEI",
)
LABEL_SUFFIX_REPLACEMENTS = {
    "CAIFRS": "CurrentAssetsIFRS",
    "NCAIFRS": "NonCurrentAssetsIFRS",
    "CLIFRS": "CurrentLiabilitiesIFRS",
    "NCLIFRS": "NonCurrentLiabilitiesIFRS",
    "OpeCFIFRS": "OperatingCashFlowIFRS",
    "InvCFIFRS": "InvestingCashFlowIFRS",
    "FinCFIFRS": "FinancingCashFlowIFRS",
    "SSIFRS": "StatementOfChangesInEquityIFRS",
}
BUSINESS_OVERVIEW_SECTION_KEYWORDS = (
    "description of business",
    "management analysis of financial position operating results and cash flows",
    "research and development activities",
)
PERCENTAGE_FIELD_NAMES = frozenset({"equity_ratio", "roe", "payout_ratio"})
PLAIN_NUMBER_FIELD_NAMES = frozenset(
    {
        "bps",
        "eps",
        "diluted_eps",
        "dividends_per_share",
        "pe_ratio",
    }
)
COUNT_FIELD_NAMES = frozenset({"num_employees"})
PREFERRED_FINANCIAL_FIELD_ORDER = (
    "revenue",
    "operating_income",
    "ordinary_income",
    "net_income",
    "comprehensive_income",
    "total_assets",
    "total_liabilities",
    "net_assets",
    "cash_and_equivalents",
    "operating_cf",
    "investing_cf",
    "financing_cf",
    "capital_stock",
    "short_term_loans",
    "current_portion_lt_loans",
    "bonds_payable",
    "long_term_loans",
    "eps",
    "diluted_eps",
    "bps",
    "dividends_per_share",
    "equity_ratio",
    "roe",
    "payout_ratio",
    "pe_ratio",
    "num_employees",
)
SUMMARY_FINANCIAL_FIELDS = (
    ("revenue", "Revenue"),
    ("operating_income", "Operating income"),
    ("net_income", "Net income"),
    ("total_assets", "Total assets"),
    ("net_assets", "Net assets"),
    ("cash_and_equivalents", "Cash and equivalents"),
    ("eps", "EPS"),
    ("dividends_per_share", "Dividends per share"),
    ("equity_ratio", "Equity ratio"),
    ("roe", "ROE"),
)
@dataclass(frozen=True)
class FilingRecord:
    doc_id: str
    doc_type_code: str
    description: str
    submitted_at: str
    download_key: str | None = None


@dataclass(frozen=True)
class DownloadSummary:
    stock_code: str
    output_dir: Path
    matched_documents: int
    downloaded_documents: int
    skipped_documents: int
    exported_json_documents: int
    failures: tuple[str, ...]


@dataclass(frozen=True)
class ExportTarget:
    filing: FilingRecord
    xbrl_path: Path


@dataclass(frozen=True)
class ExportedJsonArtifact:
    json_path: Path
    payload: dict[str, Any]
    text_sections: dict[str, str]


def normalize_stock_code(stock_code: str) -> str:
    digits = "".join(ch for ch in stock_code if ch.isdigit())
    if not digits:
        raise ValueError("stock_code must contain digits")
    if len(digits) == 5 and digits.endswith("0"):
        digits = digits[:4]
    if len(digits) != 4:
        raise ValueError(
            "stock_code must be a 4-digit code or a 5-digit EDINET secCode ending with 0"
        )
    return digits


def resolve_api_key(api_key: str | None = None) -> str:
    value = api_key or os.getenv(DEFAULT_API_KEY_ENV, "")
    if not value:
        raise ValueError(
            f"Missing EDINET API key. Set {DEFAULT_API_KEY_ENV} in your environment."
        )
    return value


def build_output_dir(stock_code: str) -> Path:
    return Path.home() / ".finreport" / "jp" / stock_code


def build_date_window(years: int) -> tuple[date, date]:
    end_date = date.today()
    start_date = subtract_years(end_date, years)
    return start_date, end_date


def prepare_download_context(stock_code: str) -> tuple[str, Path, Path, dict[str, Any]]:
    normalized_stock_code = normalize_stock_code(stock_code)
    output_dir = build_output_dir(normalized_stock_code)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / MANIFEST_FILE_NAME
    manifest = load_manifest(manifest_path)
    return normalized_stock_code, output_dir, manifest_path, manifest


def build_session_cache_key(stock_code: str, years: int) -> str:
    return f"{stock_code}:{years}"


def current_manifest_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_manifest_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None

    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_session_cache_record(
    stock_code: str, years: int, doc_ids: Iterable[str]
) -> dict[str, Any]:
    return {
        "stock_code": stock_code,
        "years": years,
        "recorded_at": current_manifest_timestamp(),
        "doc_ids": list(doc_ids),
    }


def build_filing_from_manifest_record(
    doc_id: str, record: dict[str, Any]
) -> FilingRecord | None:
    filename = record.get("filename")
    submitted_at = record.get("submitted_at")
    description = record.get("description")
    doc_type_code = record.get("doc_type_code")
    download_key = record.get("download_key")

    if not all(
        isinstance(value, str) and value
        for value in (filename, submitted_at, description, doc_type_code)
    ):
        return None
    if download_key is not None and not isinstance(download_key, str):
        return None

    assert isinstance(submitted_at, str)
    assert isinstance(description, str)
    assert isinstance(doc_type_code, str)

    return FilingRecord(
        doc_id=doc_id,
        doc_type_code=doc_type_code,
        description=description,
        submitted_at=submitted_at,
        download_key=download_key,
    )


def resolve_recent_session_cache(
    output_dir: Path,
    manifest: dict[str, Any],
    stock_code: str,
    years: int,
    now: datetime | None = None,
) -> tuple[list[FilingRecord], list[ExportTarget]] | None:
    session_key = build_session_cache_key(stock_code, years)
    session_record = manifest.get("sessions", {}).get(session_key)
    if not isinstance(session_record, dict):
        return None

    recorded_at = parse_manifest_timestamp(session_record.get("recorded_at"))
    if recorded_at is None:
        manifest.get("sessions", {}).pop(session_key, None)
        return None

    reference_time = now or datetime.now(timezone.utc)
    if reference_time.tzinfo is None:
        reference_time = reference_time.replace(tzinfo=timezone.utc)
    else:
        reference_time = reference_time.astimezone(timezone.utc)

    if reference_time - recorded_at > SESSION_CACHE_TTL:
        manifest.get("sessions", {}).pop(session_key, None)
        return None

    doc_ids = session_record.get("doc_ids")
    if not isinstance(doc_ids, list) or any(
        not isinstance(doc_id, str) or not doc_id for doc_id in doc_ids
    ):
        manifest.get("sessions", {}).pop(session_key, None)
        return None

    filings: list[FilingRecord] = []
    export_targets: list[ExportTarget] = []
    documents = manifest.get("documents", {})

    for doc_id in doc_ids:
        record = documents.get(doc_id)
        if not isinstance(record, dict):
            manifest.get("sessions", {}).pop(session_key, None)
            return None

        filing = build_filing_from_manifest_record(doc_id, record)
        if filing is None:
            manifest.get("sessions", {}).pop(session_key, None)
            return None

        cached_path = resolve_cached_path(output_dir, manifest, filing)
        if cached_path is None:
            manifest.get("sessions", {}).pop(session_key, None)
            return None

        filings.append(filing)
        export_targets.append(ExportTarget(filing=filing, xbrl_path=cached_path))

    return filings, export_targets


def update_session_cache(
    manifest: dict[str, Any],
    stock_code: str,
    years: int,
    filings: Iterable[FilingRecord],
) -> None:
    session_key = build_session_cache_key(stock_code, years)
    manifest.setdefault("sessions", {})[session_key] = build_session_cache_record(
        stock_code,
        years,
        (filing.doc_id for filing in filings),
    )


def resolve_cached_path(
    output_dir: Path, manifest: dict[str, Any], filing: FilingRecord
) -> Path | None:
    cached_record = manifest["documents"].get(filing.doc_id)
    if not isinstance(cached_record, dict):
        cached_record = find_manifest_record_by_download_key(
            manifest, filing.download_key
        )
    if not isinstance(cached_record, dict):
        return None

    cached_filename = cached_record.get("filename")
    if not isinstance(cached_filename, str) or not cached_filename:
        return None

    cached_path = output_dir / cached_filename
    return cached_path if cached_path.exists() else None


def find_manifest_record_by_download_key(
    manifest: dict[str, Any],
    download_key: str | None,
) -> dict[str, Any] | None:
    if not download_key:
        return None

    for record in manifest.get("documents", {}).values():
        if isinstance(record, dict) and record.get("download_key") == download_key:
            return record
    return None


def write_xbrl_artifact(
    output_dir: Path,
    manifest: dict[str, Any],
    filing: FilingRecord,
    xbrl_bytes: bytes,
    source_name: str,
) -> Path:
    desired_name = build_output_filename(filing, source_name)
    output_path = resolve_output_path(output_dir, desired_name, filing.doc_id)

    if output_path.exists():
        manifest["documents"][filing.doc_id] = build_manifest_record(
            filing, output_path.name
        )
        return output_path

    output_path.write_bytes(xbrl_bytes)
    manifest["documents"][filing.doc_id] = build_manifest_record(
        filing, output_path.name
    )
    return output_path


def finalize_download_summary(
    stock_code: str,
    output_dir: Path,
    manifest_path: Path,
    manifest: dict[str, Any],
    filings: Iterable[FilingRecord],
    downloaded: int,
    skipped: int,
    export_targets: list[ExportTarget],
    failures: list[str],
) -> DownloadSummary:
    exported_json_documents, exported_artifacts = export_json_files(
        export_targets, manifest, failures
    )
    export_financials_markdown(output_dir, failures)
    export_business_overview(exported_artifacts, failures)
    save_manifest(manifest_path, manifest)

    filing_list = list(filings)
    return DownloadSummary(
        stock_code=stock_code,
        output_dir=output_dir,
        matched_documents=len(filing_list),
        downloaded_documents=downloaded,
        skipped_documents=skipped,
        exported_json_documents=exported_json_documents,
        failures=tuple(failures),
    )


def download_stock_xbrl(
    stock_code: str,
    years: int,
    api_key: str | None = None,
) -> DownloadSummary:
    if years < 1:
        raise ValueError("years must be >= 1")

    return download_stock_xbrl_via_api(stock_code, years, api_key=api_key)


def download_stock_xbrl_via_api(
    stock_code: str,
    years: int,
    api_key: str | None = None,
) -> DownloadSummary:
    return asyncio.run(
        _download_stock_xbrl_via_api_async(stock_code, years, api_key=api_key)
    )


async def _download_stock_xbrl_via_api_async(
    stock_code: str,
    years: int,
    api_key: str | None = None,
) -> DownloadSummary:
    normalized_stock_code, output_dir, manifest_path, manifest = (
        prepare_download_context(stock_code)
    )
    cached_session = resolve_recent_session_cache(
        output_dir, manifest, normalized_stock_code, years
    )
    if cached_session is not None:
        filings, export_targets = cached_session
        for target in export_targets:
            print(f"Cached session {target.filing.doc_id} -> {target.xbrl_path.name}")
        return finalize_download_summary(
            stock_code=normalized_stock_code,
            output_dir=output_dir,
            manifest_path=manifest_path,
            manifest=manifest,
            filings=filings,
            downloaded=0,
            skipped=len(filings),
            export_targets=export_targets,
            failures=[],
        )

    resolved_api_key = resolve_api_key(api_key)

    start_date, end_date = build_date_window(years)
    print(
        f"Scanning EDINET API filings for {normalized_stock_code} from {start_date.isoformat()} to {end_date.isoformat()}..."
    )

    async with EdinetClient(api_key=resolved_api_key) as client:
        filings = await list_recent_filings(
            client=client,
            stock_code=normalized_stock_code,
            years=years,
        )
        downloaded = 0
        skipped = 0
        export_targets: list[ExportTarget] = []
        failures: list[str] = []

        for filing in filings:
            cached_path = resolve_cached_path(output_dir, manifest, filing)
            if cached_path is not None:
                skipped += 1
                export_targets.append(
                    ExportTarget(filing=filing, xbrl_path=cached_path)
                )
                print(f"Cached   {filing.doc_id} -> {cached_path.name}")
                continue

            try:
                archive_bytes = await client.download_document(filing.doc_id)
                xbrl_bytes, source_name = extract_xbrl_member(archive_bytes)
                output_path = write_xbrl_artifact(
                    output_dir, manifest, filing, xbrl_bytes, source_name
                )
                downloaded += 1
                export_targets.append(
                    ExportTarget(filing=filing, xbrl_path=output_path)
                )
                print(f"Saved    {filing.doc_id} -> {output_path.name}")
            except (EdinetApiError, OSError, ValueError, BadZipFile) as exc:
                failures.append(f"{filing.doc_id}: {exc}")
                print(f"Failed   {filing.doc_id}: {exc}")

    if len(export_targets) == len(filings):
        update_session_cache(manifest, normalized_stock_code, years, filings)

    return finalize_download_summary(
        stock_code=normalized_stock_code,
        output_dir=output_dir,
        manifest_path=manifest_path,
        manifest=manifest,
        filings=filings,
        downloaded=downloaded,
        skipped=skipped,
        export_targets=export_targets,
        failures=failures,
    )


async def list_recent_filings(
    client: EdinetClient,
    stock_code: str,
    years: int,
) -> list[FilingRecord]:
    end_date = date.today()
    start_date = subtract_years(end_date, years)
    filings_by_doc_id: dict[str, FilingRecord] = {}
    query_dates = list(iter_dates(start_date, end_date))
    total_dates = len(query_dates)

    for batch_start in range(0, total_dates, LIST_BATCH_SIZE):
        batch_dates = query_dates[batch_start : batch_start + LIST_BATCH_SIZE]
        responses = await asyncio.gather(
            *(client.list_documents(query_date) for query_date in batch_dates)
        )

        for response in responses:
            for entry in response.results:
                if not is_target_filing(entry, stock_code):
                    continue

                filings_by_doc_id[entry.doc_id] = FilingRecord(
                    doc_id=entry.doc_id,
                    doc_type_code=entry.doc_type_code or "",
                    description=(entry.doc_description or "").strip() or entry.doc_id,
                    submitted_at=format_submitted_at(
                        entry.submit_date_time, response.request_date
                    ),
                )

        scanned_dates = min(batch_start + len(batch_dates), total_dates)
        print(
            f"Scanned {scanned_dates}/{total_dates} days...",
            flush=True,
        )

    return select_target_filings(filings_by_doc_id.values())


def select_target_filings(filings: Iterable[FilingRecord]) -> list[FilingRecord]:
    annual_filings: list[FilingRecord] = []
    latest_semiannual: FilingRecord | None = None

    for filing in filings:
        if filing.doc_type_code == ANNUAL_DOC_TYPE_CODE:
            annual_filings.append(filing)
            continue

        if filing.doc_type_code != SEMIANNUAL_DOC_TYPE_CODE:
            continue

        if latest_semiannual is None or (filing.submitted_at, filing.doc_id) > (
            latest_semiannual.submitted_at,
            latest_semiannual.doc_id,
        ):
            latest_semiannual = filing

    selected_filings = sorted(
        annual_filings, key=lambda filing: (filing.submitted_at, filing.doc_id)
    )
    if latest_semiannual is not None:
        selected_filings.append(latest_semiannual)
        selected_filings.sort(key=lambda filing: (filing.submitted_at, filing.doc_id))

    return selected_filings


def is_target_filing(entry: DocumentMetadata, stock_code: str) -> bool:
    sec_code = (entry.sec_code or "").strip()
    if not sec_code:
        return False
    try:
        normalized_sec_code = normalize_stock_code(sec_code)
    except ValueError:
        return False
    if normalized_sec_code != stock_code:
        return False
    if (entry.doc_type_code or "") not in TARGET_DOC_TYPE_CODES:
        return False
    return entry.has_xbrl


def extract_xbrl_member(archive_bytes: bytes) -> tuple[bytes, str]:
    with ZipFile(BytesIO(archive_bytes)) as zip_file:
        member_name = pick_xbrl_member(zip_file.namelist())
        return zip_file.read(member_name), PurePosixPath(member_name).name


def build_output_filename(filing: FilingRecord, source_name: str) -> str:
    submission_stamp = sanitize_filename_component(
        build_submission_stamp(filing.submitted_at)
    )
    description = sanitize_filename_component(filing.description)
    source = sanitize_filename_component(source_name)
    return f"{submission_stamp}_{description}_{source}"


def build_submission_stamp(submitted_at: str) -> str:
    text = submitted_at.strip()
    text = text.replace("/", "-")
    text = text.replace(":", "")
    text = text.replace(" ", "_")
    if len(text) == 16:
        return f"{text}00"
    return text


def sanitize_filename_component(value: str) -> str:
    sanitized = INVALID_FILENAME_CHARS_RE.sub("_", value.strip())
    sanitized = WHITESPACE_RE.sub(" ", sanitized)
    sanitized = sanitized.strip(" .")
    return sanitized or "unknown"


def resolve_output_path(output_dir: Path, desired_name: str, doc_id: str) -> Path:
    candidate = output_dir / desired_name
    if not candidate.exists():
        return candidate

    source_path = Path(desired_name)
    return output_dir / f"{source_path.stem}_{doc_id}{source_path.suffix}"


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    if not manifest_path.exists():
        return {"version": MANIFEST_VERSION, "documents": {}, "sessions": {}}

    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"version": MANIFEST_VERSION, "documents": {}, "sessions": {}}

    if not isinstance(data, dict):
        return {"version": MANIFEST_VERSION, "documents": {}, "sessions": {}}

    documents = data.get("documents")
    if not isinstance(documents, dict):
        documents = {}

    sessions = data.get("sessions")
    if not isinstance(sessions, dict):
        sessions = {}

    manifest = dict(data)
    manifest["version"] = MANIFEST_VERSION
    manifest["documents"] = documents
    manifest["sessions"] = sessions
    return manifest


def save_manifest(manifest_path: Path, manifest: dict[str, Any]) -> None:
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build_manifest_record(filing: FilingRecord, filename: str) -> dict[str, str]:
    record = {
        "filename": filename,
        "submitted_at": filing.submitted_at,
        "description": filing.description,
        "doc_type_code": filing.doc_type_code,
    }
    if filing.download_key:
        record["download_key"] = filing.download_key
    return record


def subtract_years(target: date, years: int) -> date:
    try:
        return target.replace(year=target.year - years)
    except ValueError:
        return target.replace(month=2, day=28, year=target.year - years)


def iter_dates(start_date: date, end_date: date) -> list[date]:
    current = start_date
    dates: list[date] = []
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def format_submitted_at(submit_date_time: datetime | None, fallback_date: date) -> str:
    if submit_date_time is None:
        return fallback_date.isoformat()
    return submit_date_time.strftime("%Y-%m-%d %H:%M:%S")


def export_json_files(
    export_targets: list[ExportTarget],
    manifest: dict[str, Any],
    failures: list[str],
) -> tuple[int, list[ExportedJsonArtifact]]:
    parser = XBRLParser()
    exported = 0
    exported_artifacts: list[ExportedJsonArtifact] = []

    for target in export_targets:
        try:
            parsed = parser.parse(
                target.xbrl_path.read_bytes(), doc_id=target.filing.doc_id
            )
            text_sections = extract_text_sections(parsed.raw_elements)
            normalized = normalize_parsed_filing(parsed, target.filing)
            xbrl_path = normalize_xbrl_output_path(target.xbrl_path, normalized)
            manifest["documents"][target.filing.doc_id] = build_manifest_record(
                target.filing, xbrl_path.name
            )
            json_path = build_json_output_path(xbrl_path, normalized)
            payload = build_export_payload(normalized, xbrl_path)
            json_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            cleanup_stale_json(target.xbrl_path, xbrl_path, json_path)
            exported += 1
            exported_artifacts.append(
                ExportedJsonArtifact(
                    json_path=json_path,
                    payload=payload,
                    text_sections=text_sections,
                )
            )
            print(f"Exported {target.filing.doc_id} -> {json_path.name}")
        except (OSError, ValueError, XMLSyntaxError) as exc:
            failures.append(f"{target.filing.doc_id} JSON export: {exc}")
            print(f"Failed   {target.filing.doc_id} JSON export: {exc}")

    return exported, exported_artifacts


def build_export_payload(parsed: ParsedFiling, xbrl_path: Path) -> dict[str, Any]:
    payload = json.loads(parsed.model_dump_json(indent=2))
    apply_precise_metric_overrides(payload, xbrl_path)
    return payload


def apply_precise_metric_overrides(payload: dict[str, Any], xbrl_path: Path) -> None:
    financials = payload.get("financials")
    if not isinstance(financials, list):
        return

    xbrl_text: str | None = None
    for item in financials:
        if not isinstance(item, dict):
            continue
        field_name = item.get("field_name")
        if not isinstance(field_name, str) or not field_name:
            continue
        context_ref = item.get("context_ref")
        if not isinstance(context_ref, str) or not context_ref:
            continue
        decimals = item.get("decimals")
        if not isinstance(decimals, int) or decimals < 0:
            continue
        if xbrl_text is None:
            xbrl_text = xbrl_path.read_text(encoding="utf-8")
        precise_value = extract_precise_metric_value(xbrl_text, field_name, context_ref)
        if precise_value is not None:
            item["value"] = precise_value


def extract_precise_metric_value(
    xbrl_text: str, field_name: str, context_ref: str
) -> int | float | None:
    for element_id in load_field_element_ids_map().get(field_name, ()):
        precise_value = extract_precise_element_value(
            xbrl_text, element_id, context_ref
        )
        if precise_value is not None:
            return precise_value
    return None


def extract_precise_element_value(
    xbrl_text: str, element_id: str, context_ref: str
) -> int | float | None:
    local_name = element_id.split(":", 1)[-1]
    pattern = re.compile(
        rf'<[^>]*:{re.escape(local_name)}\b[^>]*contextRef="{re.escape(context_ref)}"[^>]*>([^<]+)</[^>]*:{re.escape(local_name)}>',
        flags=re.IGNORECASE,
    )
    match = pattern.search(xbrl_text)
    if match is None:
        return None

    return parse_precise_numeric_text(match.group(1))


def parse_precise_numeric_text(value_text: str) -> int | float | None:
    normalized = value_text.strip().replace(",", "")
    if not normalized:
        return None
    try:
        if any(marker in normalized for marker in (".", "e", "E")):
            return float(normalized)
        return int(normalized)
    except ValueError:
        return None


def export_financials_markdown(output_dir: Path, failures: list[str]) -> None:
    try:
        json_paths = list_financial_json_paths(output_dir)
        if not json_paths:
            return

        records = [load_financial_markdown_record(json_path) for json_path in json_paths]
        output_path = output_dir / FINANCE_OVERVIEW_MARKDOWN_FILE_NAME
        output_path.write_text(
            render_financials_markdown(records),
            encoding="utf-8",
        )
        cleanup_legacy_financials_markdown(output_dir, output_path)
        print(f"Exported financials -> {output_path.name}")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        failures.append(f"financials markdown export: {exc}")
        print(f"Failed   financials markdown export: {exc}")


def list_financial_json_paths(output_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in output_dir.glob("*.json")
        if FILING_JSON_NAME_RE.fullmatch(path.name)
    )


def load_financial_markdown_record(json_path: Path) -> dict[str, Any]:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    metadata = data.get("metadata")
    company = data.get("company")
    financials = data.get("financials")

    if not isinstance(metadata, dict):
        raise ValueError(f"Invalid metadata in {json_path.name}")
    if not isinstance(company, dict):
        raise ValueError(f"Invalid company in {json_path.name}")
    if not isinstance(financials, list):
        raise ValueError(f"Invalid financials in {json_path.name}")

    row: dict[str, Any] = {}
    currencies: set[str] = set()
    for item in financials:
        if not isinstance(item, dict):
            continue
        field_name = item.get("field_name")
        if not isinstance(field_name, str) or not field_name:
            continue
        row[field_name] = item.get("value")
        currency = item.get("currency")
        if isinstance(currency, str) and currency:
            currencies.add(currency)

    return {
        "company": company,
        "metadata": metadata,
        "row": row,
        "currency": resolve_financial_record_currency(currencies),
    }


def resolve_financial_record_currency(currencies: set[str]) -> str:
    if not currencies:
        return ""
    if len(currencies) == 1:
        return next(iter(currencies))
    return "/".join(sorted(currencies))


def cleanup_legacy_financials_markdown(output_dir: Path, current_output_path: Path) -> None:
    legacy_path = output_dir / LEGACY_FINANCIALS_MARKDOWN_FILE_NAME
    if legacy_path == current_output_path or not legacy_path.exists():
        return
    legacy_path.unlink()


def render_financials_markdown(records: list[dict[str, Any]]) -> str:
    if not records:
        raise ValueError("No financial records found")

    sorted_records = sorted(records, key=financial_markdown_sort_key)
    company = sorted_records[-1]["company"]
    metadata_columns = [
        "period_end",
        "period_start",
        "fiscal_year",
        "filing_type",
        "accounting_standard",
    ]
    financial_columns = collect_financial_columns(sorted_records)
    columns = metadata_columns + financial_columns

    lines = ["# Finance Overview", ""]
    company_name = company.get("name_jp") or company.get("name_en") or "Unknown Company"
    lines.append(f"- company: {company_name}")
    if company.get("name_en"):
        lines.append(f"- company_en: {company['name_en']}")
    if company.get("securities_code"):
        lines.append(f"- securities_code: {company['securities_code']}")
    if company.get("edinet_code"):
        lines.append(f"- edinet_code: {company['edinet_code']}")
    lines.append("- units: monetary values use M/B/T; ratios use %")
    lines.append("")

    lines.extend(render_financials_summary(sorted_records))

    lines.append("| " + " | ".join(columns) + " |")
    lines.append("| " + " | ".join("---" for _ in columns) + " |")
    for record in sorted_records:
        metadata = record["metadata"]
        row = record["row"]
        line_values = [
            format_markdown_cell(metadata.get(column)) for column in metadata_columns
        ]
        line_values.extend(
            format_financial_value(column, row.get(column)) for column in financial_columns
        )
        lines.append("| " + " | ".join(line_values) + " |")

    return "\n".join(lines) + "\n"


def render_financials_summary(records: list[dict[str, Any]]) -> list[str]:
    latest_record = records[-1]
    latest_metadata = latest_record["metadata"]
    latest_row = latest_record["row"]
    latest_currency = str(latest_record.get("currency") or "")
    comparable_record = find_previous_comparable_record(records, latest_record)

    filing_label = str(latest_metadata.get("filing_type") or "filing")
    period_end = str(latest_metadata.get("period_end") or "")
    fiscal_year = latest_metadata.get("fiscal_year")

    lines = ["## Key Metrics", ""]
    latest_line = f"- latest filing: {period_end} {filing_label}"
    if fiscal_year is not None:
        latest_line += f" (FY{fiscal_year})"
    lines.append(latest_line)

    if comparable_record is not None:
        comparable_metadata = comparable_record["metadata"]
        comparable_period_end = str(comparable_metadata.get("period_end") or "")
        comparable_filing_type = str(comparable_metadata.get("filing_type") or filing_label)
        lines.append(
            f"- comparable filing: {comparable_period_end} {comparable_filing_type}"
        )
    if latest_currency:
        lines.append(f"- currency: {latest_currency}")
    lines.append("")

    for field_name, label in SUMMARY_FINANCIAL_FIELDS:
        if field_name not in latest_row:
            continue
        current_value = latest_row.get(field_name)
        if current_value is None:
            continue
        comparison = ""
        if comparable_record is not None:
            previous_value = comparable_record["row"].get(field_name)
            comparison = build_summary_comparison(field_name, current_value, previous_value)
        metric_text = format_financial_value(field_name, current_value)
        if comparison:
            lines.append(f"- {label}: {metric_text} ({comparison})")
        else:
            lines.append(f"- {label}: {metric_text}")

    lines.append("")
    return lines


def find_previous_comparable_record(
    records: list[dict[str, Any]], latest_record: dict[str, Any]
) -> dict[str, Any] | None:
    latest_metadata = latest_record["metadata"]
    latest_filing_type = latest_metadata.get("filing_type")
    for record in reversed(records[:-1]):
        if record["metadata"].get("filing_type") == latest_filing_type:
            return record
    return records[-2] if len(records) >= 2 else None


def build_summary_comparison(
    field_name: str, current_value: Any, previous_value: Any
) -> str:
    if previous_value is None:
        return ""

    current_number = coerce_numeric_value(current_value)
    previous_number = coerce_numeric_value(previous_value)
    if current_number is None or previous_number is None:
        return f"vs {format_financial_value(field_name, previous_value)}"

    previous_text = format_financial_value(field_name, previous_value)
    if field_name in PERCENTAGE_FIELD_NAMES:
        delta_points = (current_number - previous_number) * 100
        return f"vs {previous_text}, {format_signed_decimal(delta_points)}pp"

    if previous_number == 0:
        delta_value = current_number - previous_number
        return f"vs {previous_text}, delta {format_signed_financial_value(field_name, delta_value)}"

    change_ratio = ((current_number / previous_number) - 1) * 100
    return f"vs {previous_text}, {format_signed_decimal(change_ratio)}%"


def format_signed_financial_value(field_name: str, value: Any) -> str:
    text = format_financial_value(field_name, abs(value))
    if not text:
        return ""
    number = coerce_numeric_value(value)
    if number is not None and number < 0:
        return f"-{text}"
    return f"+{text}"


def format_signed_decimal(value: int | float) -> str:
    if value > 0:
        return f"+{value:.1f}".rstrip("0").rstrip(".")
    if value < 0:
        return f"{value:.1f}".rstrip("0").rstrip(".")
    return "0"


def financial_markdown_sort_key(record: dict[str, Any]) -> tuple[str, str, str]:
    metadata = record["metadata"]
    period_end = metadata.get("period_end")
    filing_type = metadata.get("filing_type")
    doc_id = metadata.get("doc_id")
    return (
        str(period_end or ""),
        str(filing_type or ""),
        str(doc_id or ""),
    )


def collect_financial_columns(records: list[dict[str, Any]]) -> list[str]:
    discovered = {
        field_name
        for record in records
        for field_name in record["row"].keys()
    }
    ordered = [
        field_name
        for field_name in PREFERRED_FINANCIAL_FIELD_ORDER
        if field_name in discovered
    ]
    extras = sorted(discovered.difference(PREFERRED_FINANCIAL_FIELD_ORDER))
    return ordered + extras


def format_financial_value(field_name: str, value: Any) -> str:
    if value is None:
        return ""
    if field_name in PERCENTAGE_FIELD_NAMES:
        return format_percentage_value(value)
    if field_name in PLAIN_NUMBER_FIELD_NAMES:
        return format_decimal_value(value)
    if field_name in COUNT_FIELD_NAMES:
        return format_count_value(value)
    return format_magnitude_value(value)


def format_percentage_value(value: Any) -> str:
    number = coerce_numeric_value(value)
    if number is None:
        return format_markdown_cell(value)
    return f"{number * 100:.1f}%"


def format_decimal_value(value: Any) -> str:
    number = coerce_numeric_value(value)
    if number is None:
        return format_markdown_cell(value)
    if float(number).is_integer():
        return str(int(number))
    return f"{number:.2f}".rstrip("0").rstrip(".")


def format_count_value(value: Any) -> str:
    number = coerce_numeric_value(value)
    if number is None:
        return format_markdown_cell(value)
    if float(number).is_integer():
        return f"{int(number):,}"
    return f"{number:,.2f}".rstrip("0").rstrip(".")


def format_magnitude_value(value: Any) -> str:
    number = coerce_numeric_value(value)
    if number is None:
        return format_markdown_cell(value)

    absolute = abs(number)
    if absolute >= 1_000_000_000_000:
        scaled, suffix = number / 1_000_000_000_000, "T"
    elif absolute >= 1_000_000_000:
        scaled, suffix = number / 1_000_000_000, "B"
    elif absolute >= 1_000_000:
        scaled, suffix = number / 1_000_000, "M"
    else:
        if float(number).is_integer():
            return f"{int(number):,}"
        return f"{number:.2f}".rstrip("0").rstrip(".")

    return f"{scaled:.1f}".rstrip("0").rstrip(".") + suffix


def coerce_numeric_value(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    return None


def format_markdown_cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    return text.replace("|", r"\|")


def export_business_overview(
    exported_artifacts: list[ExportedJsonArtifact], failures: list[str]
) -> None:
    if not exported_artifacts:
        return

    latest_artifact = max(
        exported_artifacts, key=lambda artifact: artifact.json_path.stem
    )
    try:
        output_path = latest_artifact.json_path.with_name("latest_business_overview.md")
        output_path.write_text(
            render_business_overview_markdown(
                latest_artifact.payload, latest_artifact.text_sections
            ),
            encoding="utf-8",
        )
        print(f"Exported business overview -> {output_path.name}")
    except (OSError, ValueError) as exc:
        failures.append(f"business overview export: {exc}")
        print(f"Failed   business overview export: {exc}")


def render_business_overview_markdown(
    payload: dict[str, Any], text_sections: dict[str, str]
) -> str:
    raw_company = payload.get("company")
    raw_metadata = payload.get("metadata")
    company: dict[str, Any] = raw_company if isinstance(raw_company, dict) else {}
    metadata: dict[str, Any] = raw_metadata if isinstance(raw_metadata, dict) else {}
    company_name = str(
        company.get("name_jp") or company.get("name_en") or "Business Overview"
    )
    doc_id = str(metadata.get("doc_id") or "")
    period_end = str(metadata.get("period_end") or "")

    lines = [f"# {company_name} Business Overview", ""]
    if doc_id or period_end:
        lines.append(f"- doc_id: {doc_id}")
        lines.append(f"- period_end: {period_end}")
        lines.append("")

    selected_sections = text_sections

    if not selected_sections:
        lines.append("No text sections found.")
        return "\n".join(lines) + "\n"

    for title, body in selected_sections.items():
        lines.append(f"## {title}")
        lines.append("")
        lines.append(body)
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def select_business_overview_sections(text_sections: dict[str, str]) -> dict[str, str]:
    selected: dict[str, str] = {}
    for title, body in text_sections.items():
        normalized_title = title.strip().lower()
        if any(
            keyword in normalized_title
            for keyword in BUSINESS_OVERVIEW_SECTION_KEYWORDS
        ):
            selected[title] = body
    return selected


def extract_text_sections(raw_elements: dict[str, str]) -> dict[str, str]:
    sections: dict[str, str] = {}
    for element_id, value in raw_elements.items():
        if not element_id.endswith("TextBlock"):
            continue
        text = clean_text_block(value)
        if not text:
            continue
        title = humanize_element_id(element_id)
        key = build_unique_label(sections, title, element_id)
        sections[key] = text
    return sections


def clean_text_block(value: str) -> str:
    text = html.unescape(value)
    text = text.replace("\r", "")
    text = re.sub(r"<(?:br|br/|br\s*/)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(
        r"</(?:p|div|h1|h2|h3|h4|h5|h6|li|tr|table)>", "\n\n", text, flags=re.IGNORECASE
    )
    text = HTML_TAG_RE.sub("", text)
    text = text.replace("\u00a0", " ")
    lines = [" ".join(line.split()) for line in text.splitlines()]
    cleaned_lines = [line for line in lines if line]
    return "\n\n".join(cleaned_lines)


def build_json_output_path(xbrl_path: Path, parsed: ParsedFiling) -> Path:
    return xbrl_path.with_name(f"{build_json_export_stem(parsed)}.json")


def normalize_xbrl_output_path(xbrl_path: Path, parsed: ParsedFiling) -> Path:
    target_path = xbrl_path.with_name(f"{build_export_stem(parsed)}.xbrl")
    if target_path == xbrl_path:
        return xbrl_path

    xbrl_path.replace(target_path)
    print(f"Renamed  {parsed.metadata.doc_id} -> {target_path.name}")
    return target_path


def build_export_stem(parsed: ParsedFiling) -> str:
    period_end = resolve_period_end(parsed)
    filing_type = sanitize_filename_component(parsed.metadata.filing_type or "filing")
    doc_id = sanitize_filename_component(parsed.metadata.doc_id or "unknown-doc")
    return f"{period_end}_{filing_type}_{doc_id}"


def build_json_export_stem(parsed: ParsedFiling) -> str:
    period_end = resolve_period_end(parsed)
    filing_type = sanitize_filename_component(parsed.metadata.filing_type or "filing")
    return f"{period_end}_{filing_type}"


def cleanup_stale_json(
    original_xbrl_path: Path, current_xbrl_path: Path, json_path: Path
) -> None:
    stale_json_paths = {
        original_xbrl_path.with_suffix(".json"),
        current_xbrl_path.with_suffix(".json"),
    }
    for stale_json_path in stale_json_paths:
        if stale_json_path == json_path or not stale_json_path.exists():
            continue
        stale_json_path.unlink()


def normalize_parsed_filing(parsed: ParsedFiling, filing: FilingRecord) -> ParsedFiling:
    period_end = normalize_period_end(parsed)
    filing_type = normalize_filing_type(parsed, filing)
    metadata = parsed.metadata.model_copy(
        update={
            "period_end": period_end,
            "filing_type": filing_type,
        }
    )
    raw_elements = prepare_raw_elements_for_export(parsed.raw_elements)
    return parsed.model_copy(
        update={"metadata": metadata, "raw_elements": raw_elements}
    )


def prepare_raw_elements_for_export(raw_elements: dict[str, str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for element_id, value in raw_elements.items():
        stripped_value = value.strip()
        if is_numeric_text(stripped_value):
            label = humanize_element_id(element_id)
            key = build_unique_label(result, label, element_id)
            result[key] = stripped_value
    return result


def is_numeric_text(value: str) -> bool:
    text = value.strip()
    if not text or "<" in text or ">" in text:
        return False

    if text in {"-", "－", "―", "‐", "–", "—", "△", "…"}:
        return False

    if text.startswith(("△", "▲", "-", "－", "−")):
        text = text[1:]
    elif text.startswith("(") and text.endswith(")"):
        text = text[1:-1]

    text = (
        text.replace(",", "").replace("，", "").replace(" ", "").replace("\u3000", "")
    )
    return bool(NUMERIC_TEXT_RE.fullmatch(text))


@lru_cache(maxsize=1)
def load_field_element_ids_map() -> dict[str, tuple[str, ...]]:
    taxonomy_path = importlib.resources.files("edinet_xbrl").joinpath("taxonomy.json")
    with taxonomy_path.open("r", encoding="utf-8") as handle:
        fields_map = json.load(handle).get("reverse_map", {})

    field_element_ids: dict[str, tuple[str, ...]] = {}
    for field_name, entries in fields_map.items():
        if not isinstance(field_name, str) or not isinstance(entries, list):
            continue
        element_id_list: list[str] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            element_id = entry.get("element_id")
            if isinstance(element_id, str):
                element_id_list.append(element_id)
        if element_id_list:
            field_element_ids[field_name] = tuple(element_id_list)
    return field_element_ids


@lru_cache(maxsize=1)
def load_label_map() -> dict[str, str]:
    taxonomy_path = importlib.resources.files("edinet_xbrl").joinpath("taxonomy.json")
    with taxonomy_path.open("r", encoding="utf-8") as handle:
        field_map = json.load(handle)["field_map"]

    return {
        element_id: info["label_en"]
        for element_id, info in field_map.items()
        if info.get("label_en")
    }


def humanize_element_id(element_id: str) -> str:
    label_map = load_label_map()
    if element_id in label_map:
        return cleanup_label(label_map[element_id])

    local_name = element_id.split(":", 1)[-1]
    for token, replacement in sorted(
        LABEL_SUFFIX_REPLACEMENTS.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        if local_name.endswith(token):
            local_name = local_name[: -len(token)] + replacement
            break

    humanized = local_name.replace("_", " ")
    humanized = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", humanized)
    humanized = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", humanized)
    return cleanup_label(humanized or element_id)


def cleanup_label(label: str) -> str:
    cleaned = " ".join(label.split())
    for suffix in LABEL_SUFFIXES:
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)].rstrip()
    cleaned = cleaned.replace(" Of ", " of ")
    cleaned = cleaned.replace(" And ", " and ")
    return cleaned


def build_unique_label(existing: dict[str, str], label: str, element_id: str) -> str:
    if label not in existing:
        return label
    return f"{label} ({element_id})"


def normalize_period_end(parsed: ParsedFiling) -> date | None:
    text = resolve_period_end_text(parsed)
    if text is None:
        return parsed.metadata.period_end

    try:
        return date.fromisoformat(text)
    except ValueError:
        return parsed.metadata.period_end


def normalize_filing_type(parsed: ParsedFiling, filing: FilingRecord) -> str:
    period_code = lookup_raw_element(parsed, "TypeOfCurrentPeriodDEI")
    if period_code:
        normalized_period_code = period_code.strip().upper()
        if normalized_period_code in FILING_TYPE_BY_PERIOD_CODE:
            return FILING_TYPE_BY_PERIOD_CODE[normalized_period_code]

    filing_type = FILING_TYPE_BY_DOC_TYPE.get(filing.doc_type_code)
    if filing_type:
        return filing_type
    return parsed.metadata.filing_type or "filing"


def resolve_period_end(parsed: ParsedFiling) -> str:
    period_end_text = resolve_period_end_text(parsed)
    if period_end_text:
        return sanitize_date_text(period_end_text)

    if parsed.metadata.period_end is not None:
        return parsed.metadata.period_end.isoformat()

    return "unknown-date"


def resolve_period_end_text(parsed: ParsedFiling) -> str | None:
    for key in PERIOD_END_KEYS:
        value = lookup_raw_element(parsed, key)
        if value:
            return value
    return None


def lookup_raw_element(parsed: ParsedFiling, local_name: str) -> str | None:
    for element_id, value in parsed.raw_elements.items():
        if element_id.endswith(local_name) and value.strip():
            return value.strip()
    return None


def sanitize_date_text(value: str) -> str:
    text = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return sanitize_filename_component(text)
