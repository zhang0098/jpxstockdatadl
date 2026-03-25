from __future__ import annotations

import json
import sys
import tempfile
import types
import unittest
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

edinet_xbrl_stub = types.ModuleType("edinet_xbrl")
setattr(edinet_xbrl_stub, "DocumentMetadata", object)
setattr(edinet_xbrl_stub, "EdinetApiError", RuntimeError)
setattr(edinet_xbrl_stub, "EdinetClient", object)
setattr(edinet_xbrl_stub, "ParsedFiling", object)
setattr(edinet_xbrl_stub, "XBRLParser", object)
sys.modules.setdefault("edinet_xbrl", edinet_xbrl_stub)

lxml_stub = types.ModuleType("lxml")
lxml_etree_stub = types.ModuleType("lxml.etree")
setattr(lxml_etree_stub, "XMLSyntaxError", ValueError)
setattr(lxml_stub, "etree", lxml_etree_stub)
sys.modules.setdefault("lxml", lxml_stub)
sys.modules.setdefault("lxml.etree", lxml_etree_stub)

import jpxstockdatadl.downloader as downloader_module
from jpxstockdatadl.downloader import MANIFEST_VERSION
from jpxstockdatadl.downloader import apply_precise_metric_overrides
from jpxstockdatadl.downloader import build_json_output_path
from jpxstockdatadl.downloader import cleanup_stale_json
from jpxstockdatadl.downloader import extract_precise_metric_value
from jpxstockdatadl.downloader import load_manifest
from jpxstockdatadl.downloader import resolve_recent_session_cache


def isoformat_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class DownloaderSessionCacheTests(unittest.TestCase):
    def test_extract_precise_metric_value_reads_fractional_xbrl_value(self) -> None:
        xbrl_text = (
            '<jpcrp_cor:PayoutRatioSummaryOfBusinessResults '
            'contextRef="CurrentYearDuration_NonConsolidatedMember" decimals="3" unitRef="pure">'
            '0.639'
            '</jpcrp_cor:PayoutRatioSummaryOfBusinessResults>'
        )

        downloader_module.load_field_element_ids_map.cache_clear()
        original_loader = downloader_module.load_field_element_ids_map
        downloader_module.load_field_element_ids_map = lambda: {
            "payout_ratio": ("jpcrp_cor:PayoutRatioSummaryOfBusinessResults",),
        }
        try:
            value = extract_precise_metric_value(
                xbrl_text,
                "payout_ratio",
                "CurrentYearDuration_NonConsolidatedMember",
            )
        finally:
            downloader_module.load_field_element_ids_map = original_loader

        self.assertEqual(value, 0.639)

    def test_apply_precise_metric_overrides_updates_fractional_metrics(self) -> None:
        payload = {
            "financials": [
                {
                    "field_name": "payout_ratio",
                    "value": 1,
                    "context_ref": "CurrentYearDuration_NonConsolidatedMember",
                    "decimals": 3,
                },
                {
                    "field_name": "pe_ratio",
                    "value": 13,
                    "context_ref": "CurrentYearDuration",
                    "decimals": 1,
                }
            ]
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            xbrl_path = Path(tmp_dir) / "sample.xbrl"
            xbrl_path.write_text(
                '<jpcrp_cor:PayoutRatioSummaryOfBusinessResults '
                'contextRef="CurrentYearDuration_NonConsolidatedMember" decimals="3" unitRef="pure">'
                '0.639'
                '</jpcrp_cor:PayoutRatioSummaryOfBusinessResults>'
                '<jpcrp_cor:PriceEarningsRatioSummaryOfBusinessResults '
                'contextRef="CurrentYearDuration" decimals="1" unitRef="pure">'
                '16.7'
                '</jpcrp_cor:PriceEarningsRatioSummaryOfBusinessResults>',
                encoding="utf-8",
            )

            downloader_module.load_field_element_ids_map.cache_clear()
            original_loader = downloader_module.load_field_element_ids_map
            downloader_module.load_field_element_ids_map = lambda: {
                "payout_ratio": ("jpcrp_cor:PayoutRatioSummaryOfBusinessResults",),
                "pe_ratio": ("jpcrp_cor:PriceEarningsRatioSummaryOfBusinessResults",),
            }
            try:
                apply_precise_metric_overrides(payload, xbrl_path)
            finally:
                downloader_module.load_field_element_ids_map = original_loader

        self.assertEqual(payload["financials"][0]["value"], 0.639)
        self.assertEqual(payload["financials"][1]["value"], 16.7)

    def test_build_json_output_path_uses_date_and_filing_type(self) -> None:
        parsed = types.SimpleNamespace(
            metadata=types.SimpleNamespace(
                period_end=None,
                filing_type="annual",
                doc_id="DOC123",
            ),
            raw_elements={"jpdei_cor:CurrentFiscalYearEndDateDEI": "2024-12-31"},
        )

        json_path = build_json_output_path(Path("/tmp/2024-12-31_annual_DOC123.xbrl"), parsed)

        self.assertEqual(json_path.name, "2024-12-31_annual.json")

    def test_cleanup_stale_json_removes_doc_id_named_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            original_xbrl_path = output_dir / "DOC123.xbrl"
            current_xbrl_path = output_dir / "2024-12-31_annual_DOC123.xbrl"
            stale_json_path = current_xbrl_path.with_suffix(".json")
            final_json_path = output_dir / "2024-12-31_annual.json"

            stale_json_path.write_text("{}\n", encoding="utf-8")
            final_json_path.write_text("{}\n", encoding="utf-8")

            cleanup_stale_json(original_xbrl_path, current_xbrl_path, final_json_path)

            self.assertFalse(stale_json_path.exists())
            self.assertTrue(final_json_path.exists())

    def test_load_manifest_preserves_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / ".manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "documents": {"doc-1": {"filename": "doc-1.xbrl"}},
                        "sessions": {"1234:5": {"recorded_at": "2026-03-01T00:00:00Z", "doc_ids": ["doc-1"]}},
                        "extra": {"keep": True},
                    }
                ),
                encoding="utf-8",
            )

            manifest = load_manifest(manifest_path)

        self.assertEqual(manifest["version"], MANIFEST_VERSION)
        self.assertIn("doc-1", manifest["documents"])
        self.assertIn("1234:5", manifest["sessions"])
        self.assertEqual(manifest["extra"], {"keep": True})

    def test_resolve_recent_session_cache_uses_fresh_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            xbrl_path = output_dir / "doc-1.xbrl"
            xbrl_path.write_text("xbrl", encoding="utf-8")
            now = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
            manifest = {
                "version": MANIFEST_VERSION,
                "documents": {
                    "doc-1": {
                        "filename": "doc-1.xbrl",
                        "submitted_at": "2026-03-01 09:00:00",
                        "description": "Annual Securities Report",
                        "doc_type_code": "120",
                    }
                },
                "sessions": {
                    "1234:5": {
                        "recorded_at": isoformat_z(now - timedelta(days=13)),
                        "doc_ids": ["doc-1"],
                    }
                },
            }

            cached_session = resolve_recent_session_cache(output_dir, manifest, "1234", 5, now=now)

        self.assertIsNotNone(cached_session)
        assert cached_session is not None
        filings, export_targets = cached_session
        self.assertEqual([filing.doc_id for filing in filings], ["doc-1"])
        self.assertEqual([target.xbrl_path.name for target in export_targets], ["doc-1.xbrl"])

    def test_resolve_recent_session_cache_rejects_expired_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            xbrl_path = output_dir / "doc-1.xbrl"
            xbrl_path.write_text("xbrl", encoding="utf-8")
            now = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
            manifest = {
                "version": MANIFEST_VERSION,
                "documents": {
                    "doc-1": {
                        "filename": "doc-1.xbrl",
                        "submitted_at": "2026-03-01 09:00:00",
                        "description": "Annual Securities Report",
                        "doc_type_code": "120",
                    }
                },
                "sessions": {
                    "1234:5": {
                        "recorded_at": isoformat_z(now - timedelta(days=15)),
                        "doc_ids": ["doc-1"],
                    }
                },
            }

            cached_session = resolve_recent_session_cache(output_dir, manifest, "1234", 5, now=now)

        self.assertIsNone(cached_session)
        self.assertNotIn("1234:5", manifest["sessions"])

    def test_resolve_recent_session_cache_rejects_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            now = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
            manifest = {
                "version": MANIFEST_VERSION,
                "documents": {
                    "doc-1": {
                        "filename": "doc-1.xbrl",
                        "submitted_at": "2026-03-01 09:00:00",
                        "description": "Annual Securities Report",
                        "doc_type_code": "120",
                    }
                },
                "sessions": {
                    "1234:5": {
                        "recorded_at": isoformat_z(now - timedelta(days=1)),
                        "doc_ids": ["doc-1"],
                    }
                },
            }

            cached_session = resolve_recent_session_cache(output_dir, manifest, "1234", 5, now=now)

        self.assertIsNone(cached_session)
        self.assertNotIn("1234:5", manifest["sessions"])


if __name__ == "__main__":
    unittest.main()
