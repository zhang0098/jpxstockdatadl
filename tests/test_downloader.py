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
from jpxstockdatadl.downloader import export_financials_markdown
from jpxstockdatadl.downloader import finalize_download_summary
from jpxstockdatadl.downloader import load_manifest
from jpxstockdatadl.downloader import render_business_overview_markdown
from jpxstockdatadl.downloader import resolve_recent_session_cache
from jpxstockdatadl.downloader import select_business_overview_sections


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

    def test_export_financials_markdown_builds_sorted_table_and_formats_values(self) -> None:
        annual_payload = {
            "company": {
                "edinet_code": "E01377",
                "name_jp": "オーエスジー株式会社",
                "name_en": "OSG Corporation",
                "securities_code": "6136",
            },
            "metadata": {
                "doc_id": "DOC-ANNUAL",
                "period_start": "2024-12-01",
                "period_end": "2025-11-30",
                "fiscal_year": 2025,
                "accounting_standard": "JP-GAAP",
                "filing_type": "annual",
                "filed_at": None,
            },
            "financials": [
                {"field_name": "revenue", "value": 160_619_000_000, "currency": "JPY"},
                {"field_name": "net_income", "value": 14_334_000_000, "currency": "JPY"},
                {"field_name": "eps", "value": 172.11, "currency": "JPY"},
                {"field_name": "equity_ratio", "value": 0.675, "currency": "JPY"},
                {"field_name": "num_employees", "value": 7563, "currency": "JPY"},
            ],
        }
        semiannual_payload = {
            "company": {
                "edinet_code": "E01377",
                "name_jp": "オーエスジー株式会社",
                "name_en": "OSG Corporation",
                "securities_code": "6136",
            },
            "metadata": {
                "doc_id": "DOC-SEMI",
                "period_start": "2024-12-01",
                "period_end": "2025-05-31",
                "fiscal_year": 2025,
                "accounting_standard": "JP-GAAP",
                "filing_type": "semiannual",
                "filed_at": None,
            },
            "financials": [
                {"field_name": "revenue", "value": 80_100_000_000, "currency": "JPY"},
                {"field_name": "net_income", "value": 7_500_000_000, "currency": "JPY"},
                {"field_name": "eps", "value": 91.2, "currency": "JPY"},
                {"field_name": "equity_ratio", "value": 0.641, "currency": "JPY"},
                {"field_name": "num_employees", "value": 7500, "currency": "JPY"},
            ],
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            (output_dir / "2025-11-30_annual.json").write_text(
                json.dumps(annual_payload),
                encoding="utf-8",
            )
            (output_dir / "2025-05-31_semiannual.json").write_text(
                json.dumps(semiannual_payload),
                encoding="utf-8",
            )
            (output_dir / "quote.json").write_text("{}", encoding="utf-8")
            (output_dir / "financials.md").write_text("legacy\n", encoding="utf-8")

            failures: list[str] = []
            export_financials_markdown(output_dir, failures)

            markdown = (output_dir / "finance_overview.md").read_text(encoding="utf-8")

        self.assertEqual(failures, [])
        self.assertFalse((output_dir / "financials.md").exists())
        self.assertIn("- company: オーエスジー株式会社", markdown)
        self.assertIn("- securities_code: 6136", markdown)
        self.assertIn("- units: monetary values use M/B/T; ratios use %", markdown)
        self.assertIn("## Key Metrics", markdown)
        self.assertIn("- latest filing: 2025-11-30 annual (FY2025)", markdown)
        self.assertIn("- comparable filing: 2025-05-31 semiannual", markdown)
        self.assertIn("- currency: JPY", markdown)
        self.assertIn("- Revenue: 160.6B (vs 80.1B, +100.5%)", markdown)
        self.assertIn("- Net income: 14.3B (vs 7.5B, +91.1%)", markdown)
        self.assertIn("- EPS: 172.11 (vs 91.2, +88.7%)", markdown)
        self.assertIn("- Equity ratio: 67.5% (vs 64.1%, +3.4pp)", markdown)
        self.assertIn("| period_end | period_start | fiscal_year | filing_type | accounting_standard | revenue | net_income | eps | equity_ratio | num_employees |", markdown)
        semiannual_row = "| 2025-05-31 | 2024-12-01 | 2025 | semiannual | JP-GAAP | 80.1B | 7.5B | 91.2 | 64.1% | 7,500 |"
        annual_row = "| 2025-11-30 | 2024-12-01 | 2025 | annual | JP-GAAP | 160.6B | 14.3B | 172.11 | 67.5% | 7,563 |"
        self.assertIn(semiannual_row, markdown)
        self.assertIn(annual_row, markdown)
        self.assertLess(markdown.index(semiannual_row), markdown.index(annual_row))
        self.assertNotIn("| doc_id |", markdown)
        self.assertNotIn("DOC-SEMI", markdown)
        self.assertNotIn("DOC-ANNUAL", markdown)
        self.assertNotIn("quote.json", markdown)

    def test_finalize_download_summary_exports_financials_before_business_overview(self) -> None:
        call_order: list[str] = []
        original_export_json_files = downloader_module.export_json_files
        original_export_financials_markdown = downloader_module.export_financials_markdown
        original_export_business_overview = downloader_module.export_business_overview
        original_save_manifest = downloader_module.save_manifest

        def fake_export_json_files(*args: object, **kwargs: object) -> tuple[int, list[object]]:
            call_order.append("json")
            return 1, []

        def fake_export_financials_markdown(*args: object, **kwargs: object) -> None:
            call_order.append("financials")

        def fake_export_business_overview(*args: object, **kwargs: object) -> None:
            call_order.append("business")

        def fake_save_manifest(*args: object, **kwargs: object) -> None:
            call_order.append("manifest")

        downloader_module.export_json_files = fake_export_json_files
        downloader_module.export_financials_markdown = fake_export_financials_markdown
        downloader_module.export_business_overview = fake_export_business_overview
        downloader_module.save_manifest = fake_save_manifest

        try:
            summary = finalize_download_summary(
                stock_code="6136",
                output_dir=Path("/tmp/6136"),
                manifest_path=Path("/tmp/6136/.manifest.json"),
                manifest={"documents": {}, "sessions": {}, "version": MANIFEST_VERSION},
                filings=[],
                downloaded=0,
                skipped=0,
                export_targets=[],
                failures=[],
            )
        finally:
            downloader_module.export_json_files = original_export_json_files
            downloader_module.export_financials_markdown = original_export_financials_markdown
            downloader_module.export_business_overview = original_export_business_overview
            downloader_module.save_manifest = original_save_manifest

        self.assertEqual(summary.exported_json_documents, 1)
        self.assertEqual(call_order, ["json", "financials", "business", "manifest"])


class SelectBusinessOverviewSectionsTests(unittest.TestCase):
    def test_keeps_matching_titles(self) -> None:
        sections = {
            "Description of Business": "body A",
            "Business Results of Group": "body B",
            "Research and Development Activities": "body C",
            "Management Analysis of Financial Position Operating Results and Cash Flows": "body D",
            "Unrelated Section": "body E",
        }
        result = select_business_overview_sections(sections)
        self.assertEqual(
            result,
            {
                "Description of Business": "body A",
                "Business Results of Group": "body B",
                "Research and Development Activities": "body C",
                "Management Analysis of Financial Position Operating Results and Cash Flows": "body D",
            },
        )

    def test_returns_empty_for_no_match(self) -> None:
        sections = {
            "Risk Factors": "body X",
            "Corporate Governance": "body Y",
        }
        result = select_business_overview_sections(sections)
        self.assertEqual(result, {})

    def test_match_is_case_insensitive_and_strips_whitespace(self) -> None:
        sections = {"  BUSINESS RESULTS OF GROUP  ": "body Z"}
        result = select_business_overview_sections(sections)
        self.assertEqual(result, {"  BUSINESS RESULTS OF GROUP  ": "body Z"})


class RenderBusinessOverviewMarkdownTests(unittest.TestCase):
    def _make_payload(self, name_jp: str = "テスト株式会社", doc_id: str = "S100TEST", period_end: str = "2024-03-31") -> dict:
        return {
            "company": {"name_jp": name_jp},
            "metadata": {"doc_id": doc_id, "period_end": period_end},
        }

    def test_only_matching_sections_appear_in_output(self) -> None:
        payload = self._make_payload()
        text_sections = {
            "Description of Business": "We make widgets.",
            "Business Results of Group": "Revenue increased.",
            "Unrelated Section": "Should not appear.",
        }
        md = render_business_overview_markdown(payload, text_sections)
        self.assertIn("## Description of Business", md)
        self.assertIn("## Business Results of Group", md)
        self.assertNotIn("## Unrelated Section", md)
        self.assertNotIn("Should not appear.", md)

    def test_no_matching_sections_shows_fallback_message(self) -> None:
        payload = self._make_payload()
        text_sections = {"Risk Factors": "Various risks exist."}
        md = render_business_overview_markdown(payload, text_sections)
        self.assertIn("No text sections found.", md)

    def test_empty_sections_shows_fallback_message(self) -> None:
        payload = self._make_payload()
        md = render_business_overview_markdown(payload, {})
        self.assertIn("No text sections found.", md)


if __name__ == "__main__":
    unittest.main()
