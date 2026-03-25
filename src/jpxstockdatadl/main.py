import argparse
import sys

from jpxstockdatadl.downloader import DownloadSummary, download_stock_xbrl


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and process EDINET financial filings for Japanese companies.",
        epilog="Examples:\n"
        "  jpxstockdatadl 6136                       # Download last 5 years of filings\n"
        "  jpxstockdatadl 6136 --years 3             # Download last 3 years of filings\n"
        "\nOutput files:\n"
        "  - .xbrl files: Original XBRL financial data\n"
        "  - .json files: Parsed financial data for each filing\n"
        "  - latest business overview.md: Business description from latest filing\n"
        "\nEnvironment variables:\n"
        "  EDINET_API_KEY: Your EDINET API key",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "stock_code",
        help="4-digit stock code, e.g., 6136 (オーエスジー), 7203 (トヨタ), 6758 (ソニー)",
    )
    parser.add_argument(
        "-years",
        "--years",
        type=int,
        default=5,
        metavar="N",
        help="Download filings from the past N years (default: 5)",
    )
    return parser.parse_args(argv)


def emit_download_summary(summary: DownloadSummary) -> None:
    print(f"Matched filings: {summary.matched_documents}")
    print(f"Downloaded: {summary.downloaded_documents}")
    print(f"Cached: {summary.skipped_documents}")
    print(f"Exported JSON: {summary.exported_json_documents}")
    print(f"Output directory: {summary.output_dir}")
    if summary.failures:
        print("Failures:", file=sys.stderr)
        for failure in summary.failures:
            print(f"- {failure}", file=sys.stderr)


def main() -> None:
    args = parse_args()

    try:
        summary = download_stock_xbrl(args.stock_code, args.years)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    emit_download_summary(summary)


if __name__ == "__main__":
    main()
