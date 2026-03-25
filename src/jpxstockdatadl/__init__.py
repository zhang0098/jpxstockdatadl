from jpxstockdatadl.downloader import DownloadSummary, download_stock_xbrl


def download_xbrl(stock_code: str, years: int = 5) -> DownloadSummary:
    """Download and process EDINET financial filings for a Japanese company.

    Args:
        stock_code: 4-digit stock code (e.g., "6136", "7203")
        years: Number of years of filings to download (default: 5)

    Returns:
        DownloadSummary with counts of matched, downloaded, cached documents, etc.

    Raises:
        OSError: If output directory cannot be created
        ValueError: If stock_code is invalid
        RuntimeError: If API request fails
    """
    return download_stock_xbrl(stock_code, years)


__all__ = ["download_xbrl", "download_stock_xbrl", "DownloadSummary"]
