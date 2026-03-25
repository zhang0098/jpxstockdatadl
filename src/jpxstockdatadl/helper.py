def pick_xbrl_member(names: list[str]) -> str:
    public_doc_xbrl = [
        name for name in names if "/PublicDoc/" in name and name.endswith(".xbrl")
    ]
    if public_doc_xbrl:
        return public_doc_xbrl[0]

    any_xbrl = [name for name in names if name.endswith(".xbrl")]
    if any_xbrl:
        return any_xbrl[0]

    ixbrl_html = [
        name for name in names if name.endswith(("_ixbrl.htm", "_ixbrl.html"))
    ]
    if ixbrl_html:
        return ixbrl_html[0]

    raise FileNotFoundError("No XBRL or iXBRL file found in archive")