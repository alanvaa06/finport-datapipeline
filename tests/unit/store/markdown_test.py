from data_pipeline.store.markdown import to_markdown


def test_blocks_become_paragraphs_and_what_is_hidden_is_dropped():
    page = """<?xml version='1.0' encoding='ASCII'?><html><head><title>aapl-20250927</title>
    <style>p {color: red}</style></head><body>
    <div style="display:none"><ix:header><ix:hidden>false</ix:hidden></ix:header><div>nested</div></div>
    <h2>Item 7.  Management&#8217;s Discussion</h2>
    <div><span>Net sales</span> <span>rose</span></div><p>Second
    paragraph.</p><ul><li>one</li><li>two</li></ul><br/><script>var x = 1;</script>
    </body></html>"""
    assert to_markdown(page) == (
        "## Item 7. Management\u2019s Discussion\n\nNet sales rose\n\nSecond paragraph.\n\n- one\n\n- two\n"
    )


def test_a_financial_table_loses_its_spacer_columns_and_keeps_signs_with_figures():
    # as Workiva writes it: XHTML empty cells, the dollar sign and the parenthesis in cells of their
    # own, and a row without the sign spanning its figure over the sign's cell
    page = """<table>
    <tr><td colspan="3"/><td colspan="3"><span>2026</span></td><td colspan="3"/><td colspan="3">2025</td></tr>
    <tr><td colspan="3">Net sales:</td><td colspan="9"/></tr>
    <tr><td colspan="3">Products</td><td>$</td><td colspan="2">355,533</td><td colspan="3"/>
        <td>$</td><td colspan="2">307,003</td></tr>
    <tr><td colspan="3">Services</td><td colspan="3">123,301</td><td colspan="3"/><td colspan="3">109,158</td></tr>
    <tr><td colspan="3">Other | net</td><td colspan="2">(<span>321</span></td><td>)</td><td colspan="3"/>
        <td colspan="3">&#8212;</td></tr>
    </table>"""
    assert to_markdown(page) == (
        "|  | 2026 | 2025 |\n"
        "|---|---|---|\n"
        "| Net sales: |  |  |\n"
        "| Products | $355,533 | $307,003 |\n"
        "| Services | 123,301 | 109,158 |\n"
        "| Other \\| net | (321) | \u2014 |\n"
    )


def test_a_layout_table_becomes_paragraphs():
    page = """<p>Text</p><table><tr><td>(1)</td><td>Includes a one-time charge.</td></tr>
    <tr><td>&#8226;</td><td>A bullet.</td></tr></table>
    <table><tr><td>Signature line only</td></tr></table>"""
    assert to_markdown(page) == "Text\n\n(1) Includes a one-time charge.\n\n\u2022 A bullet.\n\nSignature line only\n"


def test_a_nested_table_keeps_its_text_in_the_cell_that_holds_it():
    page = """<table><tr><td>Label</td><td>Value</td></tr>
    <tr><td><table><tr><td>inner</td><td>text</td></tr></table></td><td>7</td></tr></table>"""
    assert to_markdown(page) == "| Label | Value |\n|---|---|\n| inner text | 7 |\n"


def test_older_filings_lose_edgars_header_and_read_as_windows_1252():
    page = (
        b"<DOCUMENT>\n<TYPE>EX-99.1\n<SEQUENCE>2\n<FILENAME>d1dex991.htm\n<DESCRIPTION>PRESS RELEASE\n<TEXT>\n"
        b"<html><body><p>Apple\x92s quarter</p><pre>line one\n  line two</pre></body></html>\n</TEXT>\n</DOCUMENT>"
    )
    assert to_markdown(page) == "Apple\u2019s quarter\n\n```\nline one\n  line two\n```\n"


def test_an_empty_page_is_empty():
    assert to_markdown(b"<html><head><title>x</title></head><body> </body></html>") == ""
