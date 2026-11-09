"""An SEC filing's HTML as Markdown: its text in reading order, its tables as Markdown tables.

A filing's HTML is laid out for print: each line a block, figures in tables padded with spacer
columns, the dollar sign and the closing parenthesis of a negative number in cells of their own,
and inline XBRL's header hidden at the top. `to_markdown` keeps what a reader sees:

- every block (a paragraph, a `div`, a list item, a heading) becomes a paragraph, its whitespace
  collapsed; a heading keeps its level, a list item its dash;
- what is hidden (`display:none`, the `head`, scripts and styles) is dropped;
- a table loses its empty columns and rows; a cell holding only `$` joins the figure on its right,
  one holding only `)` or `%` the figure on its left, so `$ | (1,234 | )` reads `$(1,234)`. A
  table of one row or one column, or of a short marker and its text (a footnote, a bullet), is a
  layout table and becomes paragraphs;
- a `pre` block keeps its lines.

The conversion uses the standard library's HTML parser only. It cannot make a table that the
HTML misaligns line up; the figures stay in their rows, in order.
"""

import html.parser
import re

BLOCKS = frozenset(
    {
        "address", "article", "blockquote", "body", "center", "dd", "div", "dl", "dt", "figure",
        "footer", "form", "h1", "h2", "h3", "h4", "h5", "h6", "header", "hr", "html", "li", "main",
        "ol", "p", "section", "ul",
    }
)  # fmt: skip
SKIPPED = frozenset({"head", "script", "style", "title", "noscript"})
WRAPPER = frozenset({"type", "sequence", "filename", "description"})  # EDGAR's SGML header, unclosed
VOID = frozenset({"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "wbr"})
HIDDEN = re.compile(r"display\s*:\s*none", re.IGNORECASE)
SPACES = re.compile(r"[ \t\r\n\f\v\u00a0\u2002\u2003\u2009\u200b]+")
LEADS = frozenset({"$", "US$", "C$", "\u20ac", "\u00a3", "\u00a5"})  # a currency sign before its figure
TRAILS = frozenset({")", "%", ")%", "%)", "*"})  # what closes the figure on its left
FIGURE_CHARACTERS = r"-$\u20ac\u00a3\u00a5(),.%\u2013\u2014 0-9"
FIGURE = re.compile(rf"^[{FIGURE_CHARACTERS}]*[0-9\u2013\u2014][{FIGURE_CHARACTERS}]*$")  # 1,234 (5.6)% \u2014
MARKER = re.compile(r"^(\(?[0-9ivxlcdmA-Za-z]{1,4}[.)]?|[\u2022\u25cf\u25aa\u00b7\u2013\u2014*-])$")
ENCODINGS = ("utf-8", "cp1252")


def decode(content: bytes) -> str:
    """A filing's bytes as text: UTF-8, else Windows-1252 (what older filings are written in)."""
    for encoding in ENCODINGS:
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("latin-1")


def _collapse(text: str) -> str:
    return SPACES.sub(" ", text).strip()


class _Cell:
    __slots__ = ("parts", "span")

    def __init__(self, span: int) -> None:
        self.parts: list[str] = []
        self.span = span


class _Table:
    __slots__ = ("rows",)

    def __init__(self) -> None:
        self.rows: list[list[_Cell]] = []

    def cell(self, span: int) -> _Cell:
        if not self.rows:
            self.rows.append([])
        made = _Cell(span)
        self.rows[-1].append(made)
        return made


def _span(attrs: list[tuple[str, str | None]]) -> int:
    for name, value in attrs:
        if name == "colspan" and value:
            try:
                return max(1, min(int(value.strip()), 100))
            except ValueError:
                return 1
    return 1


def _placed(table: _Table) -> tuple[list[list[tuple[int, int, str]]], int]:
    """Each row's non-empty cells as (first column, column after the last, text), and the width."""
    rows = []
    width = 0
    for row in table.rows:
        at = 0
        cells = []
        for cell in row:
            text = _collapse("".join(cell.parts))
            if text:
                cells.append((at, at + cell.span, text))
            at += cell.span
        width = max(width, at)
        if cells:
            rows.append(cells)
    return rows, width


def _columns(rows: list[list[tuple[int, int, str]]], width: int) -> list[int]:
    """The table's column of each grid column. The grid columns that one figure spans, or that a
    figure and its `$` or `)` occupy, are one column: filers pad each figure with spacer cells and
    give a sign its own cell, and a row without the sign spans the figure over the sign's cell."""
    parent = list(range(width))

    def find(column: int) -> int:
        while parent[column] != column:
            parent[column] = parent[parent[column]]
            column = parent[column]
        return column

    def join(first: int, after: int) -> None:
        root = find(first)
        for column in range(first + 1, after):
            parent[find(column)] = root

    for cells in rows:
        for at, (first, after, text) in enumerate(cells):
            if text in LEADS and at + 1 < len(cells):
                join(first, cells[at + 1][1])
            elif text in TRAILS and at > 0:
                join(cells[at - 1][0], after)
            elif FIGURE.match(text):
                join(first, after)
    first_of: dict[int, int] = {}
    for column in range(width):
        first_of.setdefault(find(column), column)
    order = {root: number for number, root in enumerate(sorted(first_of, key=first_of.__getitem__))}
    return [order[find(column)] for column in range(width)]


def _joined(left: str, right: str) -> str:
    if not left:
        return right
    if left in LEADS or left.endswith("(") or right in TRAILS:
        return left + right
    return left + " " + right


def _rows(table: _Table) -> list[list[str]]:
    rows, width = _placed(table)
    if not rows:
        return []
    columns = _columns(rows, width)
    count = max(columns) + 1
    out = []
    for cells in rows:
        row = [""] * count
        for first, _after, text in cells:
            column = columns[first]
            row[column] = _joined(row[column], text)
        out.append(row)
    return out


def _drop_empty(rows: list[list[str]]) -> list[list[str]]:
    rows = [row for row in rows if any(row)]
    if not rows:
        return []
    keep = [column for column in range(len(rows[0])) if any(row[column] for row in rows)]
    return [[row[column] for column in keep] for row in rows]


def _cell_text(text: str) -> str:
    return text.replace("\\", "\\\\").replace("|", "\\|")


def render_table(table: _Table) -> list[str]:
    """The table as Markdown blocks: one table, or paragraphs for a layout table."""
    rows = _drop_empty(_rows(table))
    if not rows:
        return []
    width = len(rows[0])
    if width == 1 or (len(rows) == 1 and width <= 2):
        return [" ".join(cell for cell in row if cell) for row in rows]
    if width == 2 and all(MARKER.match(row[0]) or not row[0] for row in rows):
        return [" ".join(cell for cell in row if cell) for row in rows]
    lines = ["| " + " | ".join(_cell_text(cell) for cell in rows[0]) + " |"]
    lines.append("|" + "---|" * width)
    lines.extend("| " + " | ".join(_cell_text(cell) for cell in row) + " |" for row in rows[1:])
    return ["\n".join(lines)]


class _Converter(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.blocks: list[str] = []
        self.line: list[str] = []
        self.prefix = ""
        self.hidden: list[tuple[str, int]] = []  # (tag, depth) of each open hidden element
        self.skipping = 0
        self.tables: list[_Table] = []
        self.cell: _Cell | None = None
        self.pre = 0
        self.wrapper = False  # the next text is a value of EDGAR's SGML header

    # text outside tables -------------------------------------------------------------------

    def _flush(self) -> None:
        if self.pre:
            text = "".join(self.line).strip("\n")
            if text.strip():
                self.blocks.append("```\n" + text + "\n```")
        else:
            text = _collapse("".join(self.line))
            if text:
                self.blocks.append(self.prefix + text)
        self.line = []
        self.prefix = ""

    def _break(self) -> None:
        if self.tables:
            if self.cell is not None:
                self.cell.parts.append(" ")
            return
        self._flush()

    # the parser's callbacks --------------------------------------------------------------------

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.hidden:
            name, depth = self.hidden[-1]
            if tag == name and tag not in VOID:
                self.hidden[-1] = (name, depth + 1)
            return
        self.wrapper = tag in WRAPPER
        if tag in SKIPPED:
            self.skipping += 1
            return
        if self.skipping or self.wrapper:
            return
        style = next((value for name, value in attrs if name == "style" and value), "")
        if HIDDEN.search(style) and tag not in VOID:
            self.hidden.append((tag, 1))
            return
        self._open(tag, attrs)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        # XHTML's `<td colspan="3"/>` is an empty cell that still takes its columns
        self.handle_starttag(tag, attrs)
        if tag not in VOID:
            self.handle_endtag(tag)

    def _open(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            if not self.tables:
                self._flush()
                self.cell = None
            elif self.cell is not None:
                self.cell.parts.append(" ")
            self.tables.append(_Table())
        elif tag == "tr" and self.tables:
            if len(self.tables) == 1:
                self.tables[-1].rows.append([])
                self.cell = None
            elif self.cell is not None:
                self.cell.parts.append(" ")
        elif tag in {"td", "th"} and self.tables:
            if len(self.tables) == 1:
                self.cell = self.tables[-1].cell(_span(attrs))
            elif self.cell is not None:
                self.cell.parts.append(" ")
        elif tag == "pre":
            self._break()
            self.pre += 1
        elif tag == "br":
            if self.pre:
                self.line.append("\n")
            else:
                self._break()
        elif tag in BLOCKS:
            self._break()
            if not self.tables:
                if tag == "li":
                    self.prefix = "- "
                elif len(tag) == 2 and tag[0] == "h" and tag[1].isdigit():
                    self.prefix = "#" * int(tag[1]) + " "

    def handle_endtag(self, tag: str) -> None:
        if self.hidden:
            name, depth = self.hidden[-1]
            if tag == name:
                if depth == 1:
                    self.hidden.pop()
                else:
                    self.hidden[-1] = (name, depth - 1)
            return
        if tag in SKIPPED:
            self.skipping = max(0, self.skipping - 1)
            return
        if tag == "table" and self.tables:
            table = self.tables.pop()
            if self.tables:  # a nested table's text stays in the cell that holds it
                return
            self.cell = None
            self.blocks.extend(render_table(table))
        elif tag in {"td", "th"} and len(self.tables) == 1:
            self.cell = None
        elif tag == "pre" and self.pre:
            self._flush()
            self.pre -= 1
        elif tag in BLOCKS:
            self._break()

    def handle_data(self, data: str) -> None:
        if self.wrapper:
            self.wrapper = False
            return
        if self.hidden or self.skipping:
            return
        if self.tables:
            if self.cell is not None:
                self.cell.parts.append(data)
            return
        self.line.append(data)

    def finish(self) -> str:
        self.close()
        while self.tables:  # a table the HTML never closed
            table = self.tables.pop()
            if not self.tables:
                self.blocks.extend(render_table(table))
        self._flush()
        return "\n\n".join(self.blocks) + "\n" if self.blocks else ""


def to_markdown(content: bytes | str) -> str:
    """The Markdown of one filing's HTML (bytes as stored, or text)."""
    converter = _Converter()
    converter.feed(content if isinstance(content, str) else decode(content))
    return converter.finish()
