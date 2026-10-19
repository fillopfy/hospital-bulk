import csv
import io
from dataclasses import dataclass, field
from typing import List, Optional

REQUIRED_COLUMNS = ("name", "address")
OPTIONAL_COLUMNS = ("phone",)
KNOWN_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS


@dataclass
class CsvRow:
    row: int  # 1-based data row number (header excluded), as in the spec
    name: str
    address: str
    phone: Optional[str] = None

    def to_payload(self, batch_id: str) -> dict:
        return {
            "name": self.name,
            "address": self.address,
            "phone": self.phone,
            "creation_batch_id": batch_id,
        }


@dataclass
class RowError:
    row: Optional[int]  # None = file-level problem
    message: str


@dataclass
class ParseResult:
    rows: List[CsvRow] = field(default_factory=list)
    errors: List[RowError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def parse_csv(content: bytes, max_rows: int = 20, max_bytes: int = 1_000_000) -> ParseResult:
    result = ParseResult()

    if not content or not content.strip():
        result.errors.append(RowError(None, "CSV file is empty"))
        return result
    if len(content) > max_bytes:
        result.errors.append(RowError(None, f"CSV file exceeds {max_bytes} bytes"))
        return result

    try:
        # utf-8-sig strips the BOM that Excel adds to exported CSVs
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        result.errors.append(RowError(None, "CSV must be UTF-8 encoded"))
        return result

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        result.errors.append(RowError(None, "CSV has no header row"))
        return result
    except csv.Error as exc:
        result.errors.append(RowError(None, f"Malformed CSV: {exc}"))
        return result

    columns = [h.strip().lower() for h in header]
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        result.errors.append(
            RowError(None, f"Missing required column(s): {', '.join(missing)}. "
                           f"Expected header: name,address,phone")
        )
        return result
    if len(set(columns)) != len(columns):
        result.errors.append(RowError(None, "Duplicate column names in header"))
        return result
    extra = [c for c in columns if c and c not in KNOWN_COLUMNS]
    if extra:
        result.warnings.append(f"Ignoring unknown column(s): {', '.join(extra)}")

    idx = {c: i for i, c in enumerate(columns)}
    data_row = 0
    try:
        for raw in reader:
            if not any(cell.strip() for cell in raw):
                continue  # skip blank lines
            data_row += 1
            if data_row > max_rows:
                continue  # keep counting so we can report the real total

            def cell(col: str) -> str:
                i = idx.get(col)
                return raw[i].strip() if i is not None and i < len(raw) else ""

            name, address, phone = cell("name"), cell("address"), cell("phone")
            problems = []
            if not name:
                problems.append("name is required")
            if not address:
                problems.append("address is required")
            if len(raw) > len(columns):
                problems.append("row has more values than header columns")
            if problems:
                result.errors.append(RowError(data_row, "; ".join(problems)))
                continue
            result.rows.append(CsvRow(data_row, name, address, phone or None))
    except csv.Error as exc:
        result.errors.append(RowError(data_row + 1, f"Malformed CSV: {exc}"))
        return result

    if data_row == 0:
        result.errors.append(RowError(None, "CSV has no data rows"))
    elif data_row > max_rows:
        result.errors.append(
            RowError(None, f"CSV has {data_row} hospitals; maximum allowed is {max_rows}")
        )
    return result
