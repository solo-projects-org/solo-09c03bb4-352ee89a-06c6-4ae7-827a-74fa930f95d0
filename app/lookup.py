"""批次回查核心：按清单版号 + 原档相对路径 + 0 起行号区间抽查过磅明细。

两类来源：
* ``original``（复用原档）：直接按行组剪枝读原档对应行，原行号即文件内行号；
* ``split``（跨日拆片）：从各拆片配套的行位映射 Parquet（单列 int64
  ``source_row_index``，与拆片行组对齐）回查，命中哪个行组就读哪个行组，
  票号或内容相同的行因原行号不同会在各自原位置各返回一次，不去重。

版本规则：请求开始核对版号，整页只读这一版清单（快照贯穿全程）；提交旧版号
明确提示重取清单；当前版的旧拆片没有行位映射时明确提示无法回查。
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pyarrow.parquet as pq

from .config import Settings
from .manifest import DayFile, Manifest, ManifestStore
from .publisher import ROW_MAP_COLUMN


class LookupError(RuntimeError):
    """回查失败基类；status_code 给出建议 HTTP 状态，code 供调用方区分。"""

    status_code = 400
    code = "lookup_error"


class LookupVersionStale(LookupError):
    """提交的版号不是当前版：清单已换版，需重新拉取清单后再回查。"""

    status_code = 409
    code = "version_stale"


class LookupVersionMissing(LookupError):
    """提交的版号没有对应归档（如失败发布从未换版）。"""

    status_code = 404
    code = "version_missing"


class LookupNoManifest(LookupError):
    """尚未发布任何版本。"""

    status_code = 404
    code = "no_manifest"


class LookupSourceNotFound(LookupError):
    """该版清单里找不到指定原档（或该原档在该版没有任何条目）。"""

    status_code = 404
    code = "source_not_found"


class LookupRowMapMissing(LookupError):
    """当前版里该原档的旧拆片没有行位映射，无法按原行号回查。"""

    status_code = 409
    code = "row_map_missing"


class LookupFileMissing(LookupError):
    """清单登记的文件/映射在磁盘上已丢失。"""

    status_code = 410
    code = "file_missing"


@dataclasses.dataclass
class LookupResult:
    version: int
    source_file: str
    start: int
    limit: int
    total_rows: int  # 原档总行数（可抽查的原行号空间）
    columns: list[str]
    records: list[dict]  # 已按原行号升序，每条带 source_row_index


class LookupService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = ManifestStore(settings.output_dir)

    # ------------------------------------------------------------ 版本核对
    def open_snapshot(self, version: int) -> Manifest:
        """请求开始核对版号并固定清单快照；整页回查都用这一个对象。"""
        current = self.store.load_current()
        if current is None:
            raise LookupNoManifest("尚无已发布版本，无法回查")
        if version != current.version:
            archived = self.store.load_version(version)
            if archived is None:
                raise LookupVersionMissing(
                    f"版号 v{version} 不存在或从未成功换版"
                )
            raise LookupVersionStale(
                f"清单已换版：请求版号 v{version} 不是当前版 v{current.version}，"
                "请重新拉取清单后用新版号回查"
            )
        return current

    # ------------------------------------------------------------ 回查入口
    def lookup(
        self, version: int, source_file: str, start: int, limit: int
    ) -> LookupResult:
        manifest = self.open_snapshot(version)  # 快照固定，之后不再读 current.json
        entries = [d for d in manifest.days if d.source_file == source_file]
        if not entries:
            if source_file not in manifest.input_files:
                raise LookupSourceNotFound(
                    f"版号 v{version} 的清单中没有原档 {source_file}"
                )
            # 在清单内但零条目（零行文件）：从原档页脚取列名，不读数据行
            rel = Path(source_file)
            if rel.is_absolute() or ".." in rel.parts:
                raise LookupError("只允许 input_dir 内的相对路径")
            input_path = (self.settings.input_dir / rel).resolve()
            root = self.settings.input_dir.resolve()
            if root not in input_path.parents and input_path != root:
                raise LookupError("路径越界")
            if not input_path.is_file():
                raise LookupFileMissing(f"原档文件已丢失: {source_file}")
            columns = pq.ParquetFile(input_path).schema_arrow.names
            return LookupResult(version, source_file, start, limit, 0, columns, [])

        total_rows = sum(d.rows for d in entries)
        if total_rows == 0:
            # 零行文件：从原档页脚取列名，不读数据行
            any_entry = entries[0]
            if any_entry.source == "original":
                schema_path = self.store.resolve(
                    any_entry, input_dir=self.settings.input_dir
                )
            else:
                schema_path = self.store.resolve_staged(any_entry.path)
            columns = pq.ParquetFile(schema_path).schema_arrow.names
            return LookupResult(version, source_file, start, limit, 0, columns, [])

        wanted = set(range(start, start + limit))
        if not wanted:
            raise LookupError("条数必须为正")

        rows: list[tuple[int, dict]] = []
        columns: list[str] = []
        for entry in entries:
            if entry.source == "original":
                cols, part = self._lookup_original(entry, wanted)
            else:
                cols, part = self._lookup_split(entry, wanted)
            if not columns:
                columns = cols
            rows.extend(part)

        if not rows and not columns:
            # 区间越过 EOF 的空页：从任一登记文件页脚补列名，不读数据行
            any_entry = entries[0]
            if any_entry.source == "original":
                schema_path = self.store.resolve(
                    any_entry, input_dir=self.settings.input_dir
                )
            else:
                schema_path = self.store.resolve_staged(any_entry.path)
            columns = pq.ParquetFile(schema_path).schema_arrow.names

        # 按原档行序返回；票号/内容相同的行也各占原位置，不去重
        rows.sort(key=lambda item: item[0])
        return LookupResult(
            version=version,
            source_file=source_file,
            start=start,
            limit=limit,
            total_rows=total_rows,
            columns=columns,
            records=[
                dict(record, source_row_index=idx) for idx, record in rows
            ],
        )

    # ----------------------------------------------------- original 回查
    def _lookup_original(
        self, entry: DayFile, wanted: set[int]
    ) -> tuple[list[str], list[tuple[int, dict]]]:
        """复用原档直接回查：只读取命中行所在的 row group。"""
        path = self.store.resolve(entry, input_dir=self.settings.input_dir)
        if not path.is_file():
            raise LookupFileMissing(f"原档文件已丢失: {entry.path}")
        pf = pq.ParquetFile(path)
        meta = pf.metadata
        hits: list[tuple[int, dict]] = []
        columns: list[str] = []
        offset = 0
        for rg in range(meta.num_row_groups):
            rg_rows = meta.row_group(rg).num_rows
            rg_range = range(offset, offset + rg_rows)
            if wanted.intersection(rg_range):  # 仅命中行组才读
                table = pf.read_row_group(rg)
                if not columns:
                    columns = table.column_names
                for local_idx in range(rg_rows):
                    source_idx = offset + local_idx
                    if source_idx in wanted:
                        hits.append((source_idx, table.slice(local_idx, 1).to_pylist()[0]))
            offset += rg_rows
        return columns, hits

    # -------------------------------------------------------- split 回查
    def _lookup_split(
        self, entry: DayFile, wanted: set[int]
    ) -> tuple[list[str], list[tuple[int, dict]]]:
        """跨日拆片回查：经行位映射定位，只读取命中行所在行组。"""
        if not entry.row_map:
            raise LookupRowMapMissing(
                f"原档 {entry.source_file} 的拆片 {entry.path} 是没有行位映射的"
                "旧版产物，无法按原行号回查；请重新发布取得带映射的新版清单"
            )
        data_path = self.store.resolve_staged(entry.path)
        map_path = self.store.resolve_staged(entry.row_map)
        if not data_path.is_file() or not map_path.is_file():
            raise LookupFileMissing(
                f"拆片或行位映射文件已丢失: {entry.path} / {entry.row_map}"
            )
        data_pf = pq.ParquetFile(data_path)
        map_pf = pq.ParquetFile(map_path)
        if data_pf.metadata.num_row_groups != map_pf.metadata.num_row_groups:
            raise LookupFileMissing(
                f"拆片 {entry.path} 与行位映射行组不一致"
            )
        col_index = map_pf.metadata.schema.names.index(ROW_MAP_COLUMN)

        hits: list[tuple[int, dict]] = []
        columns: list[str] = []
        for rg in range(map_pf.metadata.num_row_groups):
            stats = map_pf.metadata.row_group(rg).column(col_index).statistics
            if stats is not None and stats.min is not None and stats.max is not None:
                # 页脚统计剪枝：本页区间与该映射行组无交集则整组跳过，
                # 映射与拆片都不读数据行
                if stats.max < min(wanted) or stats.min > max(wanted):
                    continue
            # 命中候选行组：先读映射列定位，再按本地行号读同一行组的拆片数据
            map_col = map_pf.read_row_group(rg, columns=[ROW_MAP_COLUMN]).column(0)
            local_picks = [
                local for local, source_idx in enumerate(map_col.to_pylist())
                if source_idx in wanted
            ]
            if not local_picks:
                continue
            table = data_pf.read_row_group(rg)
            if not columns:
                columns = table.column_names
            source_values = map_col.to_pylist()
            for local in local_picks:
                hits.append((source_values[local], table.slice(local, 1).to_pylist()[0]))
        return columns, hits
