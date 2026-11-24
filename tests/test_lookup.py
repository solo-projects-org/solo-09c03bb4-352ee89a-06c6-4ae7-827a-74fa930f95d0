"""批次回查测试：原档直查、拆片映射回查、行组剪枝、版号规则与换版闸门。"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app import api
from app.lookup import LookupService
from app.manifest import ManifestStore
from app.publisher import ROW_MAP_COLUMN, PublishError, publish
from conftest import SCHEMA, utc, write_parquet


def _client(settings):
    api.get_settings = lambda: settings  # type: ignore[method-assign]
    api.get_service = lambda: api.DatasetService(settings)  # type: ignore[method-assign]
    return TestClient(api.app)


def _publish_mixed(settings):
    """4 行跨日档：原档行号 0..3，拆成 23 日(0) 与 24 日(1,2,3) 两片。"""
    path = settings.input_dir / "mixed.parquet"
    write_parquet(path, [
        ("M1", utc("2026-11-23 23:20:00"), 10),
        ("M2", utc("2026-11-24 00:05:00"), 20),
        ("M3", utc("2026-11-24 09:15:00"), 30),
        ("M4", utc("2026-11-24 18:30:00"), 40),
    ])
    return publish(["mixed.parquet"], settings).manifest


# ----------------------------------------------------------- 拆片映射产物
def test_split_writes_row_map_aligned_with_pieces(settings):
    manifest = _publish_mixed(settings)
    store = ManifestStore(settings.output_dir)
    splits = {d.date: d for d in manifest.days if d.source == "split"}
    assert set(splits) == {"2026-11-23", "2026-11-24"}
    for day, entry in splits.items():
        assert entry.row_map is not None
        map_path = store.resolve_staged(entry.row_map)
        data_path = store.resolve_staged(entry.path)
        assert map_path.is_file() and data_path.is_file()
        map_pf = pq.ParquetFile(map_path)
        data_pf = pq.ParquetFile(data_path)
        assert map_pf.schema_arrow.names == [ROW_MAP_COLUMN]
        assert map_pf.metadata.num_rows == entry.rows
        assert map_pf.metadata.num_row_groups == data_pf.metadata.num_row_groups
    # 合并两片的原行号，恰好覆盖 0..3，不重不漏
    indices = []
    for entry in splits.values():
        indices.extend(
            pq.read_table(store.resolve_staged(entry.row_map)).column(0).to_pylist()
        )
    assert sorted(indices) == [0, 1, 2, 3]


# ----------------------------------------------------------- 原档直查
def test_lookup_original_returns_rows_in_original_order(settings):
    path = settings.input_dir / "same.parquet"
    write_parquet(path, [
        ("A", utc("2026-11-23 01:00:00"), 1),
        ("B", utc("2026-11-23 02:00:00"), 2),
        ("C", utc("2026-11-23 03:00:00"), 3),
    ])
    manifest = publish(["same.parquet"], settings).manifest

    result = LookupService(settings).lookup(manifest.version, "same.parquet", 1, 2)
    assert result.total_rows == 3
    assert [(r["source_row_index"], r["ticket_id"]) for r in result.records] == [
        (1, "B"),
        (2, "C"),
    ]
    assert result.columns == ["ticket_id", "weigh_time", "gross_weight_kg"]


def test_lookup_split_reconstructs_original_positions(settings):
    manifest = _publish_mixed(settings)
    result = LookupService(settings).lookup(
        manifest.version, "mixed.parquet", 0, 4
    )
    # 跨两片拼回，按原档行序，原行号连续
    assert result.total_rows == 4
    assert [(r["source_row_index"], r["ticket_id"]) for r in result.records] == [
        (0, "M1"),
        (1, "M2"),
        (2, "M3"),
        (3, "M4"),
    ]


def test_lookup_split_pagination_only_touches_wanted_rows(settings):
    manifest = _publish_mixed(settings)
    svc = LookupService(settings)
    page = svc.lookup(manifest.version, "mixed.parquet", 2, 1)
    assert [(r["source_row_index"], r["ticket_id"]) for r in page.records] == [
        (2, "M3")
    ]
    # 翻页取最后一行
    page = svc.lookup(manifest.version, "mixed.parquet", 3, 10)
    assert [(r["source_row_index"], r["ticket_id"]) for r in page.records] == [
        (3, "M4")
    ]
    # 越过 EOF：合法空页
    page = svc.lookup(manifest.version, "mixed.parquet", 99, 5)
    assert page.records == [] and page.total_rows == 4


def test_duplicate_ticket_and_identical_rows_each_returned_once_per_position(settings):
    # 两行票号相同、再两行整行内容完全相同：回查按原行号各返回一次，不去重
    path = settings.input_dir / "dup.parquet"
    write_parquet(path, [
        ("DUP", utc("2026-11-23 23:00:00"), 7),
        ("DUP", utc("2026-11-24 01:00:00"), 7),
        ("SAME", utc("2026-11-24 02:00:00"), 9),
        ("SAME", utc("2026-11-24 03:00:00"), 9),
    ])
    manifest = publish(["dup.parquet"], settings).manifest
    result = LookupService(settings).lookup(manifest.version, "dup.parquet", 0, 10)
    assert [r["source_row_index"] for r in result.records] == [0, 1, 2, 3]
    assert [r["ticket_id"] for r in result.records] == ["DUP", "DUP", "SAME", "SAME"]


# ----------------------------------------------------------- 行组剪枝
def test_lookup_reads_only_row_groups_containing_hits(settings, monkeypatch):
    # 3 个 row group 的跨日档，每 group 2 行；全局行号 0..5
    def one_row(ticket, when, weight):
        return pa.table(
            {
                "ticket_id": [ticket],
                "weigh_time": [when],
                "gross_weight_kg": [weight],
            },
            schema=SCHEMA,
        )

    writer = pq.ParquetWriter(settings.input_dir / "mrg.parquet", SCHEMA)
    seq = [
        ("A", utc("2026-11-23 10:00:00"), 1),
        ("B", utc("2026-11-23 11:00:00"), 1),
        ("C", utc("2026-11-24 10:00:00"), 1),
        ("D", utc("2026-11-24 11:00:00"), 1),
        ("E", utc("2026-11-24 12:00:00"), 1),
        ("F", utc("2026-11-25 10:00:00"), 1),
    ]
    for i in range(0, 6, 2):
        rg = pa.concat_tables([one_row(*seq[i]), one_row(*seq[i + 1])])
        writer.write_table(rg)
    writer.close()
    manifest = publish(["mrg.parquet"], settings).manifest

    read_groups: list[tuple[str, int]] = []
    orig_read = pq.ParquetFile.read_row_group
    orig_init = pq.ParquetFile.__init__

    def tagged_init(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        self._test_path = str(args[0])

    def spy(self, rg, *args, **kwargs):
        read_groups.append((self._test_path, rg))
        return orig_read(self, rg, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "__init__", tagged_init)
    monkeypatch.setattr(pq.ParquetFile, "read_row_group", spy)
    # 只要原行号 4（原档第 3 组；映射到 24 日拆片的第 2 组）
    result = LookupService(settings).lookup(manifest.version, "mrg.parquet", 4, 1)
    assert [r["ticket_id"] for r in result.records] == ["E"]
    # 每个被打开的 Parquet 只读取一个命中行组；23 日、25 日拆片整组跳过
    assert read_groups, "应当至少读取一个命中行组"
    assert len(read_groups) == 2  # 映射片 + 数据片各一次
    assert {rg for _, rg in read_groups} == {1}
    touched = sorted(p.rsplit("/", 1)[-1] for p, _ in read_groups)
    assert touched == ["mrg__2026-11-24.parquet", "mrg__2026-11-24.rowmap.parquet"]


def test_original_lookup_also_prunes_row_groups(settings, monkeypatch):
    path = settings.input_dir / "rg.parquet"
    writer = pq.ParquetWriter(path, SCHEMA)
    for ticket, when in [
        ("A", utc("2026-11-23 01:00:00")),
        ("B", utc("2026-11-23 02:00:00")),
        ("C", utc("2026-11-23 03:00:00")),
        ("D", utc("2026-11-23 04:00:00")),
    ]:
        writer.write_table(
            pa.table(
                {"ticket_id": [ticket], "weigh_time": [when], "gross_weight_kg": [1]},
                schema=SCHEMA,
            )
        )
    writer.close()
    manifest = publish(["rg.parquet"], settings).manifest
    assert manifest.days[0].source == "original"

    read_groups: list[int] = []
    orig = pq.ParquetFile.read_row_group

    def spy(self, rg, *args, **kwargs):
        read_groups.append(rg)
        return orig(self, rg, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "read_row_group", spy)
    result = LookupService(settings).lookup(manifest.version, "rg.parquet", 2, 1)
    assert [r["ticket_id"] for r in result.records] == ["C"]
    assert read_groups == [2]


def test_empty_page_and_zero_row_file_still_report_columns(settings):
    manifest = _publish_mixed(settings)
    svc = LookupService(settings)
    # 越过 EOF 的空页：仍返回完整列名（仅读页脚，不读数据行）
    page = svc.lookup(manifest.version, "mixed.parquet", 99, 5)
    assert page.records == []
    assert page.columns == ["ticket_id", "weigh_time", "gross_weight_kg"]

    # 零行文件：清单无 day 条目但在 input_files 内，total=0、列名来自页脚
    pq.write_table(SCHEMA.empty_table(), settings.input_dir / "empty.parquet")
    m2 = publish(["mixed.parquet", "empty.parquet"], settings).manifest
    page = LookupService(settings).lookup(m2.version, "empty.parquet", 0, 10)
    assert page.total_rows == 0 and page.records == []
    assert page.columns == ["ticket_id", "weigh_time", "gross_weight_kg"]


# ----------------------------------------------------------- 版号规则
def test_stale_version_is_rejected_and_current_still_works(settings):
    v1 = _publish_mixed(settings).version
    same = settings.input_dir / "same.parquet"
    write_parquet(same, [("S", utc("2026-11-23 01:00:00"), 1)])
    v2 = publish(["mixed.parquet", "same.parquet"], settings).manifest.version
    assert (v1, v2) == (1, 2)

    svc = LookupService(settings)
    with pytest.raises(Exception) as exc:
        svc.lookup(v1, "mixed.parquet", 0, 1)
    assert exc.value.status_code == 409 and exc.value.code == "version_stale"
    # 旧版归档仍在，但 API 拒绝旧版号、提示重取清单
    assert ManifestStore(settings.output_dir).load_version(v1) is not None
    # 当前版号正常回查
    ok = svc.lookup(v2, "mixed.parquet", 0, 1)
    assert ok.records[0]["ticket_id"] == "M1"


def test_unknown_and_zero_version(settings):
    _publish_mixed(settings)
    svc = LookupService(settings)
    with pytest.raises(Exception) as exc:
        svc.lookup(99, "mixed.parquet", 0, 1)
    assert exc.value.status_code == 404 and exc.value.code == "version_missing"


def test_source_not_in_manifest_404(settings):
    manifest = _publish_mixed(settings)
    with pytest.raises(Exception) as exc:
        LookupService(settings).lookup(manifest.version, "nope.parquet", 0, 1)
    assert exc.value.status_code == 404 and exc.value.code == "source_not_found"


def test_old_split_manifest_without_row_map_is_unlookupable(settings):
    # 手工构造一份“当前版但拆片无 row_map”的旧结构清单（模拟历史遗留版）
    from app.manifest import Manifest

    manifest = _publish_mixed(settings)
    payload = manifest.to_dict()
    for d in payload["days"]:
        d.pop("row_map", None)
    legacy = Manifest.from_dict(payload)
    ManifestStore(settings.output_dir).commit(legacy)

    with pytest.raises(Exception) as exc:
        LookupService(settings).lookup(legacy.version, "mixed.parquet", 0, 1)
    assert exc.value.status_code == 409 and exc.value.code == "row_map_missing"


def test_publish_failure_keeps_previous_version_lookupable(settings):
    v1 = _publish_mixed(settings)
    bad = settings.input_dir / "bad.parquet"
    write_parquet(bad, [("X", None, 9)])
    with pytest.raises(PublishError):
        publish(["mixed.parquet", "bad.parquet"], settings)
    # 失败后上一版仍是当前版，回查照常
    result = LookupService(settings).lookup(v1.version, "mixed.parquet", 0, 4)
    assert [r["ticket_id"] for r in result.records] == ["M1", "M2", "M3", "M4"]


# ----------------------------------------------------------- 换版闸门
def test_missing_row_map_file_blocks_swap(settings, monkeypatch):
    # 让映射路径在校验时判定为“不齐全”：换版闸门必须拦住，current.json 不被替换
    from app import manifest as manifest_mod

    real_resolve = manifest_mod.ManifestStore.resolve_staged

    def hide_rowmap(self, rel_path):
        p = real_resolve(self, rel_path)
        if rel_path.endswith(".rowmap.parquet"):
            return p.parent / "missing-rowmap.parquet"
        return p

    monkeypatch.setattr(manifest_mod.ManifestStore, "resolve_staged", hide_rowmap)
    with pytest.raises(PublishError, match="行位映射文件缺失"):
        _publish_mixed(settings)
    assert ManifestStore(settings.output_dir).load_current() is None
    # 暂存目录被清理
    assert not [p for p in (settings.output_dir / "staged").rglob("*") if p.is_file()]


def test_footer_fast_path_still_reads_no_data_rows(settings, monkeypatch):
    # 发布快路径不受映射逻辑影响：同日档不写任何映射、不读数据行
    path = settings.input_dir / "fast.parquet"
    write_parquet(path, [
        ("T1", utc("2026-11-23 01:00:00"), 1),
        ("T2", utc("2026-11-23 23:00:00"), 2),
    ])

    def boom(*args, **kwargs):
        raise AssertionError("快路径不应读取数据行")

    monkeypatch.setattr(pq, "read_table", boom)
    monkeypatch.setattr(pq.ParquetFile, "read", boom)
    monkeypatch.setattr(pq.ParquetFile, "iter_batches", boom)

    result = publish(["fast.parquet"], settings)
    assert result.outcomes[0].mode == "reuse_footer"
    assert result.manifest.days[0].row_map is None
    assert not list((settings.output_dir / "staged").rglob("*.rowmap.parquet"))


# ----------------------------------------------------------- HTTP API
def test_lookup_api_happy_path_and_errors(settings):
    client = _client(settings)
    # 无版本
    assert client.get(
        "/batches/lookup?version=1&source_file=x.parquet&start=0&limit=1"
    ).status_code == 404

    manifest = _publish_mixed(settings)
    url = (
        f"/batches/lookup?version={manifest.version}"
        "&source_file=mixed.parquet&start=1&limit=2"
    )
    r = client.get(url)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["rows_total"] == 4 and body["rows_returned"] == 2
    assert [(x["source_row_index"], x["ticket_id"]) for x in body["records"]] == [
        (1, "M2"),
        (2, "M3"),
    ]

    # 旧版号 → 409 提示重取清单
    same = settings.input_dir / "same.parquet"
    write_parquet(same, [("S", utc("2026-11-23 01:00:00"), 1)])
    client.post("/publish", json={"files": ["mixed.parquet", "same.parquet"]})
    r = client.get(url)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "version_stale"

    # 不存在的版号
    r = client.get(
        "/batches/lookup?version=99&source_file=mixed.parquet&start=0&limit=1"
    )
    assert r.status_code == 404 and r.json()["detail"]["code"] == "version_missing"

    # 原档不在清单
    r = client.get(
        "/batches/lookup?version=2&source_file=nope.parquet&start=0&limit=1"
    )
    assert r.status_code == 404 and r.json()["detail"]["code"] == "source_not_found"

    # 路径穿越 / 参数非法
    r = client.get(
        "/batches/lookup?version=2&source_file=../escape.parquet&start=0&limit=1"
    )
    assert r.status_code == 422
    r = client.get(
        "/batches/lookup?version=0&source_file=a.parquet&start=0&limit=1"
    )
    assert r.status_code == 422
    r = client.get(
        "/batches/lookup?version=2&source_file=a.parquet&start=-1&limit=1"
    )
    assert r.status_code == 422
    r = client.get(
        "/batches/lookup?version=2&source_file=a.parquet&start=0&limit=0"
    )
    assert r.status_code == 422


def test_lookup_api_uses_one_manifest_snapshot_per_request(settings, monkeypatch):
    manifest = _publish_mixed(settings)
    client = _client(settings)
    calls = {"n": 0}
    store_cls = ManifestStore
    orig = store_cls.load_current

    def counting_load_current(self):
        calls["n"] += 1
        return orig(self)

    monkeypatch.setattr(store_cls, "load_current", counting_load_current)
    r = client.get(
        f"/batches/lookup?version={manifest.version}"
        "&source_file=mixed.parquet&start=0&limit=4"
    )
    assert r.status_code == 200
    # 整页只在开头核对一次版号（读一次 current.json），之后全用快照
    assert calls["n"] == 1
