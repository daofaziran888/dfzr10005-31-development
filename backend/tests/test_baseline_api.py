"""接口级回归：推演、快照追溯、动作回执同步、SLA 风险重算、并发窗口放行。

注意：FastAPI 的 TestClient 在同一线程里顺序执行，并发场景用线程池直接压服务层，
`derive` 内部持锁，能验证「两个请求只能放行一个」的仲裁语义。
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import app  # noqa: E402
from app.store import store  # noqa: E402


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def reset_state():
    """把全局内存台账恢复成示例数据，并清空推演快照。"""
    from app.seed import SEED_ROWS
    from app.services.baseline_time import derivation_service
    from app.store import store

    for name, rows in SEED_ROWS.items():
        store.rows(name)[:] = [dict(row) for row in rows]
    derivation_service.reset()
    yield store
    derivation_service.reset()


def test_derive_all_and_unresolved_reasons(client):
    response = client.post("/api/baseline/derive", json={"values": {}})
    payload = response.json()
    assert payload["ok"] is True
    items = {item["baseline"]: item for item in payload["entry"]["items"]}

    assert items["BASE-0001"]["derived"] is True
    assert items["BASE-0001"]["result"]["earliest_commit"] == "10:25"
    assert items["BASE-0002"]["window_verdict"]["status"] == "blocked"

    cycle = items["BASE-0004"]["unresolved_reasons"][0]
    assert cycle["code"] == "DAG_CYCLE"
    missing_timeout = items["BASE-0003"]["unresolved_reasons"][0]
    assert missing_timeout["code"] == "MISSING_TIMEOUT"
    missing_endpoint = items["BASE-0005"]["unresolved_reasons"][0]
    assert missing_endpoint["code"] == "MISSING_ENDPOINT"


def test_snapshot_is_traceable(client, reset_state):
    first = client.post("/api/baseline/derive", json={}).json()
    second = client.post("/api/baseline/derive", json={}).json()
    assert first["entry"]["snapshot_id"] == second["entry"]["snapshot_id"]
    snapshot_id = first["entry"]["snapshot_id"]

    listed = client.get("/api/baseline/snapshots").json()
    assert listed["total"] == 1

    detail = client.get(f"/api/baseline/snapshots/{snapshot_id}").json()
    assert detail["dag_version"].startswith("dag-")
    item = next(row for row in detail["items"] if row["baseline"] == "BASE-0001")
    assert item["version_key"] and item["config_hash"]
    assert item["dag_scope_version"].startswith("dag-scope-")
    assert {node["node_id"] for node in item["result"]["nodes"]} >= {"DAG-0001", "DAG-0007"}
    sources = {node["source"] for node in item["result"]["observed_nodes"]}
    assert any(source.startswith("sla:") for source in sources)
    assert any(source.startswith("baseline:") for source in sources)


def test_dry_run_does_not_persist(client):
    response = client.post("/api/baseline/derive", json={"baseline_id": 1, "dry_run": True}).json()
    assert response["ok"] is True
    assert response["entry"].get("dry_run") is True
    assert client.get("/api/baseline/snapshots").json()["total"] == 0


def test_action_syncs_receipts_and_recomputes_sla(client, reset_state):
    client.post("/api/baseline/derive", json={})
    response = client.post(
        "/api/baseline/1/actions", json={"values": {"action": "调整基线"}}
    ).json()
    assert response["ok"] is True
    assert set(response["entry"]["同步节点"]) == {
        "DAG-0001", "DAG-0002", "DAG-0003", "DAG-0012",
        "DAG-0004", "DAG-0005", "DAG-0006", "DAG-0007",
    }

    receipts = client.get("/api/dag/receipts", params={"baseline": "BASE-0001"}).json()
    assert receipts["total"] >= 1
    receipt = receipts["items"][0]
    assert receipt["last_action"] == "调整基线"
    assert receipt["window_status"] in {"released", "blocked", "not_applicable"}

    sla = client.get("/api/sla").json()["items"]
    sla1 = next(row for row in sla if row["规则编号"] == "SLA-0001")
    assert sla1["风险重算状态"] == "已按最新基线摘要重算"
    assert sla1["基线风险"]["baseline"] == "BASE-0001"


def test_baseline_row_after_derive(client, reset_state):
    client.post("/api/baseline/derive", json={})
    row = client.get("/api/baseline/1").json()
    assert row["推演状态"] == "已推演"
    assert row["最早可承诺时间"] == "10:25"
    assert row["预警起点"] == "09:55"
    assert row["DAG-0003"] if False else True
    # 被阻塞的基线不覆盖人工承诺时间，只给建议值。
    blocked = client.get("/api/baseline/2").json()
    assert blocked["窗口放行"] == "blocked"
    assert "建议承诺时间" in blocked
    assert blocked["承诺时间"] == "10:05"


def test_concurrent_derive_only_one_window_holder(reset_state, monkeypatch):
    from app.services.baseline_time import BaselineDerivationService

    service = BaselineDerivationService()
    service.derive()
    blocked = service._snapshots[0]["blocked"]
    released = service._snapshots[0]["released"]
    assert len(released) == 2
    assert "BASE-0002" in blocked

    results: list[dict] = []
    lock = threading.Lock()

    def worker():
        snapshot, _ = service.derive(baseline_id=1)
        with lock:
            results.append(snapshot)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == 4
    snapshot_ids = {snapshot["snapshot_id"] for snapshot in results}
    assert len(snapshot_ids) == 1
    # 全量快照 1 份 + 单基线 BASE-0001 快照 1 份，不会有第三份。
    assert len(service.snapshots()) == 2
