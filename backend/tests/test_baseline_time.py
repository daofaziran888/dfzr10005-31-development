"""基线时间推演的回归测试。

用纯函数覆盖 DAG 解析、正推/逆推、不可推演判定；用服务实例覆盖快照去重、
共享节点裁决与窗口放行。测试之间通过重新构造 Store 数据互不影响。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import dag_time  # noqa: E402
from app.services.baseline_time import BaselineDerivationService  # noqa: E402
from app.store import Store, store  # noqa: E402


def dag_rows():
    return [
        {"id": 1, "节点编号": "A", "节点名称": "接入", "上游节点": "—", "超时分钟": "30"},
        {"id": 2, "节点编号": "B", "节点名称": "清洗", "上游节点": "A", "超时分钟": "20"},
        {"id": 3, "节点编号": "C", "节点名称": "汇总", "上游节点": "A,B", "超时分钟": "40"},
    ]


def graph_of(rows):
    return dag_time.build_graph(rows)


# ---------------------------------------------------------------------------
# 图解析
# ---------------------------------------------------------------------------
def test_parse_upstreams_handles_placeholders_and_separators():
    valid, ignored = dag_time.parse_upstreams("A，B;C、D A 无")
    assert valid == ["A", "B", "C", "D"]
    assert ignored == ["无"]


def test_missing_timeout_is_not_zero():
    value, reason = dag_time.parse_timeout("任务依赖编排样例1")
    assert value is None
    assert "不是有效整数" in reason

    value, reason = dag_time.parse_timeout("0")
    assert value is None
    assert "不是正数" in reason


def test_cycle_detection_reports_path():
    graph = graph_of(
        [
            {"id": 1, "节点编号": "A", "上游节点": "B", "超时分钟": "10"},
            {"id": 2, "节点编号": "B", "上游节点": "A", "超时分钟": "10"},
        ]
    )
    codes = [item["code"] for item in graph.issues]
    assert "DAG_CYCLE" in codes
    cycle = next(item for item in graph.issues if item["code"] == "DAG_CYCLE")
    assert "A" in cycle["message"] and "B" in cycle["message"]


def test_dangling_upstream_is_error():
    graph = graph_of([{"id": 1, "节点编号": "A", "上游节点": "ZZ", "超时分钟": "10"}])
    assert any(item["code"] == "MISSING_UPSTREAM" for item in graph.issues)


# ---------------------------------------------------------------------------
# 推演
# ---------------------------------------------------------------------------
def derive(rows, **kwargs):
    graph = graph_of(rows)
    defaults = dict(
        endpoint_refs=["C"],
        scope_refs=[],
        level=1,
        warn_lead_min=30,
        observations={},
        baseline_code="TEST",
    )
    defaults.update(kwargs)
    return dag_time.derive_baseline_time(graph, **defaults)


def test_forward_and_backward_pass():
    result = derive(dag_rows()).result
    # A 06:00-06:30, B 06:30-06:50, C 06:50-07:30
    assert result["model_finish"] == "07:30"
    assert result["earliest_commit"] == "07:30"
    assert result["warn_start"] == "07:00"
    assert result["critical_path"] == ["A", "B", "C"]
    assert result["zero_slack_nodes"] == ["A", "B", "C"]


def test_parallel_branch_has_slack():
    rows = dag_rows() + [
        {"id": 4, "节点编号": "D", "节点名称": "旁路", "上游节点": "A", "超时分钟": "10"},
    ]
    graph = graph_of(rows)
    result = dag_time.derive_baseline_time(
        graph,
        endpoint_refs=["C", "D"],
        scope_refs=[],
        level=1,
        warn_lead_min=30,
        observations={},
        baseline_code="TEST",
    ).result
    by_id = {node["node_id"]: node for node in result["nodes"]}
    # D 06:30-06:40，最晚 07:30 完成（与 C 同时作为终点），松弛 50 分钟。
    assert by_id["D"]["slack_min"] == 50
    assert "D" not in result["zero_slack_nodes"]


def test_missing_timeout_blocks_derivation():
    rows = dag_rows()
    rows[1]["超时分钟"] = "—"
    derivation = derive(rows)
    assert derivation.derived is False
    assert derivation.issues[0]["code"] == "MISSING_TIMEOUT"


def test_missing_endpoint_blocks_derivation():
    derivation = derive(dag_rows(), endpoint_refs=[])
    assert derivation.derived is False
    assert derivation.issues[0]["code"] == "MISSING_ENDPOINT"


def test_cycle_blocks_derivation():
    rows = dag_rows() + [
        {"id": 4, "节点编号": "X", "上游节点": "Y", "超时分钟": "10"},
        {"id": 5, "节点编号": "Y", "上游节点": "X", "超时分钟": "10"},
    ]
    derivation = derive(rows, endpoint_refs=["X"])
    assert derivation.derived is False
    assert derivation.issues[0]["code"] == "DAG_CYCLE"


def test_observation_pushes_commit_later_and_flags_breach():
    derivation = derive(dag_rows(), observations={"C": {"minutes": 8 * 60, "source": "sla:SLA-1"}})
    assert derivation.derived is True
    result = derivation.result
    assert result["model_finish"] == "07:30"
    assert result["earliest_commit"] == "08:00"
    assert result["risk"]["level"] == "high"
    assert result["risk"]["node_breaches"] == ["C"]


def test_invalid_observation_warns_but_does_not_zero():
    warnings = dag_time.observation_warnings([{"node_id": "C", "raw": "2026-11-13", "source": "sla:1"}])
    assert warnings[0]["code"] == "INVALID_OBSERVATION"
    assert dag_time.build_observations([{"node_id": "C", "raw": "2026-11-13"}]) == {}


def test_shared_deadline_constraint_tightens_only():
    constraints = {"C": {"latest_finish_offset": 60, "source": "OTHER"}}
    result = derive(dag_rows(), deadline_constraints=constraints).result
    by_id = {node["node_id"]: node for node in result["nodes"]}
    assert by_id["C"]["latest_finish"] == "07:00"
    assert result["shared_deadline_constraints"] == [
        {"node_id": "C", "latest_finish": "07:00", "source": "OTHER"}
    ]


# ---------------------------------------------------------------------------
# 服务层：去重、共享节点、窗口
# ---------------------------------------------------------------------------
@pytest.fixture()
def fresh_store(monkeypatch):
    isolated = Store()
    monkeypatch.setattr("app.services.baseline_time.store", isolated)
    isolated.rows("dag")[:] = dag_rows()
    isolated.rows("baseline")[:] = [
        {"id": 1, "status": "监控中", "基线编号": "BASE-A", "基线名称": "A 基线", "关键路径等级": "一级",
         "关键路径": "C", "依赖范围": "A,B", "最近产出": "2026-11-13 07:00", "预警提前分钟": "30"},
        {"id": 2, "status": "监控中", "基线编号": "BASE-B", "基线名称": "B 基线", "关键路径等级": "二级",
         "关键路径": "C", "依赖范围": "A,B", "最近产出": "2026-11-13 07:10", "预警提前分钟": "30"},
    ]
    isolated.rows("sla")[:] = [
        {"id": 1, "规则编号": "SLA-1", "监控对象": "汇总 C", "产出时间": "2026-11-13 07:05"},
    ]
    return isolated


def test_repeat_derive_keeps_one_snapshot(fresh_store):
    service = BaselineDerivationService()
    first, _ = service.derive()
    second, message = service.derive()
    assert first["snapshot_id"] == second["snapshot_id"]
    assert second["deduplicated"] is True
    assert len(service.snapshots()) == 1
    assert "未重复落账" in message


def test_higher_level_baseline_wins_shared_node(fresh_store):
    service = BaselineDerivationService()
    snapshot, _ = service.derive()
    by_label = {item["baseline"]: item for item in snapshot["items"]}
    assert by_label["BASE-A"]["window_verdict"]["status"] == "released"
    assert by_label["BASE-B"]["window_verdict"]["status"] == "blocked"
    assert by_label["BASE-B"]["window_verdict"]["blocked_by"] == "BASE-A"
    shared = next(row for row in snapshot["shared_nodes"] if row["node_id"] == "C")
    assert shared["critical_path_owner"] == "BASE-A"


def test_action_receipt_syncs_dag_nodes(fresh_store):
    service = BaselineDerivationService()
    service.derive()
    row = fresh_store.find("baseline", 1)
    synced = service.sync_action_receipt(row, "启用监控", "基线配置已启用监控")
    assert set(synced) == {"A", "B", "C"}
    receipt = next(item for item in service.dag_receipts() if item["node_id"] == "C")
    assert receipt["last_action"] == "启用监控"
    assert receipt["baseline"] in {"BASE-A", "BASE-B"}


def test_sla_risk_recomputed(fresh_store):
    service = BaselineDerivationService()
    service.derive()
    rule = fresh_store.rows("sla")[0]
    assert rule["风险重算状态"] == "已按最新基线摘要重算"
    assert rule["基线风险"]["baseline"] in {"BASE-A", "BASE-B"}


def test_cross_batch_window_preemption(fresh_store):
    """先到的低等级基线占用窗口；后到的高等级基线把它抢占。"""
    service = BaselineDerivationService()
    first, _ = service.derive(baseline_id=2)
    assert first["items"][0]["window_verdict"]["status"] == "released"

    second, _ = service.derive(baseline_id=1)
    assert second["items"][0]["window_verdict"]["status"] == "released"
    assert [row["baseline"] for row in second["preempted"]] == ["BASE-B"]
    holds = {hold["baseline"]: hold for hold in service.holds()}
    assert holds["BASE-B"]["revoked"] is True
    assert holds["BASE-B"]["revoked_by"] == {"baseline": "BASE-A"}


def test_same_level_later_request_is_blocked(fresh_store):
    """同级并发：后到的请求不抢占，直接不放行。"""
    service = BaselineDerivationService()
    fresh_store.find("baseline", 1)["关键路径等级"] = "二级"
    service.derive(baseline_id=1)
    second, _ = service.derive(baseline_id=2)
    verdict = second["items"][0]["window_verdict"]
    assert verdict["status"] == "blocked"
    assert verdict["blocked_by"] == "BASE-A"


def test_sla_link_rejects_ambiguous_monitor():
    from app.services.baseline_time import resolve_sla_link

    node, reason = resolve_sla_link(
        {"监控对象": "核心报表 DAG-0001 与 DAG-0011"}, {"DAG-0001", "DAG-0011"}
    )
    assert node is None
    assert "歧义" in reason


def test_sla_link_matches_node_id_not_prefix():
    from app.services.baseline_time import resolve_sla_link

    node, reason = resolve_sla_link({"监控对象": "看板 DAG-0009"}, {"DAG-0009", "DAG-00090"})
    assert (node, reason) == ("DAG-0009", None)
