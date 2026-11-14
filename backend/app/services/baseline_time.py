"""基线时间推演：快照台账、共享节点裁决、窗口放行与跨台账同步。

职责边界：
- 时间计算全部在 `app.services.dag_time`，这里只负责取数、编排、留痕和写回；
- 快照按「版本键」去重，同一版本只保留一份结果；
- 关键路径窗口的占用在进程内锁里做仲裁，并发请求只会放行一个；
- 推演结果写回基线行、DAG 节点回执与 SLA 规则风险字段，供三张台账协同。
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
from datetime import datetime
from typing import Any, Iterable

from app.services import dag_time
from app.store import store

BASELINE_MODULE = "baseline"
DAG_MODULE = "dag"
SLA_MODULE = "sla"

DEFAULT_LEVEL = 2
DEFAULT_WARN_LEAD_MIN = dag_time.DEFAULT_WARN_LEAD_MIN

_REF_SPLIT = re.compile(r"[,，;；、/\s]+")
_PATH_SPLIT = re.compile(r"(?:->|→|=>|>)")
_LEVEL_MAP = {
    "1": 1, "一级": 1, "一级关键": 1, "高": 1, "high": 1,
    "2": 2, "二级": 2, "二级关键": 2, "中": 2, "medium": 2,
    "3": 3, "三级": 3, "三级关键": 3, "低": 3, "low": 3,
}
_LEVEL_LABEL = {1: "一级", 2: "二级", 3: "三级"}


def _stamp() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _short_hash(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _is_null(value: Any) -> bool:
    return _text(value).lower() in dag_time._NULL_TOKENS


# ---------------------------------------------------------------------------
# 基线配置解析
# ---------------------------------------------------------------------------
def parse_ref_field(raw: Any) -> tuple[list[str], list[str], list[str]]:
    """解析「关键路径 / 依赖范围」字段。

    返回（终点引用, 路径上出现的全部节点引用, 被忽略的占位片段）。
    支持 `DAG-0007` 单节点、逗号分隔多节点，也支持 `DAG-0001>DAG-0003>DAG-0007`
    这种路径写法——路径的最后一段是终点，中间节点同时进入依赖范围。
    """
    if raw is None:
        return [], [], []
    text = str(raw).strip()
    if not text or text.lower() in dag_time._NULL_TOKENS:
        return [], [], []
    endpoints: list[str] = []
    all_refs: list[str] = []
    ignored: list[str] = []
    for segment in _PATH_SPLIT.split(text):
        tokens = [token.strip() for token in _REF_SPLIT.split(segment) if token.strip()]
        path_nodes = [token for token in tokens if token.lower() not in dag_time._NULL_TOKENS]
        ignored.extend(token for token in tokens if token.lower() in dag_time._NULL_TOKENS)
        if path_nodes:
            endpoints.append(path_nodes[-1])
            all_refs.extend(path_nodes)
    # 去重保序
    def dedupe(values: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for value in values:
            if value not in seen:
                seen.add(value)
                ordered.append(value)
        return ordered

    return dedupe(endpoints), dedupe(all_refs), ignored


def parse_level(raw: Any) -> tuple[int | None, str | None]:
    """解析关键路径等级：空值不默认成某一级；非法值给出原因。"""
    if _is_null(raw):
        return None, "关键路径等级为空"
    key = _text(raw).lower()
    if key in _LEVEL_MAP:
        return _LEVEL_MAP[key], None
    return None, f"关键路径等级「{_text(raw)}」无法识别（可用 1/2/3 或 一级/二级/三级）"


def parse_warn_lead(raw: Any) -> tuple[int | None, str | None]:
    if _is_null(raw):
        return DEFAULT_WARN_LEAD_MIN, None
    try:
        value = int(float(_text(raw)))
    except ValueError:
        return None, f"预警提前分钟「{_text(raw)}」不是整数"
    if value <= 0:
        return None, f"预警提前分钟「{value}」不是正数"
    return value, None


def _refs_in_text(text: str, dag_ids: set[str]) -> list[str]:
    """在自由文本里找完整的节点编号，避免 DAG-0001 被 DAG-00011 这类前缀误命中。"""
    hits: list[str] = []
    for node_id in dag_ids:
        pattern = r"(?<![A-Za-z0-9_-])" + re.escape(node_id) + r"(?![0-9])"
        if re.search(pattern, text):
            hits.append(node_id)
    return hits


def resolve_sla_link(entry: dict[str, Any], dag_ids: set[str]) -> tuple[str | None, str | None]:
    """SLA 规则没有显式关联字段时，用「监控对象」里出现的 DAG 节点编号建立关联。

    能唯一命中最长编号才算关联成功；零命中表示不关联，多命中表示歧义，都要说明。
    """
    monitor = _text(entry.get("监控对象"))
    if not monitor:
        return None, "监控对象为空，无法关联 DAG 节点"
    hits = _refs_in_text(monitor, dag_ids)
    if not hits:
        return None, None
    hits.sort(key=lambda node_id: (-len(node_id), node_id))
    if len(hits) > 1:
        return None, f"监控对象「{monitor}」命中多个节点 {hits}，关联存在歧义"
    return hits[0], None


def _baseline_node_refs(
    entry: dict[str, Any], dag_ids: set[str]
) -> tuple[list[str], list[str], list[str], list[str]]:
    """拆出（关键路径终点, 路径上的节点, 依赖范围节点, 被忽略的占位片段）。"""
    endpoint_refs, path_refs, ignored = parse_ref_field(entry.get("关键路径"))
    scope_refs, scope_path_refs, scope_ignored = parse_ref_field(entry.get("依赖范围"))
    legacy_refs = (
        [node_id for node_id in dag_ids if node_id in _text(entry.get("依赖范围"))]
        if not scope_refs
        else []
    )
    return (
        endpoint_refs,
        list(dict.fromkeys(path_refs + scope_path_refs + legacy_refs)),
        scope_refs + legacy_refs,
        ignored + scope_ignored,
    )


# ---------------------------------------------------------------------------
# 推演服务
# ---------------------------------------------------------------------------
class BaselineDerivationService:
    def __init__(self) -> None:
        self._snapshots: list[dict[str, Any]] = []
        self._version_index: dict[str, str] = {}
        self._window_holds: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    # -- 读取 ---------------------------------------------------------------
    def snapshots(self, *, baseline: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        rows = list(reversed(self._snapshots))
        if baseline:
            rows = [row for row in rows if row.get("baseline") == baseline]
        return rows[: max(limit, 1)]

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        for row in self._snapshots:
            if row.get("snapshot_id") == snapshot_id:
                return row
        return None

    def holds(self) -> list[dict[str, Any]]:
        return list(self._window_holds)

    def reset(self) -> None:
        """清空内存快照与版本索引；仅用于测试或本地重置，不影响业务台账。"""
        with self._lock:
            self._snapshots.clear()
            self._version_index.clear()
            self._window_holds.clear()

    def dag_receipts(self, *, baseline: str | None = None) -> list[dict[str, Any]]:
        receipts: list[dict[str, Any]] = []
        for node in store.rows(DAG_MODULE):
            receipt = node.get("基线回执")
            if not isinstance(receipt, dict):
                continue
            if baseline and receipt.get("baseline") != baseline:
                continue
            receipts.append({"node_id": node.get("节点编号"), "node_row_id": node.get("id"), **receipt})
        return receipts

    # -- 推演 ---------------------------------------------------------------
    def derive(
        self,
        *,
        baseline_id: int | None = None,
        baseline_ref: str | None = None,
        scope_id: int | None = None,
        scope_ref: str | None = None,
        now: str | None = None,
        dry_run: bool = False,
    ) -> tuple[dict[str, Any] | None, str]:
        """推演入口：返回（快照, 说明）。

        - 指定 baseline_id：推演单条基线；
        - 指定 scope_id / scope_ref：把该节点及其全部上游当作一条临时基线；
        - 都不指定：推演当前所有未关闭基线，并按共享节点裁决排序；
        - dry_run=True：只返回预演结果，不落快照、不写回三张台账。
        """
        with self._lock:
            try:
                return self._derive_locked(
                    baseline_id=baseline_id,
                    baseline_ref=baseline_ref,
                    scope_id=scope_id,
                    scope_ref=scope_ref,
                    now=now,
                    dry_run=dry_run,
                )
            except _DeriveAbort as abort:
                return None, abort.message

    def _derive_locked(
        self,
        *,
        baseline_id: int | None,
        baseline_ref: str | None,
        scope_id: int | None,
        scope_ref: str | None,
        now: str | None,
        dry_run: bool = False,
    ) -> tuple[dict[str, Any] | None, str]:
        timestamp = now or _stamp()
        dag_rows = store.rows(DAG_MODULE)
        graph = dag_time.build_graph(dag_rows)
        dag_ids = set(graph.nodes)

        targets, notes = _select_targets(
            graph, dag_ids, baseline_id, baseline_ref, scope_id, scope_ref
        )
        if not targets:
            return None, notes[0] if notes else "没有可推演的基线或节点"

        # 观测值：基线「最近产出」（按等级优先）与 SLA「产出时间」/显式节点都并入。
        observations, observation_issues = _collect_observations(targets, dag_ids)

        shared = {
            node_id: [target["label"] for target in targets if node_id in target["scope"]]
            for node_id in dag_ids
        }
        shared = {node_id: owners for node_id, owners in shared.items() if len(owners) > 1}

        results: list[dict[str, Any]] = []
        for target in targets:
            level = target["level"] if target["level"] is not None else DEFAULT_LEVEL
            observations_for_target = {
                node_id: observations[node_id]
                for node_id in target["scope"]
                if node_id in observations
            }
            derivation = dag_time.derive_baseline_time(
                graph,
                endpoint_refs=target["endpoint_refs"],
                scope_refs=target["scope_refs"],
                level=level,
                warn_lead_min=target["warn_lead_min"],
                observations=observations_for_target,
                baseline_code=target["label"],
            )
            results.append({"target": target, "derivation": derivation})

        resolvable = [item for item in results if item["derivation"].derived]
        resolutions = _resolve_shared_nodes(resolvable, shared)
        for item in results:
            target = item["target"]
            constraints = resolutions.get(target["label"], {})
            if not item["derivation"].derived or not constraints:
                continue
            observations_for_target = {
                node_id: observations[node_id]
                for node_id in target["scope"]
                if node_id in observations
            }
            item["derivation"] = dag_time.derive_baseline_time(
                graph,
                endpoint_refs=target["endpoint_refs"],
                scope_refs=target["scope_refs"],
                level=target["level"] if target["level"] is not None else DEFAULT_LEVEL,
                warn_lead_min=target["warn_lead_min"],
                observations=observations_for_target,
                baseline_code=target["label"],
                deadline_constraints=constraints,
            )

        results.sort(key=lambda item: item["target"]["sort_key"])
        # 窗口上下文参与版本键：同样的 DAG/配置，如果窗口已被别的基线占住，
        # 结论不同，不能再命中旧快照。
        batch_labels = {item["target"]["label"] for item in results}
        prior_holds = [
            {
                "baseline": hold["baseline"],
                "path": hold["path"],
                "start": hold["start"],
                "end": hold["end"],
                "level": hold.get("level"),
            }
            for hold in self._window_holds
            if not hold.get("revoked") and hold["baseline"] not in batch_labels
        ]
        prior_holds.sort(key=lambda hold: (hold["baseline"], hold["start"], hold["end"]))
        verdicts, preempted = _arbitrate_windows(results, self._window_holds)

        items_payload: list[dict[str, Any]] = []
        attention: list[dict[str, Any]] = []
        for item in results:
            target = item["target"]
            derivation = item["derivation"]
            target_label = target["label"]
            verdict = verdicts.get(target_label)

            # DAG 版本只覆盖实际参与推演的节点。
            if derivation.derived:
                scope_dag_version = self._dag_scope_version(graph, set(derivation.result["scope_nodes"]))
            else:
                scope_dag_version = None

            config = {
                "endpoint_refs": target["endpoint_refs"],
                "scope_refs": target["scope_refs"],
                "level": target["level"],
                "warn_lead_min": target["warn_lead_min"],
                "baseline_status": target["status"],
                "observations": {
                    node_id: observations[node_id]["raw"]
                    for node_id in sorted(target["scope"])
                    if node_id in observations
                },
            }
            config_hash = _short_hash(config)
            # 共享节点裁决会改写逆推截止，必须进版本键，否则会命中旧结论。
            context = {
                "constraints": (derivation.result or {}).get("shared_deadline_constraints", []),
                "prior_holds": prior_holds,
            }
            context_hash = _short_hash(context)
            version_key = _short_hash(
                {
                    "dag": scope_dag_version or graph.version,
                    "config": config_hash,
                    "context": context_hash,
                    "datasets": dag_time.TIME_BASIS,
                }
            )
            window_key = None
            if derivation.derived:
                window_key = _short_hash(
                    {
                        "critical_path": derivation.result["critical_path"],
                        "commit": derivation.result["commit_offset_min"],
                        "warn_start": derivation.result["warn_start_offset_min"],
                    }
                )
            existing_id = self._version_index.get(version_key)
            deduplicated = existing_id is not None
            snapshot_id = existing_id if deduplicated else "snap-" + version_key[:12]
            report = {
                "baseline": target_label,
                "baseline_id": target["baseline_id"],
                "kind": target["kind"],
                "endpoint_refs": target["endpoint_refs"],
                "scope_refs": target["scope_refs"],
                "level": target["level"],
                "level_label": _LEVEL_LABEL.get(target["level"]),
                "derived": derivation.derived,
                "unresolved_reasons": [] if derivation.derived else derivation.issues,
                "issues": observation_issues,
                "result": derivation.result or None,
                "risk": derivation.result.get("risk") if derivation.derived else None,
                "version_key": version_key,
                "config_hash": config_hash,
                "context_hash": context_hash,
                "dag_version": graph.version,
                "dag_scope_version": scope_dag_version,
                "window_key": window_key,
                "deduplicated": deduplicated,
                "window_verdict": verdict,
                "snapshot_id": snapshot_id,
                "derived_at": timestamp,
            }
            if derivation.derived and verdict and verdict["status"] == "blocked":
                report["risk"] = dict(report["risk"] or {}, window_blocked=True)
            elif derivation.derived:
                report["risk"] = dict(
                    report["risk"] or {},
                    safe_commit=verdict["status"] == "released" if verdict else None,
                )
            items_payload.append(report)
            if derivation.derived and target["level"] is None:
                report["issues"] = report["issues"] + [
                    dag_time.issue(
                        "MISSING_LEVEL",
                        f"关键路径等级缺失，窗口仲裁按{_LEVEL_LABEL[DEFAULT_LEVEL]}级占位",
                        severity="warning",
                        source="baseline",
                    )
                ]
            if not derivation.derived or (verdict and verdict["status"] == "blocked"):
                attention.append(
                    {
                        "baseline": target_label,
                        "snapshot_id": snapshot_id,
                        "reason": (
                            "不可推演"
                            if not derivation.derived
                            else "关键路径窗口被占用，本次不放行"
                        ),
                    }
                )

        snapshot_id = "snap-" + _short_hash(
            {"items": [item["version_key"] for item in items_payload], "dag": graph.version}
        )[:12]
        snapshot = {
            "snapshot_id": snapshot_id,
            "generated_at": timestamp,
            "deduplicated": any(item["deduplicated"] for item in items_payload),
            "dag_version": graph.version,
            "time_basis": dag_time.TIME_BASIS,
            "level_priority": {str(level): _LEVEL_LABEL[level] for level in sorted(_LEVEL_LABEL)},
            "items": items_payload,
            "released": [
                item["baseline"] for item in items_payload
                if item["window_verdict"] and item["window_verdict"]["status"] == "released"
            ],
            "blocked": [
                item["baseline"] for item in items_payload
                if item["window_verdict"] and item["window_verdict"]["status"] == "blocked"
            ],
            "attention": attention,
            "preempted": [
                {"baseline": hold["baseline"], "window_key": hold["window_key"]}
                for hold in preempted
            ],
            "window_holds": _hold_view(self._window_holds),
            "shared_nodes": [
                {
                    "node_id": node_id,
                    "owners": owners,
                    "critical_path_owner": _critical_owner(node_id, resolvable),
                    "resolution": _resolution_for(node_id, resolutions),
                }
                for node_id, owners in sorted(shared.items())
            ],
            "notes": notes,
        }

        # 同一版本只保留一份：整份快照的版本键已存在，就返回既有快照，不重复落账。
        version_seen = all(item["deduplicated"] for item in items_payload)
        existing = self.get_snapshot(snapshot_id) if version_seen else None
        if version_seen and existing is not None:
            keep = {item["baseline"] for item in items_payload}
            snapshot.update(
                {
                    "deduplicated": True,
                    "generated_at": existing["generated_at"],
                    "requested_at": timestamp,
                    "items": [
                        {**item, "deduplicated": True}
                        for item in existing["items"]
                        if item["baseline"] in keep
                    ],
                    "released": [label for label in existing["released"] if label in keep],
                    "blocked": [label for label in existing["blocked"] if label in keep],
                    "attention": [
                        row for row in existing["attention"] if row["baseline"] in keep
                    ],
                    "shared_nodes": existing["shared_nodes"],
                    "notes": existing["notes"],
                }
            )
        elif not dry_run:
            for hold in preempted:
                hold["revoked"] = True
                hold["revoked_by"] = {
                    "baseline": item["baseline"] for item in snapshot["items"] if item["derived"]
                }
            self._snapshots.append(snapshot)
            for item in snapshot["items"]:
                self._version_index.setdefault(item["version_key"], snapshot_id)
            self._apply_side_effects(snapshot, timestamp)
        else:
            snapshot["dry_run"] = True
        summary = self._summary(snapshot)
        return snapshot, summary

    def _dag_scope_version(self, graph: dag_time.Graph, scope: set[str]) -> str:
        payload = [
            [node_id, list(graph.nodes[node_id].upstreams), graph.nodes[node_id].raw_timeout]
            for node_id in sorted(scope)
            if node_id in graph.nodes
        ]
        return "dag-scope-" + _short_hash(payload)[:12]

    def _summary(self, snapshot: dict[str, Any]) -> str:
        derived = [item for item in snapshot["items"] if item["derived"]]
        unresolved = [item for item in snapshot["items"] if not item["derived"]]
        blocked = snapshot["blocked"]
        parts = [
            f"快照 {snapshot['snapshot_id']}：{len(derived)} 条可推演，"
            f"{len(unresolved)} 条不可推演，放行 {len(snapshot['released'])} 条"
        ]
        if blocked:
            parts.append("窗口阻塞：" + "、".join(blocked))
        if unresolved:
            parts.append("不可推演：" + "、".join(item["baseline"] for item in unresolved))
        if snapshot.get("deduplicated"):
            parts.append("同一版本已推演过，本次回放既有快照，未重复落账")
        return "；".join(parts)

    # -- 写回 ---------------------------------------------------------------
    def _apply_side_effects(self, snapshot: dict[str, Any], timestamp: str) -> None:
        """一次推演同步三张台账：基线行、DAG 节点回执、SLA 规则风险。"""
        # 回执按优先级写入：等级高、承诺早的基线先占位，避免低优先级覆盖高优先级。
        ordered = sorted(
            snapshot["items"],
            key=lambda item: (
                item.get("level") or DEFAULT_LEVEL,
                (item.get("result") or {}).get("commit_offset_min", 0),
                item["baseline"],
            ),
        )
        for item in ordered:
            if item["kind"] == "baseline" and item["baseline_id"]:
                self._write_baseline_row(item, snapshot)
            else:
                # 节点级临时推演只出结果，不写基线行/回执，避免覆盖真实配置。
                continue
            self._write_dag_receipts(item, snapshot, timestamp)
            verdict = item.get("window_verdict") or {}
            if item["derived"] and verdict.get("status") == "released" and item["window_key"]:
                result = item["result"]
                # 同一基线的旧占用作废，避免历史窗口一直挡着后来的推演。
                for hold in self._window_holds:
                    if hold["baseline"] == item["baseline"] and not hold.get("revoked"):
                        hold["revoked"] = True
                        hold["revoked_reason"] = "同基线重新推演"
                self._window_holds.append(
                    {
                        "baseline": item["baseline"],
                        "snapshot_id": snapshot["snapshot_id"],
                        "window_key": item["window_key"],
                        "path": sorted(result["critical_path"]),
                        "start": result["warn_start_offset_min"],
                        "end": result["commit_offset_min"],
                        "level": item["level"],
                        "held_at": timestamp,
                    }
                )
                # 只保留最近 200 条占用记录，避免内存台账无限增长。
                if len(self._window_holds) > 200:
                    del self._window_holds[: len(self._window_holds) - 200]
        # 本批次放行/抢占的窗口也要体现在快照里。
        snapshot["window_holds"] = _hold_view(self._window_holds)
        self._write_sla_risks(snapshot, timestamp)

    def _write_baseline_row(self, item: dict[str, Any], snapshot: dict[str, Any]) -> None:
        row = store.find(BASELINE_MODULE, int(item["baseline_id"] or 0))
        if row is None:
            return
        row["时间推演快照"] = snapshot["snapshot_id"]
        row["DAG版本"] = item["dag_version"]
        row["推演时间"] = snapshot["generated_at"]
        row["关键路径等级"] = item["level_label"] or "未配置"
        verdict = item.get("window_verdict") or {}
        row["窗口放行"] = verdict.get("status", "not_applicable")

        if not item["derived"]:
            row["推演状态"] = "不可推演"
            row["不可推演原因"] = [
                reason["message"] for reason in item.get("unresolved_reasons", [])
            ]
            # 清掉上一次推演留下的结论，避免失败时沿用旧承诺。
            for field in (
                "最早可承诺时间",
                "预警起点",
                "模型完成时间",
                "风险摘要",
                "建议承诺时间",
                "建议预警起点",
                "最近产出核对",
                "共享节点裁决",
                "窗口占用",
            ):
                row.pop(field, None)
            row["零松弛节点"] = []
            row["风险等级"] = "未知"
            row["窗口放行"] = "not_applicable"
            row["abnormal"] = True
            return

        result = item["result"]
        risk = item.get("risk") or {}
        verdict = item.get("window_verdict") or {}
        row["推演状态"] = "已推演"
        row["不可推演原因"] = []
        row["最早可承诺时间"] = result["earliest_commit"]
        row["预警起点"] = result["warn_start"]
        row["模型完成时间"] = result["model_finish"]
        # 保留人工维护的「关键路径」配置字段，推演得出的路径单独落一列，便于对照。
        row["推演关键路径"] = " → ".join(result["critical_path"])
        row["推演范围"] = result["scope_nodes"]
        row["零松弛节点"] = result["zero_slack_nodes"]
        row["风险等级"] = risk.get("label")
        row["风险摘要"] = risk.get("headline")
        row["最近产出核对"] = result.get("observed_latest")
        row["共享节点裁决"] = result.get("shared_deadline_constraints", [])
        row["窗口占用"] = (
            {"window_key": item["window_key"], "status": verdict.get("status")}
            if item["window_key"]
            else None
        )

        if verdict.get("status") == "blocked":
            row["建议承诺时间"] = result["earliest_commit"]
            row["建议预警起点"] = result["warn_start"]
            row["abnormal"] = True
            return

        if verdict.get("status") == "released":
            row["承诺时间"] = result["earliest_commit"]
            row["预警时间"] = result["warn_start"]
            row["abnormal"] = risk.get("level") == "high"

    def _write_dag_receipts(
        self, item: dict[str, Any], snapshot: dict[str, Any], timestamp: str
    ) -> None:
        """把推演结论与动作回执落到每个参与节点的清单行上。"""
        if not item["derived"]:
            return
        node_rows = {row["node_id"]: row for row in item["result"]["nodes"]}
        for node_id, node in node_rows.items():
            row = self._dag_row(node_id)
            if row is None:
                continue
            previous = row.get("基线回执")
            previous = previous if isinstance(previous, dict) else {}
            if not _receipt_should_replace(previous, item):
                continue
            row["基线回执"] = {
                **{
                    key: previous[key]
                    for key in ("last_action", "last_action_message", "last_action_at")
                    if key in previous
                },
                "baseline": item["baseline"],
                "snapshot_id": snapshot["snapshot_id"],
                "level": item["level_label"],
                "critical": node["critical"],
                "zero_slack": node["zero_slack"],
                "slack_min": node["slack_min"],
                "earliest_start": node["earliest_start"],
                "earliest_finish": node["earliest_finish"],
                "latest_finish": node["latest_finish"],
                "commit": item["result"]["earliest_commit"],
                "warn_start": item["result"]["warn_start"],
                "window_status": (item.get("window_verdict") or {}).get("status"),
                "observed_finish": node["observed_finish"],
                "observed_source": node["observed_source"],
                "derived_at": timestamp,
            }

    def _write_sla_risks(self, snapshot: dict[str, Any], timestamp: str) -> None:
        """SLA 规则按新摘要重算风险：关联上就写风险字段，关联不上明确写原因。"""
        risk_by_node: dict[str, dict[str, Any]] = {}
        for item in snapshot["items"]:
            # 只有正式基线推演才改写 SLA 风险；节点级临时推演不改台账。
            if item["kind"] != "baseline":
                continue
            if item["derived"]:
                for node_id in item["result"]["scope_nodes"]:
                    risk_by_node.setdefault(node_id, item)
            else:
                continue

        dag_ids = {_text(row.get("节点编号")) for row in store.rows(DAG_MODULE)}
        for rule in store.rows(SLA_MODULE):
            explicit = _text(rule.get("DAG节点") or rule.get("关联节点"))
            node_id = explicit or None
            link_reason = None
            if not node_id:
                node_id, link_reason = resolve_sla_link(rule, dag_ids)
            if node_id is None:
                rule["基线风险"] = None
                rule["风险重算状态"] = link_reason or "未关联 DAG 节点，不参与基线风险重算"
                continue
            item = risk_by_node.get(node_id)
            if item is None:
                rule["基线风险"] = None
                rule["风险重算状态"] = f"节点 {node_id} 没有可推演基线，风险未重算"
                continue
            risk = item["risk"] or {}
            rule["基线风险"] = {
                "baseline": item["baseline"],
                "snapshot_id": item.get("snapshot_id"),
                "level": risk.get("label"),
                "headline": risk.get("headline"),
                "earliest_commit": risk.get("earliest_commit"),
                "warn_start": risk.get("warn_start"),
                "critical_path": risk.get("critical_path"),
                "zero_slack_nodes": risk.get("zero_slack_nodes"),
                "observed_latest": risk.get("observed_latest"),
                "window_blocked": bool(risk.get("window_blocked")),
            }
            rule["风险等级"] = risk.get("label")
            rule["风险重算状态"] = "已按最新基线摘要重算"
            rule["风险快照"] = item.get("snapshot_id")
            rule["风险重算时间"] = timestamp
            if risk.get("level") == "high":
                rule["abnormal"] = True

    def _dag_row(self, node_id: str) -> dict[str, Any] | None:
        for row in store.rows(DAG_MODULE):
            if _text(row.get("节点编号")) == node_id:
                return row
        return None

    # -- 动作回执 -----------------------------------------------------------
    def sync_action_receipt(self, baseline_row: dict[str, Any], action: str, message: str) -> list[str]:
        """基线动作同步到 DAG 节点清单；返回被同步的节点编号。"""
        dag_rows = store.rows(DAG_MODULE)
        dag_ids = {_text(row.get("节点编号")) for row in dag_rows}
        endpoint_refs, path_refs, scope_refs, _ = _baseline_node_refs(baseline_row, dag_ids)
        graph = dag_time.build_graph(dag_rows)
        nodes = dag_time.ancestors(graph.nodes, endpoint_refs + scope_refs) | set(path_refs)
        label = _text(baseline_row.get("基线编号")) or f"baseline#{baseline_row.get('id')}"
        synced: list[str] = []
        for row in dag_rows:
            node_id = _text(row.get("节点编号"))
            if node_id not in nodes:
                continue
            receipt = row.get("基线回执")
            receipt = dict(receipt) if isinstance(receipt, dict) else {
                "baseline": label,
                "snapshot_id": baseline_row.get("时间推演快照"),
                "level": baseline_row.get("关键路径等级"),
            }
            receipt["last_action"] = action
            receipt["last_action_message"] = message
            receipt["last_action_at"] = _stamp()
            receipt["baseline_status"] = baseline_row.get("status")
            row["基线回执"] = receipt
            synced.append(node_id)
        return synced


class _DeriveAbort(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


# ---------------------------------------------------------------------------
# 目标选择与取数
# ---------------------------------------------------------------------------
def _parse_row_level(entry: dict[str, Any]) -> tuple[int | None, str | None]:
    """基线行没有显式等级字段，按「关键路径等级 → 基线名称 → 状态」依次读取。"""
    for field in ("关键路径等级", "基线等级", "优先级", "level"):
        if not _is_null(entry.get(field)):
            return parse_level(entry.get(field))
    name = _text(entry.get("基线名称"))
    match = re.search(r"(一级|二级|三级|[123]级)", name)
    if match:
        return parse_level(match.group(1))
    return None, None


def _scope_nodes(graph: dag_time.Graph, refs: Iterable[str]) -> set[str]:
    return dag_time.ancestors(graph.nodes, refs)


def _collect_observations(
    targets: list[dict[str, Any]], dag_ids: set[str]
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """汇总最近产出：基线行按等级取最晚，SLA 规则以显式/可唯一关联的节点为准。"""
    entries: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []

    baseline_rows = [
        target for target in targets if target["kind"] == "baseline" and target["row"] is not None
    ]
    baseline_rows.sort(key=lambda target: (target["level"] or 99, -target["baseline_id"]))
    for target in baseline_rows:
        value = target["row"].get("最近产出")
        if _is_null(value):
            continue
        node_ids = (
            _refs_in_text(_text(value), dag_ids)
            or list(target["endpoint_refs"])
            or sorted(target["scope"])
        )
        for node_id in node_ids:
            entries.append(
                {
                    "node_id": node_id,
                    "raw": value,
                    "source": f"baseline:{target['target_ref']}",
                }
            )

    for rule in store.rows(SLA_MODULE):
        explicit_node = _text(rule.get("DAG节点") or rule.get("关联节点"))
        if not explicit_node:
            node_id, reason = resolve_sla_link(rule, dag_ids)
            if reason:
                issues.append(
                    dag_time.issue(
                        "AMBIGUOUS_SLA_LINK",
                        f"SLA 规则 {rule.get('规则编号')} {reason}",
                        severity="warning",
                        source="sla",
                    )
                )
            if node_id is None:
                continue
        else:
            node_id = explicit_node
        if node_id not in dag_ids:
            issues.append(
                dag_time.issue(
                    "DANGLING_SLA_NODE",
                    f"SLA 规则 {rule.get('规则编号')} 关联的节点 {node_id} 不在 DAG 中",
                    node_id=node_id,
                    severity="warning",
                    source="sla",
                )
            )
            continue
        entries.append(
            {
                "node_id": node_id,
                "raw": rule.get("产出时间"),
                "source": f"sla:{rule.get('规则编号')}",
            }
        )

    observations = dag_time.build_observations(entries)
    issues.extend(dag_time.observation_warnings(entries))
    return observations, issues


# 回执优先级：已放行 > 窗口阻塞 > 不可推演；同级取等级更高（数字更小）的基线。
_RECEIPT_STATUS_RANK = {"released": 3, "blocked": 2, "not_applicable": 1}
_RECEIPT_LEVEL_RANK = {"一级": 3, "二级": 2, "三级": 1, None: 0}


def _receipt_should_replace(previous: dict[str, Any], item: dict[str, Any]) -> bool:
    """同一节点被多条基线引用时，DAG 回执只保留优先级最高的一条。"""
    if not previous:
        return True
    old_status = _RECEIPT_STATUS_RANK.get(str(previous.get("window_status")), 0)
    new_status = _RECEIPT_STATUS_RANK.get(
        str((item.get("window_verdict") or {}).get("status")), 0
    )
    if new_status != old_status:
        return new_status > old_status
    old_level = _RECEIPT_LEVEL_RANK.get(previous.get("level"), 0)
    new_level = _RECEIPT_LEVEL_RANK.get(item.get("level_label"), 0)
    # 状态、等级都相同时保留先写入的那条；写入顺序已按「等级 → 承诺时间」排好。
    return new_level > old_level


def _select_targets(
    graph: dag_time.Graph,
    dag_ids: set[str],
    baseline_id: int | None,
    baseline_ref: str | None,
    scope_id: int | None,
    scope_ref: str | None,
) -> tuple[list[dict[str, Any]], list[str]]:
    notes: list[str] = []
    targets: list[dict[str, Any]] = []

    def as_baseline(row: dict[str, Any]) -> dict[str, Any]:
        endpoint_refs, path_refs, scope_refs, ignored = _baseline_node_refs(row, dag_ids)
        level, level_reason = _parse_row_level(row)
        warn_lead, warn_reason = parse_warn_lead(row.get("预警提前分钟"))
        label = _text(row.get("基线编号")) or f"baseline#{row.get('id')}"
        if ignored:
            notes.append(f"{label}：字段中的占位片段 {ignored} 已忽略")
        if level_reason:
            notes.append(f"{label}：{level_reason}")
        if warn_reason:
            notes.append(f"{label}：{warn_reason}")
        scope = _scope_nodes(graph, endpoint_refs + scope_refs)
        return {
            "kind": "baseline",
            "target_ref": label,
            "label": label,
            "baseline_id": int(row.get("id") or 0),
            "row": row,
            "status": row.get("status"),
            "endpoint_refs": endpoint_refs,
            "path_refs": path_refs,
            "scope_refs": scope_refs,
            "scope": scope,
            "level": level,
            "warn_lead": warn_lead,
            "warn_lead_min": warn_lead if warn_lead is not None else DEFAULT_WARN_LEAD_MIN,
            # 展示顺序与窗口仲裁保持一致：等级 → 基线编号。
            "sort_key": (level or 99, label),
        }

    def as_scope(row: dict[str, Any]) -> dict[str, Any]:
        node_id = _text(row.get("节点编号"))
        target = {
            "kind": "dag-node",
            "target_ref": node_id,
            "label": node_id,
            "baseline_id": None,
            "row": None,
            "status": row.get("status"),
            "endpoint_refs": [node_id],
            "path_refs": [node_id],
            "scope_refs": [],
            "scope": _scope_nodes(graph, [node_id]),
            "level": DEFAULT_LEVEL,
            "warn_lead": DEFAULT_WARN_LEAD_MIN,
            "warn_lead_min": DEFAULT_WARN_LEAD_MIN,
            "sort_key": (99, int(row.get("id") or 0), node_id),
        }
        return target

    if baseline_id is not None or baseline_ref:
        rows = store.rows(BASELINE_MODULE)
        for row in rows:
            if baseline_id is not None and int(row.get("id") or 0) != baseline_id:
                continue
            if baseline_ref and baseline_ref != _text(row.get("基线编号")):
                continue
            targets.append(as_baseline(row))
        if not targets:
            raise _DeriveAbort(f"基线配置 {baseline_ref or baseline_id} 不存在或已归档")
        return targets, notes

    if scope_id is not None or scope_ref:
        for row in store.rows(DAG_MODULE):
            if scope_id is not None and int(row.get("id") or 0) != scope_id:
                continue
            if scope_ref and scope_ref != _text(row.get("节点编号")):
                continue
            targets.append(as_scope(row))
        if not targets:
            raise _DeriveAbort(f"DAG 节点 {scope_ref or scope_id} 不存在或已归档")
        return targets, notes

    for row in store.rows(BASELINE_MODULE):
        status = _text(row.get("status"))
        if status == "已关闭":
            notes.append(f"{_text(row.get('基线编号'))}：已关闭，跳过推演")
            continue
        targets.append(as_baseline(row))
    if not targets:
        raise _DeriveAbort("没有处于启用状态的基线配置，无法推演")
    return targets, notes


# ---------------------------------------------------------------------------
# 共享节点裁决
# ---------------------------------------------------------------------------
def _critical_owner(node_id: str, resolvable: list[dict[str, Any]]) -> str | None:
    """共享节点归哪个基线：先看谁的零松弛集合包含它，再看关键路径顺序。"""
    owners = [item for item in resolvable if node_id in item["derivation"].result["scope_nodes"]]
    if not owners:
        return None
    zero = [item for item in owners if node_id in item["derivation"].result["zero_slack_nodes"]]
    ranked = sorted(
        zero or owners,
        key=lambda item: (
            item["target"]["level"] or DEFAULT_LEVEL,
            item["derivation"].result["commit_offset_min"],
            item["target"]["label"],
        ),
    )
    return ranked[0]["target"]["label"]


def _resolve_shared_nodes(
    resolvable: list[dict[str, Any]],
    shared: dict[str, list[str]],
) -> dict[str, dict[str, dict[str, Any]]]:
    """裁决共享节点：等级高者先把窗口钉死，低等级基线只能继承更早的截止。"""
    if not resolvable:
        return {}
    scope_of = {
        item["target"]["label"]: set(item["derivation"].result["scope_nodes"])
        for item in resolvable
    }
    resolutions: dict[str, dict[str, dict[str, Any]]] = {}
    ordered = sorted(
        resolvable,
        key=lambda item: (
            item["target"]["level"] or DEFAULT_LEVEL,
            item["derivation"].result["commit_offset_min"],
            item["target"]["label"],
        ),
    )
    for owner in ordered:
        label = owner["target"]["label"]
        for node_id, node_owners in shared.items():
            if label not in node_owners:
                continue
            for other in ordered:
                other_label = other["target"]["label"]
                if other_label == label or other_label not in node_owners:
                    continue
                if node_id not in scope_of.get(other_label, set()):
                    continue
                if (other["target"]["level"] or DEFAULT_LEVEL) < (owner["target"]["level"] or DEFAULT_LEVEL):
                    continue
                if (other["target"]["level"] or DEFAULT_LEVEL) == (owner["target"]["level"] or DEFAULT_LEVEL) and (
                    other["derivation"].result["commit_offset_min"],
                    other_label,
                ) < (
                    owner["derivation"].result["commit_offset_min"],
                    label,
                ):
                    continue
                # 共享节点的最晚完成直接采用裁决方自己在该节点上的最晚完成时间，
                # 也就是「关键路径等级更高者的时间口径」；setdefault 保证先写入的
                # 高优先级裁决不会被后来者覆盖。
                owner_node = next(
                    (
                        node
                        for node in owner["derivation"].result["nodes"]
                        if node["node_id"] == node_id
                    ),
                    None,
                )
                if owner_node is None:
                    continue
                resolutions.setdefault(other_label, {}).setdefault(
                    node_id,
                    {
                        "latest_finish_offset": owner_node["latest_finish_offset_min"],
                        "source": label,
                    },
                )
    return resolutions


def _resolution_for(
    node_id: str, resolutions: dict[str, dict[str, dict[str, Any]]]
) -> dict[str, Any] | None:
    for label, constraints in resolutions.items():
        if node_id in constraints:
            return {"owner": constraints[node_id]["source"], "constrained_baseline": label}
    return None


# ---------------------------------------------------------------------------
# 关键路径窗口：冲突对只放行一个
# ---------------------------------------------------------------------------
def _overlap_key(item: dict[str, Any]) -> tuple[list[str], int, int] | None:
    if not item["derivation"].derived:
        return None
    result = item["derivation"].result
    return (
        sorted(result["critical_path"]),
        result["warn_start_offset_min"],
        result["commit_offset_min"],
    )


def _windows_overlap(
    path_a: list[str],
    start_a: int,
    end_a: int,
    path_b: list[str],
    start_b: int,
    end_b: int,
) -> str | None:
    """两条关键路径在同一时间窗口内是否冲突；冲突时返回共享路径描述。"""
    if not (start_a < end_b and start_b < end_a):
        return None
    ends_a = {path_a[i + 1] for i in range(len(path_a) - 1)}
    ends_b = {path_b[i + 1] for i in range(len(path_b) - 1)}
    if not (set(path_a) & set(path_b)) and not (ends_a & ends_b):
        return None
    return "+".join(path_a)


def _conflict(a: dict[str, Any], b: dict[str, Any]) -> str | None:
    key_a = _overlap_key(a)
    key_b = _overlap_key(b)
    if not key_a or not key_b:
        return None
    return _windows_overlap(*key_a, *key_b)


def _hold_conflict(item: dict[str, Any], hold: dict[str, Any]) -> str | None:
    """同批次之外的窗口占用：已被其他基线在并发请求里先一步拿走。"""
    key = _overlap_key(item)
    if not key:
        return None
    return _windows_overlap(*key, hold["path"], hold["start"], hold["end"])


def _priority_key(label: str, level: int | None, commit: int) -> tuple[int, int, str]:
    return (level or DEFAULT_LEVEL, commit, label)


def _arbitrate_windows(
    results: list[dict[str, Any]],
    prior_holds: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """两个基线并发占用同一关键路径窗口时，只放行优先级更高的那个。

    `prior_holds` 是其他并发推演批次已放行的窗口：
    - 新请求优先级更低/相同 → 直接 blocked；
    - 新请求优先级更高 → 抢占窗口，旧占用标记 revoked 并交回调用方处理。
    返回（判决表, 被抢占的占用列表）。
    """
    verdicts: dict[str, dict[str, Any]] = {}
    revoked: list[dict[str, Any]] = []
    batch_labels = {item["target"]["label"] for item in results}
    active_holds = [
        hold
        for hold in (prior_holds or [])
        if hold.get("baseline") not in batch_labels and not hold.get("revoked")
    ]
    ordered = sorted(
        results,
        key=lambda item: (
            item["target"]["level"] or DEFAULT_LEVEL,
            item["derivation"].result.get("commit_offset_min", 0),
            item["target"]["label"],
        ),
    )
    for index, item in enumerate(ordered):
        label = item["target"]["label"]
        if not item["derivation"].derived:
            verdicts[label] = {"status": "not_applicable", "reason": "不可推演，不参与窗口仲裁"}
            continue
        item_priority = _priority_key(
            label,
            item["target"]["level"],
            item["derivation"].result["commit_offset_min"],
        )
        hold_conflict = None
        hold_owner = None
        for hold in active_holds:
            window = _hold_conflict(item, hold)
            if not window:
                continue
            if item_priority < _priority_key(
                hold["baseline"], hold.get("level"), hold["end"]
            ):
                # 新请求优先级更高：抢占旧窗口，旧占用转为 revoked。
                hold["revoked"] = True
                revoked.append(hold)
                continue
            hold_conflict = window
            hold_owner = hold
            break
        if hold_conflict and hold_owner:
            verdicts[label] = {
                "status": "blocked",
                "released": False,
                "blocked_by": hold_owner["baseline"],
                "window": hold_conflict,
                "reason": (
                    f"关键路径窗口 {hold_conflict} 已被并发推演 {hold_owner['baseline']} 占用"
                    f"（截止 {format_cycle(hold_owner['end'])}），本次不放行"
                ),
            }
            continue
        for earlier in ordered[:index]:
            if not earlier["derivation"].derived:
                continue
            conflict = _conflict(earlier, item)
            if conflict:
                verdicts[label] = {
                    "status": "blocked",
                    "released": False,
                    "blocked_by": earlier["target"]["label"],
                    "window": conflict,
                    "reason": (
                        f"关键路径窗口 {conflict} 已被 {earlier['target']['label']} "
                        f"占用（{earlier['derivation'].result['commit_offset_min']} 分钟截止），本次不放行"
                    ),
                }
                break
        else:
            verdicts[label] = {
                "status": "released",
                "released": True,
                "window": "+".join(sorted(item["derivation"].result["critical_path"])),
                "reason": "窗口空闲，已放行",
            }
    return verdicts, revoked


def _hold_view(holds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """窗口占用的可读视图：只暴露追溯需要的字段。"""
    return [
        {
            "baseline": hold["baseline"],
            "window_key": hold["window_key"],
            "start": format_cycle(hold["start"]),
            "end": format_cycle(hold["end"]),
            "revoked": bool(hold.get("revoked")),
            "revoked_reason": hold.get("revoked_reason"),
        }
        for hold in holds
        if not hold.get("revoked") or hold.get("revoked_reason")
    ]

def format_cycle(offset: int) -> str:
    """偏移分钟 → HH:MM，用于占用说明。"""
    return dag_time.format_clock(offset) or str(offset)


# ---------------------------------------------------------------------------
# 调用链自描述：给前端与排障用，说明 DAG → 基线 → SLA 每一步取什么数
# ---------------------------------------------------------------------------
CHAIN_STEPS: list[dict[str, Any]] = [
    {
        "step": 1,
        "stage": "dag",
        "name": "读取依赖图节点",
        "reads": ["dag.节点编号", "dag.节点名称", "dag.上游节点", "dag.超时分钟", "dag.id"],
        "writes": [],
        "rules": [
            "上游字段按逗号/分号/顿号/箭头切分，占位片段「无」「—」直接忽略",
            "同一节点编号重复登记只取第一条，并记 DUPLICATE_NODE",
        ],
        "fail_codes": ["EMPTY_DAG", "MISSING_NODE_ID", "DUPLICATE_NODE"],
    },
    {
        "step": 2,
        "stage": "dag",
        "name": "构建有向图并查环",
        "reads": ["dag.上游节点"],
        "writes": [],
        "rules": [
            "Kahn 拓扑排序，剩余节点用 DFS 找环",
            "悬空上游（未登记节点）与自环都记为 error",
        ],
        "fail_codes": ["DAG_CYCLE", "SELF_LOOP", "MISSING_UPSTREAM", "UNORDERED_SCOPE"],
    },
    {
        "step": 3,
        "stage": "baseline",
        "name": "解析基线配置",
        "reads": ["baseline.基线编号", "baseline.关键路径", "baseline.依赖范围", "baseline.最近产出", "baseline.预警提前分钟"],
        "writes": [],
        "rules": [
            "关键路径支持单节点、逗号列表和 A>B>C 路径写法；路径末段是终点",
            "终点 + 依赖范围一起求上游祖先闭包，作为推演范围",
            "等级读取顺序：关键路径等级 → 基线名称中的「一级/二级/三级」，缺失不默认成某一级",
        ],
        "fail_codes": ["MISSING_ENDPOINT", "DANGLING_SCOPE", "MISSING_LEVEL", "INVALID_WARN_LEAD"],
    },
    {
        "step": 4,
        "stage": "dag",
        "name": "正推最早开始/完成",
        "reads": ["dag.超时分钟"],
        "writes": [],
        "rules": [
            "批次从 06:00 起算，偏移分钟直接累加",
            "节点最早开始 = 所有上游最早完成的最大值",
            "超时分钟缺失或非正数直接判定不可推演，不按 0 处理",
        ],
        "fail_codes": ["MISSING_TIMEOUT"],
    },
    {
        "step": 5,
        "stage": "baseline",
        "name": "逆推最晚完成与松弛",
        "reads": ["dag.超时分钟", "baseline.关键路径"],
        "writes": [],
        "rules": [
            "以理论完成时间为截止逆推；终点取范围内无下游节点",
            "松弛 = 最晚开始 - 最早开始；0 松弛即关键路径节点",
            "共享节点裁决只收紧截止（取更早值），绝不放宽",
        ],
        "fail_codes": [],
    },
    {
        "step": 6,
        "stage": "baseline+ sla",
        "name": "并入最近产出",
        "reads": ["baseline.最近产出", "sla.产出时间", "sla.监控对象", "sla.DAG节点"],
        "writes": [],
        "rules": [
            "基线最近产出落到关键路径终点；SLA 产出时间落到其关联节点，以较晚观测为准",
            "日期缺 HH:MM 无法参与推演，只记 warning，不伪装成 00:00",
        ],
        "fail_codes": ["INVALID_OBSERVATION", "AMBIGUOUS_SLA_LINK", "DANGLING_SLA_NODE"],
    },
    {
        "step": 7,
        "stage": "baseline",
        "name": "计算承诺与预警",
        "reads": ["baseline.预警提前分钟"],
        "writes": ["baseline.最早可承诺时间", "baseline.预警起点", "baseline.风险摘要"],
        "rules": [
            "最早可承诺 = max(理论完成, 最近产出观测完成)",
            "预警起点 = 最早可承诺 - 预警提前分钟，未配置时默认 30 分钟",
        ],
        "fail_codes": ["INVALID_WARN_LEAD"],
    },
    {
        "step": 8,
        "stage": "baseline",
        "name": "共享节点裁决与窗口放行",
        "reads": ["baseline.关键路径等级", "baseline.最早可承诺时间"],
        "writes": ["baseline.共享节点裁决", "baseline.窗口占用"],
        "rules": [
            "同一节点被多条基线引用时，按等级 → 最早承诺时间 → 基线编号排序裁决",
            "同一条关键路径窗口重叠时只放行优先级最高的一条，其余标记 blocked",
        ],
        "fail_codes": [],
    },
    {
        "step": 9,
        "stage": "dag+baseline+sla",
        "name": "留痕与回写",
        "reads": [],
        "writes": [
            "推演快照（只保留一份/版本）",
            "dag.基线回执（每条节点行）",
            "sla.基线风险 / 风险重算状态",
            "baseline.承诺时间 / 预警时间（仅放行的基线）",
        ],
        "rules": [
            "快照版本键 = DAG 范围内版本 + 基线配置哈希 + 窗口上下文 + 时间口径；重复推演直接返回旧快照",
            "DAG 回执按「等级 → 承诺时间」顺序写入，优先级更高的基线占位；已放行 > 阻塞 > 不可推演",
            "动作回执同步到 DAG 节点清单，并触发 SLA 风险重算",
        ],
        "fail_codes": [],
    },
]


def derivation_chain() -> dict[str, Any]:
    """把调用链、时间口径与快照字段一并返回，前端据此展示流程图。"""
    counts = {
        "dag": len(store.rows(DAG_MODULE)),
        "baseline": len(store.rows(BASELINE_MODULE)),
        "sla": len(store.rows(SLA_MODULE)),
    }
    return {
        "chain": CHAIN_STEPS,
        "time_basis": dag_time.TIME_BASIS,
        "counts": counts,
        "snapshot_fields": [
            "snapshot_id",
            "generated_at",
            "dag_version",
            "time_basis",
            "items[].version_key",
            "items[].config_hash",
            "items[].dag_scope_version",
            "items[].scope_nodes",
            "items[].result.nodes",
            "items[].result.observed_nodes",
            "items[].window_verdict",
        ],
    }


# 路由层共用的单例：快照、版本索引都在这里，跨模块读写同一份内存台账。
# 注意：store 是模块级导入，测试里 monkeypatch 的是本模块的 store 名字，
# 单例本身不缓存 store，因此每次推演都会读到最新的台账对象。
derivation_service = BaselineDerivationService()
