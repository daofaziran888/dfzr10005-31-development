"""DAG → 基线的时间推演引擎。

这一层只做确定性计算，不读写业务台账：

1. 把 DAG 节点清单解析成有向图（节点 → 上游），显式收集数据质量问题；
2. 找出环与悬空上游，环内的节点不参与拓扑排序；
3. 对基线的依赖范围求祖先闭包，按 Kahn 拓扑序做最早开始/最早完成正推；
4. 以理论完成时间（不含最近产出校准）为截止做最晚完成逆推，得到松弛分钟；
5. 把最近产出（基线「最近产出」、SLA「产出时间」）作为已观测完成时间并入，
   得到「最早可承诺时间」与风险摘要。

所有异常都以结构化 issue 返回，调用方据此给出不可推演原因，绝不把缺失数据当 0。
"""
from __future__ import annotations

import heapq
import re
from dataclasses import dataclass, field
from typing import Any, Iterable

# 自然日批次的起算时刻：所有偏移分钟都相对这一时刻，时钟字符串 = CYCLE_START + 偏移。
CYCLE_START_MIN = 6 * 60
DEFAULT_WARN_LEAD_MIN = 30
LEVEL_VALUES = (1, 2, 3)

TIME_BASIS: dict[str, str] = {
    "cycle": "按自然日一个 DAG 批次计算，批次从 06:00 起算；1 个时间单位 = 1 分钟",
    "clock": "所有时刻都是「06:00 + 偏移分钟」的 24 小时制时钟值，同日口径",
    "timezone": "Asia/Shanghai（服务端本地时区）",
    "priority": "关键路径等级 1 > 2 > 3（数字越小优先级越高）；同级按最早承诺时间升序，再按基线编号升序",
    "slack_basis": "松弛以理论模型完成时间（不含最近产出校准）为截止做逆推；0 松弛即关键路径节点",
    "commit_basis": "最早可承诺时间 = max(理论完成时间, 最近产出观测完成时间)，避免承诺早于事实",
    "warn_basis": f"预警起点 = 最早可承诺时间 - 预警提前分钟（基线未填时默认 {DEFAULT_WARN_LEAD_MIN} 分钟）",
}

LEVEL_BUFFER_LABELS = {1: "一级关键", 2: "二级关键", 3: "三级关键"}

_UPSTREAM_SPLIT = re.compile(r"[,，;；、/\s]+")
_TIMEOUT_RE = re.compile(r"^(\d+)\s*(?:分钟|min|m)?$", re.IGNORECASE)
_CLOCK_RE = re.compile(r"(?<!\d)([01]?\d|2[0-3]):([0-5]\d)")
_NULL_TOKENS = {"", "无", "—", "-", "--", "none", "null", "n/a", "nan"}


# ---------------------------------------------------------------------------
# 基础解析
# ---------------------------------------------------------------------------
def issue(
    code: str,
    message: str,
    *,
    node_id: str | None = None,
    severity: str = "error",
    source: str = "dag",
) -> dict[str, Any]:
    """统一的问题结构：code 供程序判断，message 供人阅读。"""
    return {
        "code": code,
        "severity": severity,
        "source": source,
        "node_id": node_id,
        "message": message,
    }


def parse_upstreams(raw: Any) -> tuple[list[str], list[str]]:
    """解析上游节点列表，返回（有效节点编号, 被忽略的无效片段）。"""
    if raw is None:
        return [], []
    text = str(raw).strip()
    if text.lower() in _NULL_TOKENS:
        return [], []
    tokens = [token.strip() for token in _UPSTREAM_SPLIT.split(text)]
    valid = [token for token in tokens if token and token.lower() not in _NULL_TOKENS]
    ignored = [token for token in tokens if token and token.lower() in _NULL_TOKENS]
    # 去重但保持录入顺序，避免同一上游把入度算重。
    seen: set[str] = set()
    ordered: list[str] = []
    for token in valid:
        if token not in seen:
            seen.add(token)
            ordered.append(token)
    return ordered, ignored


def parse_timeout(raw: Any) -> tuple[int | None, str | None]:
    """把「超时分钟」解析成正整数；缺失/非法/非正数都返回原因，而不是 0。"""
    if raw is None:
        return None, "超时分钟为空"
    if isinstance(raw, bool):
        return None, f"超时分钟「{raw}」不是有效整数"
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, float):
        if not raw.is_integer():
            return None, f"超时分钟「{raw}」不是整数"
        value = int(raw)
    else:
        text = str(raw).strip()
        if text.lower() in _NULL_TOKENS:
            return None, "超时分钟为空"
        match = _TIMEOUT_RE.match(text)
        if not match:
            return None, f"超时分钟「{text}」不是有效整数"
        value = int(match.group(1))
    if value <= 0:
        return None, f"超时分钟「{value}」不是正数，无法参与时间推演"
    return value, None


def parse_clock_minutes(raw: Any) -> tuple[int | None, str | None]:
    """从日期/时间文本里取出时刻（分钟）；只有日期没有时刻时返回原因。"""
    if raw is None:
        return None, "时间字段为空"
    text = str(raw).strip()
    if text.lower() in _NULL_TOKENS:
        return None, "时间字段为空"
    match = _CLOCK_RE.search(text)
    if not match:
        return None, f"时间「{text}」缺少 HH:MM 时刻"
    return int(match.group(1)) * 60 + int(match.group(2)), None


def format_clock(minutes: int | None) -> str | None:
    """偏移分钟（相对自然日 06:00）→ HH:MM；None 原样返回，不伪装成 00:00。"""
    if minutes is None:
        return None
    clock = CYCLE_START_MIN + minutes
    clock %= 24 * 60
    return f"{clock // 60:02d}:{clock % 60:02d}"


# ---------------------------------------------------------------------------
# 图构建
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Node:
    node_id: str
    name: str
    upstreams: tuple[str, ...]
    timeout_min: int | None
    raw_timeout: Any
    row_id: int | None = None


@dataclass
class Graph:
    nodes: dict[str, Node]
    issues: list[dict[str, Any]]
    order: list[str]
    cycles: list[list[str]]
    version: str


def _find_cycle(nodes: dict[str, Node], candidates: set[str]) -> list[str] | None:
    """在候选集合里找一条环；返回形如 A → B → A 的路径。"""
    color: dict[str, int] = {}
    stack: list[str] = []

    def visit(node_id: str) -> list[str] | None:
        color[node_id] = 1
        stack.append(node_id)
        for upstream in nodes[node_id].upstreams:
            if upstream not in candidates:
                continue
            state = color.get(upstream, 0)
            if state == 0:
                found = visit(upstream)
                if found:
                    return found
            elif state == 1:
                start = stack.index(upstream)
                return stack[start:] + [upstream]
        stack.pop()
        color[node_id] = 2
        return None

    for node_id in sorted(candidates):
        if color.get(node_id, 0) == 0:
            found = visit(node_id)
            if found:
                return found
    return None


def _topological_order(nodes: dict[str, Node]) -> tuple[list[str], list[list[str]]]:
    """Kahn 拓扑排序；返回（可排序节点, 检测到的环列表）。"""
    children: dict[str, list[str]] = {node_id: [] for node_id in nodes}
    indegree: dict[str, int] = {}
    for node_id, node in nodes.items():
        upstreams = [up for up in node.upstreams if up in nodes]
        indegree[node_id] = len(upstreams)
        for upstream in upstreams:
            children[upstream].append(node_id)

    ready = [node_id for node_id, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        current = heapq.heappop(ready)
        order.append(current)
        for child in sorted(children[current]):
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, child)

    cycles: list[list[str]] = []
    remaining = {node_id for node_id, degree in indegree.items() if degree > 0}
    while remaining:
        found = _find_cycle(nodes, remaining)
        if not found:
            break
        cycles.append(found)
        remaining -= set(found)

    return order, cycles


def build_graph(rows: Iterable[dict[str, Any]]) -> Graph:
    """把 DAG 明细行解析成图，并把所有数据质量问题显式列出。"""
    nodes: dict[str, Node] = {}
    issues: list[dict[str, Any]] = []

    for row in rows:
        raw_id = row.get("节点编号")
        node_id = str(raw_id or "").strip()
        if not node_id:
            issues.append(issue("MISSING_NODE_ID", "存在没有节点编号的依赖图节点，已跳过"))
            continue
        if node_id in nodes:
            issues.append(
                issue(
                    "DUPLICATE_NODE",
                    f"节点编号 {node_id} 重复登记，只采用第一条",
                    node_id=node_id,
                )
            )
            continue
        upstreams, ignored = parse_upstreams(row.get("上游节点"))
        timeout_min, timeout_reason = parse_timeout(row.get("超时分钟"))
        node = Node(
            node_id=node_id,
            name=str(row.get("节点名称") or node_id).strip(),
            upstreams=tuple(upstreams),
            timeout_min=timeout_min,
            raw_timeout=row.get("超时分钟"),
            row_id=row.get("id"),
        )
        nodes[node_id] = node
        if ignored:
            issues.append(
                issue(
                    "IGNORED_UPSTREAM_TOKEN",
                    f"节点 {node_id} 的上游字段含占位片段 {ignored}，已忽略",
                    node_id=node_id,
                    severity="warning",
                )
            )
        if timeout_reason:
            issues.append(
                issue(
                    "MISSING_TIMEOUT",
                    f"节点 {node_id}（{node.name}）超时分钟不可用：{timeout_reason}",
                    node_id=node_id,
                )
            )

    for node in nodes.values():
        for upstream in node.upstreams:
            if upstream not in nodes:
                issues.append(
                    issue(
                        "MISSING_UPSTREAM",
                        f"节点 {node.node_id} 的上游 {upstream} 未在 DAG 节点清单登记",
                        node_id=node.node_id,
                    )
                )
            elif upstream == node.node_id:
                issues.append(
                    issue(
                        "SELF_LOOP",
                        f"节点 {node.node_id} 把自己登记为上游，构成自环",
                        node_id=node.node_id,
                    )
                )

    if not nodes:
        issues.append(issue("EMPTY_DAG", "DAG 节点清单为空，无法推演"))

    order, cycles = _topological_order(nodes)
    for cycle in cycles:
        issues.append(
            issue(
                "DAG_CYCLE",
                "依赖路径存在环：" + " → ".join(cycle) + "，无法确定拓扑顺序",
                node_id=cycle[0],
            )
        )

    version = "dag-" + _short_hash(
        [
            [
                node.node_id,
                list(node.upstreams),
                node.raw_timeout,
            ]
            for node in sorted(nodes.values(), key=lambda item: item.node_id)
        ]
    )
    return Graph(nodes=nodes, issues=issues, order=order, cycles=cycles, version=version)


def _short_hash(payload: Any) -> str:
    import hashlib
    import json

    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:12]


# ---------------------------------------------------------------------------
# 推演
# ---------------------------------------------------------------------------
def ancestors(nodes: dict[str, Node], starts: Iterable[str]) -> set[str]:
    """上游祖先闭包（含起点本身），只沿已登记的节点走。"""
    seen: set[str] = set()
    stack = [node_id for node_id in starts if node_id in nodes]
    seen.update(stack)
    while stack:
        current = stack.pop()
        for upstream in nodes[current].upstreams:
            if upstream in nodes and upstream not in seen:
                seen.add(upstream)
                stack.append(upstream)
    return seen


def scope_issues(graph: Graph, scope: set[str]) -> list[dict[str, Any]]:
    """只保留影响该基线的 error；图级 issue（node_id 为空）全部保留。"""
    return [
        item
        for item in graph.issues
        if item["severity"] == "error"
        and (item["node_id"] is None or item["node_id"] in scope)
    ]


def scope_cycles(graph: Graph, scope: set[str]) -> list[list[str]]:
    return [cycle for cycle in graph.cycles if set(cycle) & scope]


@dataclass
class Derivation:
    """一次基线推演的结果；derived=False 时 result 里只有不可推演原因。"""

    derived: bool
    result: dict[str, Any]
    issues: list[dict[str, Any]] = field(default_factory=list)


def derive_baseline_time(
    graph: Graph,
    *,
    endpoint_refs: list[str],
    scope_refs: list[str],
    level: int,
    warn_lead_min: int,
    observations: dict[str, dict[str, Any]],
    baseline_code: str,
    deadline_constraints: dict[str, dict[str, Any]] | None = None,
) -> Derivation:
    """按端点 + 依赖范围推演时间。

    observations: node_id → {minutes, source, raw}。
    deadline_constraints: node_id → {latest_finish_offset, source}，来自共享节点裁决；
    只收紧最晚完成时间（取更早值），绝不放宽基线自身的承诺。
    """
    issues: list[dict[str, Any]] = []

    if not endpoint_refs:
        issues.append(
            issue(
                "MISSING_ENDPOINT",
                "关键路径为空，无法确定基线终点",
                source="baseline",
            )
        )
    if not graph.nodes:
        issues.append(issue("EMPTY_DAG", "DAG 节点清单为空，无法推演", source="dag"))
    if level not in LEVEL_VALUES:
        issues.append(
            issue(
                "MISSING_LEVEL",
                f"关键路径等级「{level}」不在 1/2/3 范围内，无法裁决共享节点优先级",
                source="baseline",
            )
        )
    if warn_lead_min <= 0:
        issues.append(
            issue(
                "INVALID_WARN_LEAD",
                f"预警提前分钟「{warn_lead_min}」不是正数，预警起点无法推演",
                source="baseline",
            )
        )

    for ref in endpoint_refs + scope_refs:
        if ref not in graph.nodes:
            issues.append(
                issue(
                    "DANGLING_SCOPE",
                    f"基线 {baseline_code} 引用的节点 {ref} 不在 DAG 节点清单中",
                    node_id=ref,
                    source="baseline",
                )
            )
    if issues:
        return Derivation(False, {}, issues)

    scope = ancestors(graph.nodes, set(endpoint_refs) | set(scope_refs))
    blocking = scope_issues(graph, scope)
    if blocking:
        return Derivation(False, {}, blocking)
    cycles = scope_cycles(graph, scope)
    if cycles:
        cycle_issues = [
            issue(
                "DAG_CYCLE",
                "依赖路径存在环：" + " → ".join(cycle) + "，无法确定拓扑顺序",
                node_id=cycle[0],
                source="dag",
            )
            for cycle in cycles
        ]
        return Derivation(False, {}, cycle_issues)

    ordered = [node_id for node_id in graph.order if node_id in scope]
    if len(ordered) != len(scope):
        # 理论上被 blocking/cycle 拦住了；留一条显式兜底，避免静默跳过节点。
        return Derivation(
            False,
            {},
            [issue("UNORDERED_SCOPE", f"基线 {baseline_code} 的依赖范围无法完成拓扑排序", source="baseline")],
        )

    node_map = graph.nodes
    model_start: dict[str, int] = {}
    model_finish: dict[str, int] = {}
    observed_finish: dict[str, int] = {}

    for node_id in ordered:
        node = node_map[node_id]
        upstreams = [up for up in node.upstreams if up in scope]
        start = max((model_finish[up] for up in upstreams), default=0)
        model_start[node_id] = start
        assert node.timeout_min is not None  # blocking 已排除缺失
        model_finish[node_id] = start + node.timeout_min
        observed = observations.get(node_id)
        if observed and observed.get("minutes") is not None:
            observed_finish[node_id] = int(observed["minutes"]) - CYCLE_START_MIN

    model_ready = max(model_finish.values(), default=0)
    observed_ready = max(
        (max(model_finish[node_id], observed_finish[node_id]) for node_id in ordered if node_id in observed_finish),
        default=model_ready,
    )
    commit_offset = max(model_ready, observed_ready)

    # 逆推：终点（范围内没有下游的节点）以理论完成时间为截止。
    successors: dict[str, list[str]] = {node_id: [] for node_id in ordered}
    for node_id in ordered:
        for upstream in node_map[node_id].upstreams:
            if upstream in successors:
                successors[upstream].append(node_id)

    constraints = deadline_constraints or {}
    latest_finish: dict[str, int] = {}
    latest_start: dict[str, int] = {}
    applied_constraints: list[dict[str, Any]] = []
    for node_id in reversed(ordered):
        downstream = successors[node_id]
        assert node_map[node_id].timeout_min is not None
        finish = (
            min(latest_start[down] for down in downstream)
            if downstream
            else model_ready
        )
        constraint = constraints.get(node_id)
        if constraint and int(constraint["latest_finish_offset"]) < finish:
            finish = int(constraint["latest_finish_offset"])
            applied_constraints.append(
                {
                    "node_id": node_id,
                    "latest_finish": format_clock(finish),
                    "source": constraint.get("source"),
                }
            )
        latest_finish[node_id] = finish
        latest_start[node_id] = finish - node_map[node_id].timeout_min

    node_rows: list[dict[str, Any]] = []
    zero_slack: list[str] = []
    for node_id in ordered:
        node = node_map[node_id]
        slack = latest_start[node_id] - model_start[node_id]
        observed = observations.get(node_id)
        observed_offset = observed_finish.get(node_id)
        latest = latest_finish[node_id]
        node_rows.append(
            {
                "node_id": node_id,
                "name": node.name,
                "upstreams": list(node.upstreams),
                "timeout_min": node.timeout_min,
                "earliest_start": format_clock(model_start[node_id]),
                "earliest_start_offset_min": model_start[node_id],
                "earliest_finish": format_clock(model_finish[node_id]),
                "earliest_finish_offset_min": model_finish[node_id],
                "latest_finish": format_clock(latest),
                "latest_finish_offset_min": latest,
                "slack_min": slack,
                "zero_slack": slack == 0,
                "critical": slack == 0,
                "observed_finish": format_clock(observed_offset) if observed_offset is not None else None,
                "observed_source": observed.get("source") if observed else None,
                "observed_beyond_deadline": (
                    observed_offset is not None and observed_offset > latest
                ),
            }
        )
        if slack == 0:
            zero_slack.append(node_id)

    # 关键路径：从模型完成时间最大的终点回溯，取每个节点里最早完成最晚的上游。
    terminal_nodes = [node_id for node_id in ordered if not successors[node_id]]
    endpoint_candidates = [ref for ref in endpoint_refs if ref in scope] or terminal_nodes
    endpoint = max(
        endpoint_candidates,
        key=lambda node_id: (model_finish[node_id], node_id),
    )
    critical_path: list[str] = [endpoint]
    cursor = endpoint
    while node_map[cursor].upstreams:
        upstreams = [up for up in node_map[cursor].upstreams if up in scope]
        if not upstreams:
            break
        cursor = max(upstreams, key=lambda up: (model_finish[up], up))
        critical_path.append(cursor)
    critical_path.reverse()

    observed_nodes = [
        {
            "node_id": node_id,
            "finish": format_clock(observed_finish[node_id]),
            "finish_offset_min": observed_finish[node_id],
            "source": observations[node_id].get("source"),
            "raw": observations[node_id].get("raw"),
        }
        for node_id in ordered
        if node_id in observed_finish
    ]
    latest_observation = max(observed_nodes, key=lambda item: item["finish_offset_min"], default=None)

    warn_start_offset = commit_offset - warn_lead_min
    breaches = [row for row in node_rows if row["observed_beyond_deadline"]]
    overrun_nodes = [row["node_id"] for row in node_rows if row["slack_min"] < 0]
    headroom_min = (
        commit_offset - latest_observation["finish_offset_min"]
        if latest_observation
        else None
    )

    reasons: list[str] = []
    if overrun_nodes:
        level_name, risk_level = "高", "high"
        reasons.append(
            "共享节点裁决把最晚完成时间压到早于最早完成时间：" + "、".join(overrun_nodes)
        )
    elif breaches:
        level_name, risk_level = "高", "high"
        reasons.append(
            "最近产出晚于节点最晚完成时间：" + "、".join(row["node_id"] for row in breaches)
        )
    elif latest_observation is None:
        level_name, risk_level = "中", "medium"
        reasons.append("缺少最近产出，风险按模型时间估算，尚未用事实校验")
    elif headroom_min is not None and headroom_min <= warn_lead_min:
        level_name, risk_level = "中", "medium"
        reasons.append(
            f"最近产出 {latest_observation['finish']} 距承诺仅 {max(headroom_min, 0)} 分钟，"
            f"已进入 {warn_lead_min} 分钟预警窗口"
        )
    else:
        level_name, risk_level = "低", "low"
        reasons.append(f"最近产出 {latest_observation['finish']} 早于预警起点，尚有余量")
    if any(row["zero_slack"] for row in node_rows):
        reasons.append(f"{len(zero_slack)} 个节点零松弛，任何上游延迟都会直接推迟承诺")
    if warn_start_offset < 0:
        reasons.append("预警起点早于批次起算 06:00，需要人工确认上一批次是否跨天")

    observed_payload = None
    if latest_observation:
        observed_payload = {
            "node_id": latest_observation["node_id"],
            "finish": latest_observation["finish"],
            "source": latest_observation["source"],
            "raw": latest_observation["raw"],
        }

    result = {
        "baseline": baseline_code,
        "endpoint_refs": endpoint_refs,
        "scope_refs": scope_refs,
        "scope_size": len(scope),
        "scope_nodes": ordered,
        "level": level,
        "level_label": LEVEL_BUFFER_LABELS[level],
        "warn_lead_min": warn_lead_min,
        "model_finish_offset_min": model_ready,
        "model_finish": format_clock(model_ready),
        "commit_offset_min": commit_offset,
        "earliest_commit": format_clock(commit_offset),
        "warn_start_offset_min": warn_start_offset,
        "warn_start": format_clock(warn_start_offset),
        "critical_path": critical_path,
        "zero_slack_nodes": zero_slack,
        "nodes": node_rows,
        "shared_deadline_constraints": applied_constraints,
        "observed_latest": observed_payload,
        "observed_nodes": observed_nodes,
        "headroom_min": headroom_min,
        "risk": {
            "level": risk_level,
            "label": level_name,
            "headline": f"{baseline_code} · {level_name}风险：" + reasons[0],
            "earliest_commit": format_clock(commit_offset),
            "warn_start": format_clock(warn_start_offset),
            "warn_lead_min": warn_lead_min,
            "critical_path": critical_path,
            "zero_slack_nodes": zero_slack,
            "observed_latest": observed_payload,
            "headroom_min": headroom_min,
            "node_breaches": [row["node_id"] for row in breaches],
            "shared_deadline_constraints": applied_constraints,
            "reasons": reasons,
            "level_value": level,
        },
    }
    return Derivation(True, result, [])


def build_observations(entries: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """合并多个来源的最近产出，按节点保留最晚的观测值；非法时间记 warning 后丢弃。

    entries: {node_id, raw, source}
    """
    observations: dict[str, dict[str, Any]] = {}
    for entry in entries:
        minutes, reason = parse_clock_minutes(entry.get("raw"))
        if minutes is None:
            continue
        current = observations.get(entry["node_id"])
        if current is None or minutes > int(current["minutes"]):
            observations[entry["node_id"]] = {
                "minutes": minutes,
                "source": entry.get("source"),
                "raw": entry.get("raw"),
            }
    return observations


def observation_warnings(entries: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for entry in entries:
        minutes, reason = parse_clock_minutes(entry.get("raw"))
        if minutes is None:
            warnings.append(
                issue(
                    "INVALID_OBSERVATION",
                    f"最近产出「{entry.get('raw')}」无法参与推演：{reason}",
                    node_id=entry.get("node_id"),
                    severity="warning",
                    source=entry.get("source", "observation"),
                )
            )
    return warnings
