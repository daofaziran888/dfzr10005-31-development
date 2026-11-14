<template>
  <section class="page" data-module="baseline">
    <header class="page-head">
      <div>
        <h2>基线监控管理</h2>
        <p class="page-desc">
          承诺时间与预警时间由 DAG 关键路径推演得出：依赖范围取上游闭包，按节点超时正推最早完成、
          按关键路径逆推最晚完成，再并入最近产出。循环依赖、节点或超时缺失都会给出不可推演原因，不会按 0 处理。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" :disabled="deriving" @click="derive('')">
          {{ deriving ? '推演中…' : '推演基线时间' }}
        </button>
        <button class="btn" type="button" :disabled="!scopeRef || deriving" @click="deriveNode">
        {{ deriving ? '推演中…' : '按节点推演' }}
      </button>
      <input v-model="scopeRef" class="scope-input" placeholder="DAG-0007" aria-label="按节点推演" />
      <button class="btn" type="button" @click="exportRows">导出基线清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>推演结果</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>
            <span v-if="row['推演状态'] === '已推演'" class="tag tag-ok">
              承诺 {{ row['最早可承诺时间'] }} · 预警 {{ row['预警起点'] }}
            </span>
            <span v-else-if="row['推演状态'] === '不可推演'" class="tag tag-bad">不可推演</span>
            <span v-else class="tag">待推演</span>
            <span v-if="row['窗口放行'] === 'blocked'" class="tag tag-warn">窗口阻塞</span>
            <span v-if="row['风险等级'] && row['风险等级'] !== '未知'" class="tag" :class="riskClass(String(row['风险等级']))">
              {{ row['风险等级'] }}风险
            </span>
          </td>
          <td class="row-actions">
            <button class="link" type="button" @click="derive(String(row.id))">单条推演</button>
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 2" class="empty-state">暂无基线配置，可先登记基线配置</td>
        </tr>
      </tbody>
    </table>

    <section class="panel">
      <header class="panel-head">
        <h3>时间推演结果</h3>
        <span class="panel-note" v-if="snapshot">
          快照 {{ snapshot.snapshot_id }} · DAG 版本 {{ snapshot.dag_version }} · {{ snapshot.generated_at }}
          <span v-if="snapshot.deduplicated" class="tag">同版本回放</span>
        </span>
      </header>
      <p v-if="message" class="panel-note">{{ message }}</p>
      <table class="data-table" v-if="snapshot">
        <thead>
          <tr>
            <th>基线</th>
            <th>等级</th>
            <th>推演状态</th>
            <th>最早可承诺</th>
            <th>预警起点</th>
            <th>关键路径</th>
            <th>零松弛节点</th>
            <th>风险</th>
            <th>窗口</th>
            <th>原因 / 摘要</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in snapshot.items" :key="item.baseline">
            <td>{{ item.baseline }}</td>
            <td>{{ item.level_label ?? '未配置' }}</td>
            <td>
              <span class="tag" :class="item.derived ? 'tag-ok' : 'tag-bad'">
                {{ item.derived ? '可推演' : '不可推演' }}
              </span>
            </td>
            <td>{{ item.result?.earliest_commit ?? '—' }}</td>
            <td>{{ item.result?.warn_start ?? '—' }}</td>
            <td class="wide-cell">
              {{ item.result ? item.result.critical_path.join(' → ') : '—' }}
            </td>
            <td class="wide-cell">
              {{ item.result?.zero_slack_nodes?.join('、') ?? '—' }}
            </td>
            <td>
              <span v-if="item.risk" class="tag" :class="riskClass(item.risk.label)">
                {{ item.risk.label }} · {{ item.risk.headline }}
              </span>
              <span v-else>—</span>
            </td>
            <td>
              <span v-if="item.window_verdict" class="tag" :class="item.window_verdict.status === 'released' ? 'tag-ok' : item.window_verdict.status === 'blocked' ? 'tag-warn' : ''">
                {{ windowLabel(item.window_verdict.status) }}
              </span>
              <span v-else>—</span>
            </td>
            <td class="wide-cell">
              <template v-if="item.derived">{{ item.risk?.headline }}</template>
              <ul v-else class="reason-list">
                <li v-for="reason in item.unresolved_reasons" :key="reason.code">
                  [{{ reason.code }}] {{ reason.message }}
                </li>
              </ul>
            </td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>DAG → 基线 → SLA 时间调用链</h3>
        <button class="link" type="button" @click="chainVisible = !chainVisible">
          {{ chainVisible ? '收起流程图' : '展开流程图' }}
        </button>
      </header>
      <p class="panel-note">时间口径：{{ chain?.time_basis.cycle }}；{{ chain?.time_basis.clock }}；{{ chain?.time_basis.priority }}。</p>
      <DerivationFlow v-if="chainVisible && chain" :chain="chain.chain" />
      <div v-if="chainVisible && chain" class="flow">
        <article v-for="step in chain.chain" :key="step.step" class="flow-step">
          <header>
            <span class="flow-index">{{ step.step }}</span>
            <span class="tag">{{ step.stage }}</span>
            <strong>{{ step.name }}</strong>
          </header>
          <ul class="flow-rules">
            <li v-for="rule in step.rules" :key="rule">{{ rule }}</li>
          </ul>
          <p v-if="step.fail_codes.length" class="flow-fail">
            不可推演码：{{ step.fail_codes.join('、') }}
          </p>
        </article>
      </div>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>推演快照</h3>
        <span class="panel-note">同一版本只保留一份，重复推演直接回放，快照可追溯。</span>
      </header>
      <div class="snapshot-list">
        <button
          v-for="item in snapshots"
          :key="item.snapshot_id"
          class="snapshot-chip"
          type="button"
          @click="openSnapshot(item.snapshot_id)"
        >
          <strong>{{ item.snapshot_id }}</strong>
          <span>{{ item.generated_at }}</span>
          <span>放行 {{ item.released.length }} · 阻塞 {{ item.blocked.length }}</span>
        </button>
        <span v-if="!snapshots.length" class="empty-state">还没有推演快照</span>
      </div>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条基线配置</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'
import DerivationFlow from '@/components/DerivationFlow.vue'

type Row = Record<string, unknown>

type UnresolvedReason = { code: string; message: string }
type Risk = { level: string; label: string; headline: string }
type WindowVerdict = { status: string; reason: string }
type DerivationItem = {
  baseline: string
  level_label?: string | null
  derived: boolean
  unresolved_reasons: UnresolvedReason[]
  result?: { earliest_commit: string; warn_start: string; critical_path: string[]; zero_slack_nodes: string[] } | null
  risk?: Risk | null
  window_verdict?: WindowVerdict | null
}
type Snapshot = {
  snapshot_id: string
  generated_at: string
  dag_version: string
  deduplicated?: boolean
  items: DerivationItem[]
  released: string[]
  blocked: string[]
}
type ChainStep = {
  step: number
  stage: string
  name: string
  reads: string[]
  writes: string[]
  rules: string[]
  fail_codes: string[]
}
type Chain = {
  chain: ChainStep[]
  time_basis: Record<string, string>
}

const ENDPOINT = '/api/baseline'
const columns = ['基线编号', '基线名称', '承诺时间', '预警时间', '关键路径', '依赖范围', '监控状态', '最近产出']
const actions = ['启用监控', '调整基线', '关闭基线']
const filterFields = columns.slice(0, 3)

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const message = ref('')
const deriving = ref(false)
const filters = ref<Record<string, string>>({})
const snapshot = ref<Snapshot | null>(null)
const snapshots = ref<Snapshot[]>([])
const chain = ref<Chain | null>(null)
const chainVisible = ref(true)
const scopeRef = ref('')

const stats = computed(() => {
  const items = snapshot.value?.items ?? []
  const derived = items.filter((item) => item.derived)
  return [
    { label: '基线配置总数', value: total.value },
    { label: '最近快照可推演', value: derived.length },
    { label: '不可推演', value: items.length - derived.length },
    { label: '窗口放行 / 阻塞', value: `${snapshot.value?.released.length ?? 0} / ${snapshot.value?.blocked.length ?? 0}` },
  ]
})

function riskClass(level: string) {
  if (level === '高') return 'tag-bad'
  if (level === '中') return 'tag-warn'
  return 'tag-ok'
}

function windowLabel(status: string) {
  if (status === 'released') return '已放行'
  if (status === 'blocked') return '被占用'
  if (status === 'not_applicable') return '不适用'
  return status
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

async function callDerive(body: Record<string, unknown>) {
  deriving.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/derive`, {
      method: 'POST',
      body: JSON.stringify(body),
    })
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '推演未完成')
    }
    message.value = payload.message
    snapshot.value = payload.entry
    await Promise.all([reload(), loadSnapshots()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '基线时间推演失败'
  } finally {
    deriving.value = false
  }
}

function derive(rowId: string) {
  void callDerive(rowId ? { baseline_id: Number(rowId) } : {})
}

function deriveNode() {
  const ref = scopeRef.value.trim()
  if (!ref) return
  void callDerive({ scope_ref: ref, dry_run: true })
}

async function openSnapshot(snapshotId: string) {
  try {
    const response = await request(`${ENDPOINT}/snapshots/${snapshotId}`)
    if (!response.ok) throw new Error('快照读取失败')
    snapshot.value = await response.json()
    message.value = `已载入历史快照 ${snapshotId}`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '快照读取失败'
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '动作未生效')
    }
    message.value = payload.message
    await Promise.all([reload(), loadSnapshots()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '基线操作失败'
  }
}

async function loadSnapshots() {
  try {
    const response = await request(`${ENDPOINT}/snapshots?limit=20`)
    if (!response.ok) return
    const payload = await response.json()
    snapshots.value = payload.items ?? []
  } catch {
    snapshots.value = []
  }
}

async function loadChain() {
  try {
    const response = await request(`${ENDPOINT}/derivation/chain`)
    if (!response.ok) return
    chain.value = await response.json()
  } catch {
    chain.value = null
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('基线配置列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '基线列表读取失败'
  }
}

onMounted(async () => {
  await Promise.all([reload(), loadSnapshots(), loadChain()])
})
</script>
