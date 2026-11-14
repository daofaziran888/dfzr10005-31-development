<template>
  <div class="flow-scroll">
    <svg :viewBox="`0 0 ${WIDTH} ${height}`" class="flow-svg" role="img" aria-label="DAG 到基线的时间推演调用链">
      <defs>
        <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="4" orient="auto">
          <path d="M0,0 L8,4 L0,8 z" fill="#64748b" />
        </marker>
        <marker id="arrow-dash" markerWidth="8" markerHeight="8" refX="6" refY="4" orient="auto">
          <path d="M0,0 L8,4 L0,8 z" fill="#b42318" />
        </marker>
      </defs>

      <!-- 头部：三个台账作为输入 -->
      <g>
        <rect x="30" y="16" width="380" height="46" rx="8" fill="#101828" />
        <text x="46" y="44" fill="#e5e7eb" font-size="13">输入：DAG 节点清单 · 基线配置 · SLA 规则</text>
        <line x1="210" y1="62" x2="210" y2="86" stroke="#64748b" stroke-width="1.6" marker-end="url(#arrow)" />
      </g>

      <!-- 主流程：每一步一个节点 -->
      <g v-for="(step, index) in chain" :key="step.step">
        <rect :x="X" :y="y(index)" :width="BOX_W" :height="BOX_H" rx="8" fill="#fff" stroke="#d8dee6" />
        <rect :x="X" :y="y(index)" width="4" :height="BOX_H" rx="2" fill="#1f6feb" />
        <text :x="X + 16" :y="y(index) + 22" font-size="12" fill="#1f6feb">STEP {{ step.step }} · {{ step.stage }}</text>
        <text :x="X + 16" :y="y(index) + 42" font-size="14" font-weight="600" fill="#1f2937">{{ step.name }}</text>
        <text :x="X + 16" :y="y(index) + 62" font-size="11" fill="#64748b">{{ readLine(step) }}</text>
        <text :x="X + 16" :y="y(index) + 78" font-size="11" fill="#64748b">{{ writeLine(step) }}</text>

        <!-- 主链箭头 -->
        <line
          v-if="index < chain.length - 1"
          :x1="X + BOX_W / 2"
          :y1="y(index) + BOX_H"
          :x2="X + BOX_W / 2"
          :y2="y(index + 1)"
          stroke="#64748b"
          stroke-width="1.6"
          marker-end="url(#arrow)"
        />

        <!-- 失败分支：不可推演 -->
        <template v-if="step.fail_codes.length">
          <line
            :x1="X + BOX_W"
            :y1="y(index) + BOX_H / 2"
            :x2="MARK_X - 8"
            :y2="y(index) + BOX_H / 2"
            stroke="#b42318"
            stroke-width="1.4"
            stroke-dasharray="5 4"
            marker-end="url(#arrow-dash)"
          />
          <rect :x="MARK_X" :y="y(index) + 10" width="360" height="66" rx="8" fill="#fff5f5" stroke="#f3b4b4" />
          <text :x="MARK_X + 12" :y="y(index) + 30" font-size="12" font-weight="600" fill="#b42318">不可推演</text>
          <text :x="MARK_X + 12" :y="y(index) + 48" font-size="11" fill="#7f1d1d">{{ step.fail_codes.join(' · ') }}</text>
          <text :x="MARK_X + 12" :y="y(index) + 64" font-size="11" fill="#7f1d1d">给出原因，绝不按 0 处理</text>
        </template>
      </g>

      <!-- 收尾：窗口仲裁的两个出口 -->
      <g>
        <line
          :x1="X + BOX_W / 2"
          :y1="endY"
          :x2="X + BOX_W / 2"
          :y2="endY + 30"
          stroke="#64748b"
          stroke-width="1.6"
          marker-end="url(#arrow)"
        />
        <line :x1="X + 90" :y1="endY + 30" :x2="X + 90" :y2="endY + 54" stroke="#15803d" stroke-width="1.6" marker-end="url(#arrow)" />
        <line :x1="X + 290" :y1="endY + 30" :x2="X + 290" :y2="endY + 54" stroke="#b45309" stroke-width="1.6" marker-end="url(#arrow)" />
        <rect :x="X" :y="endY + 54" width="180" height="44" rx="8" fill="#e7f6ec" stroke="#a7d8b6" />
        <text :x="X + 90" :y="endY + 74" font-size="12" text-anchor="middle" fill="#15803d">放行：写回承诺 / 预警</text>
        <text :x="X + 90" :y="endY + 90" font-size="11" text-anchor="middle" fill="#15803d">DAG 回执 + SLA 风险重算</text>
        <rect :x="X + 200" :y="endY + 54" width="180" height="44" rx="8" fill="#fef3e2" stroke="#f2cf9b" />
        <text :x="X + 290" :y="endY + 74" font-size="12" text-anchor="middle" fill="#b45309">阻塞：只放行一个</text>
        <text :x="X + 290" :y="endY + 90" font-size="11" text-anchor="middle" fill="#b45309">只给建议值，不覆盖承诺</text>
      </g>
    </svg>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

type ChainStep = {
  step: number
  stage: string
  name: string
  reads: string[]
  writes: string[]
  rules: string[]
  fail_codes: string[]
}

const props = defineProps<{ chain: ChainStep[] | null }>()

const WIDTH = 860
const X = 30
const BOX_W = 380
const BOX_H = 96
const GAP = 34
const MARK_X = 450
const TOP = 90

const chain = computed(() => props.chain ?? [])
const height = computed(() => endY.value + 30 + 54 + 44 + 24)
const endY = computed(() => {
  const steps = Math.max(chain.value.length, 1)
  return TOP + (steps - 1) * (BOX_H + GAP) + BOX_H
})

function y(index: number): number {
  return TOP + index * (BOX_H + GAP)
}

function readLine(step: ChainStep): string {
  const reads = step.reads.length ? step.reads.join('、') : '无'
  return `取数：${truncate(reads, 64)}`
}

function writeLine(step: ChainStep): string {
  const writes = step.writes.length ? step.writes.join('、') : '无'
  return `写回：${truncate(writes, 64)}`
}

function truncate(text: string, limit: number): string {
  return text.length > limit ? `${text.slice(0, limit)}…` : text
}
</script>
