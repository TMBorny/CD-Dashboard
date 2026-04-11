<script setup lang="ts">
import { computed } from 'vue';

interface Props {
  tone?: 'emerald' | 'amber' | 'rose' | 'slate' | 'default';
  label?: string;
}

const props = withDefaults(defineProps<Props>(), {
  tone: 'default',
});

const toneClass = computed(() => {
  switch (props.tone) {
    case 'emerald': return 'bg-emerald-100 text-emerald-800 border-transparent';
    case 'amber': return 'bg-amber-100 text-amber-800 border-transparent';
    case 'rose': return 'bg-rose-100 text-rose-800 border-transparent';
    case 'slate': return 'bg-slate-100 text-slate-800 border-transparent';
    default: return 'bg-slate-50 text-slate-600 border-slate-200';
  }
});
</script>

<template>
  <span :class="['inline-flex items-center rounded-full border px-3 py-1 text-xs font-semibold', toneClass]">
    <slot>{{ label }}</slot>
  </span>
</template>
