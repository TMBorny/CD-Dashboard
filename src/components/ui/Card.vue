<script setup lang="ts">
interface Props {
  title?: string;
  subtitle?: string;
  bgClass?: string;
}
withDefaults(defineProps<Props>(), {
  bgClass: 'bg-white',
});
</script>

<template>
  <div :class="['rounded-[28px] border border-slate-200 p-6 shadow-sm', bgClass]">
    <div v-if="title || subtitle || $slots.header" class="mb-4">
      <slot name="header">
        <p v-if="subtitle" class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">{{ subtitle }}</p>
        <h3 v-if="title" class="mt-2 text-lg font-semibold text-slate-950">{{ title }}</h3>
      </slot>
    </div>
    <slot />
  </div>
</template>
