<script setup lang="ts">
import { computed } from 'vue';
import { useQuery } from '@tanstack/vue-query';
import { getStaticManifest } from '@/api/static';
import { isStaticDataMode } from '@/config/runtime';
import Sidebar from './components/Sidebar.vue';

const { data: staticManifest } = useQuery({
  queryKey: ['staticExportManifest'],
  queryFn: getStaticManifest,
  enabled: isStaticDataMode,
});

const staticSnapshotLabel = computed(() => staticManifest.value?.latestSnapshotDate || 'Unavailable');
const staticExportLabel = computed(() => {
  if (!staticManifest.value?.exportedAt) {
    return 'Unavailable';
  }

  return new Date(staticManifest.value.exportedAt).toLocaleString();
});
</script>

<template>
  <Sidebar>
    <div
      v-if="isStaticDataMode"
      class="sticky top-0 z-30 border-b border-amber-200 bg-amber-50/95 px-6 py-3 text-sm text-amber-900 backdrop-blur"
    >
      Read-only static snapshot.
      Latest snapshot: {{ staticSnapshotLabel }}.
      Exported: {{ staticExportLabel }}.
    </div>
    <router-view />
  </Sidebar>
</template>

<style>
body {
  margin: 0;
  min-height: 100vh;
  background: #f8fafc;
  color: #0f172a;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

#app {
  min-height: 100vh;
}
</style>
