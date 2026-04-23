<script setup lang="ts">
import { ref } from 'vue';
import { useRoute } from 'vue-router';
import { isStaticDataMode } from '@/config/runtime';

const route = useRoute();
const isOpen = ref(true);
const buildLabel = import.meta.env.VITE_APP_BUILD ?? 'local';

const navItems = [
  {
    label: 'Client Health',
    path: '/admin/client-health',
    icon: '📊',
    name: 'AdminClientHealth',
  },
  {
    label: 'Error Analysis',
    path: '/admin/error-analysis',
    icon: '🚨',
    name: 'AdminErrorAnalysis',
  },
  {
    label: 'Job Monitoring',
    path: '/admin/jobs',
    icon: '⚙️',
    name: 'AdminJobsMonitoring',
  },
  {
    label: 'Operations',
    path: '/admin/operations',
    icon: '🛠️',
    name: 'AdminOperations',
  },
];

const toggleSidebar = () => {
  isOpen.value = !isOpen.value;
};

const isActive = (itemPath: string) => {
  return route.path === itemPath || route.path.startsWith(itemPath + '/');
};
</script>

<template>
  <div class="flex min-h-screen">
    <!-- Sidebar -->
    <div :class="['fixed left-0 top-0 h-screen bg-slate-900 text-white transition-all duration-300 flex flex-col z-40', isOpen ? 'w-64' : 'w-20']">
      <!-- Header -->
      <div class="flex items-center gap-3 border-b border-slate-800 px-3 py-4">
        <button @click="toggleSidebar" class="p-2 hover:bg-slate-800 rounded-lg transition flex-shrink-0">
          <span v-if="isOpen">←</span>
          <span v-else>→</span>
        </button>
        <div v-if="isOpen" class="flex min-w-0 items-center gap-2">
          <div class="flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-2xl bg-blue-500 text-sm font-bold text-white shadow-sm">
            CD
          </div>
          <div class="min-w-0">
            <p class="truncate text-lg font-semibold leading-none text-white">CourseDog</p>
            <p class="mt-1 truncate text-xs uppercase tracking-[0.2em] text-slate-400">Dashboard</p>
          </div>
        </div>
      </div>

      <!-- Navigation Items -->
      <nav class="flex-1 px-3 py-6 space-y-2">
        <router-link
          v-for="item in navItems"
          :key="item.path"
          :to="item.path"
          :class="[
            'flex items-center justify-center lg:justify-start gap-3 px-3 py-3 rounded-lg transition font-medium',
            isActive(item.path)
              ? 'bg-blue-600 text-white'
              : 'text-slate-300 hover:bg-slate-800 hover:text-white',
          ]"
        >
          <span class="text-xl flex-shrink-0">{{ item.icon }}</span>
          <span v-if="isOpen" class="truncate">{{ item.label }}</span>
        </router-link>
      </nav>

      <!-- Footer -->
      <div v-if="isOpen" class="px-4 py-6 border-t border-slate-800">
        <p v-if="isStaticDataMode" class="mb-2 text-xs uppercase tracking-[0.16em] text-amber-300">Static mode</p>
        <p class="text-xs text-slate-400">{{ buildLabel }}</p>
      </div>
    </div>

    <!-- Main Content -->
    <main :class="['transition-all duration-300 w-full overflow-y-auto overflow-x-hidden', isOpen ? 'ml-64' : 'ml-20']">
      <slot />
    </main>
  </div>
</template>
