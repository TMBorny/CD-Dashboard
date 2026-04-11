<script setup lang="ts">
import { ref } from 'vue';
import { useRoute } from 'vue-router';

const route = useRoute();
const isOpen = ref(true);

const navItems = [
  {
    label: 'Client Health',
    path: '/admin/client-health',
    icon: '📊',
    name: 'AdminClientHealth',
  },
  {
    label: 'Job Monitoring',
    path: '/admin/jobs',
    icon: '⚙️',
    name: 'AdminJobsMonitoring',
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
      <div class="flex items-center justify-between px-3 py-4 border-b border-slate-800">
        <button @click="toggleSidebar" class="p-2 hover:bg-slate-800 rounded-lg transition flex-shrink-0">
          <span v-if="isOpen">←</span>
          <span v-else>→</span>
        </button>
        <div v-if="isOpen" class="flex items-center gap-2 ml-2">
          <div class="w-7 h-7 bg-blue-500 rounded-lg flex items-center justify-center font-bold text-sm">CD</div>
          <span class="font-semibold truncate">Dashboard</span>
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
        <p class="text-xs text-slate-400">v1.0.0</p>
      </div>
    </div>

    <!-- Main Content -->
    <main :class="['transition-all duration-300 w-full overflow-y-auto overflow-x-hidden', isOpen ? 'ml-64' : 'ml-20']">
      <slot />
    </main>
  </div>
</template>
