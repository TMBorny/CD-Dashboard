<script setup lang="ts">
import Sidebar from './components/Sidebar.vue';
</script>

<template>
  <Sidebar>
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
