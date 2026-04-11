import { createRouter, createWebHistory } from 'vue-router';
import ClientHealthDashboard from '@/components/Admin/ClientHealth/ClientHealthDashboard.vue';
import ClientHealthDetail from '@/components/Admin/ClientHealth/ClientHealthDetail.vue';
import JobsMonitoring from '@/components/Admin/JobsMonitoring/JobsMonitoring.vue';

const routes = [
  {
    path: '/',
    redirect: '/admin/client-health',
  },
  {
    path: '/admin/client-health',
    name: 'AdminClientHealth',
    component: ClientHealthDashboard,
  },
  {
    path: '/admin/client-health/:school',
    name: 'AdminClientHealthDetail',
    component: ClientHealthDetail,
    props: true,
  },
  {
    path: '/admin/jobs',
    name: 'AdminJobsMonitoring',
    component: JobsMonitoring,
  },
  {
    path: '/:catchAll(.*)',
    redirect: '/admin/client-health',
  },
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

export default router;