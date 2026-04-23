import { createRouter, createWebHashHistory, createWebHistory } from 'vue-router';
import ClientHealthDashboard from '@/components/Admin/ClientHealth/ClientHealthDashboard.vue';
import ClientHealthDetail from '@/components/Admin/ClientHealth/ClientHealthDetail.vue';
import ErrorAnalysisDashboard from '@/components/Admin/ErrorAnalysis/ErrorAnalysisDashboard.vue';
import JobRunDetail from '@/components/Admin/JobsMonitoring/JobRunDetail.vue';
import JobsMonitoring from '@/components/Admin/JobsMonitoring/JobsMonitoring.vue';
import Operations from '@/components/Admin/Operations/Operations.vue';
import { isStaticDataMode, siteBase } from '@/config/runtime';

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
    path: '/admin/error-analysis',
    name: 'AdminErrorAnalysis',
    component: ErrorAnalysisDashboard,
  },
  {
    path: '/admin/jobs',
    name: 'AdminJobsMonitoring',
    component: JobsMonitoring,
  },
  {
    path: '/admin/jobs/:jobId',
    name: 'AdminJobRunDetail',
    component: JobRunDetail,
    props: true,
  },
  {
    path: '/admin/operations',
    name: 'AdminOperations',
    component: Operations,
  },
  {
    path: '/:catchAll(.*)',
    redirect: '/admin/client-health',
  },
];

const router = createRouter({
  history: isStaticDataMode ? createWebHashHistory(siteBase) : createWebHistory(siteBase),
  routes,
});

export default router;
