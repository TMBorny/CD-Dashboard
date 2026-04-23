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
    meta: { title: 'Client Health Dashboard' }
  },
  {
    path: '/admin/client-health/:school',
    name: 'AdminClientHealthDetail',
    component: ClientHealthDetail,
    props: true,
    meta: { title: 'Client Health Detail' }
  },
  {
    path: '/admin/error-analysis',
    name: 'AdminErrorAnalysis',
    component: ErrorAnalysisDashboard,
    meta: { title: 'Error Analysis' }
  },
  {
    path: '/admin/jobs',
    name: 'AdminJobsMonitoring',
    component: JobsMonitoring,
    meta: { title: 'Jobs' }
  },
  {
    path: '/admin/jobs/:jobId',
    name: 'AdminJobRunDetail',
    component: JobRunDetail,
    props: true,
    meta: { title: 'Job Detail' }
  },
  {
    path: '/admin/operations',
    name: 'AdminOperations',
    component: Operations,
    meta: { title: 'Operations' }
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

router.afterEach((to) => {
  const baseTitle = 'CD Dashboard';
  if (to.meta && to.meta.title) {
    document.title = `${to.meta.title} - ${baseTitle}`;
  } else {
    document.title = baseTitle;
  }
});

export default router;
