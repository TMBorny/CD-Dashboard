const axios = require('axios');

async function test() {
  const baseUrl = process.env.COURSEDOG_BASE_URL || 'https://app.coursedog.com';
  const email = process.env.COURSEDOG_EMAIL;
  const password = process.env.COURSEDOG_PASSWORD;

  if (!email || !password) {
    throw new Error('Set COURSEDOG_EMAIL and COURSEDOG_PASSWORD before running this script.');
  }

  const login = await axios.post(`${baseUrl}/api/v1/sessions`, { email, password });
  const token = login.data.token;
  console.log('Token length:', token.length);

  const res = await axios.get(`${baseUrl}/api/v1/admin/schools/products`, { headers: { Authorization: `Bearer ${token}` } });
  console.log('PRODUCTS len', res.data.length, res.data.slice(0, 1));

  // let's check integrations hub overview health for smu_peoplesoft
  const health = await axios.get(`${baseUrl}/api/v1/int/smu_peoplesoft/integrations-hub/overview/health`, { headers: { Authorization: `Bearer ${token}` } });
  console.log('HEALTH', health.data);
}
test().catch(console.error);
