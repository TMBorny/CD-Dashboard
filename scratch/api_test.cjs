const axios = require('axios');
async function test() {
  const login = await axios.post('https://app.coursedog.com/api/v1/sessions', { email: 'REDACTED_EMAIL', password: 'REDACTED_PASSWORD' });
  const token = login.data.token;
  console.log('Token length:', token.length);
  
  const res = await axios.get('https://app.coursedog.com/api/v1/admin/schools/products', { headers: { Authorization: `Bearer ${token}` } });
  console.log('PRODUCTS len', res.data.length, res.data.slice(0, 1));

  // let's check integrations hub overview health for smu_peoplesoft
  const health = await axios.get('https://app.coursedog.com/api/v1/int/smu_peoplesoft/integrations-hub/overview/health', { headers: { Authorization: `Bearer ${token}` } });
  console.log('HEALTH', health.data);
}
test().catch(console.error);
