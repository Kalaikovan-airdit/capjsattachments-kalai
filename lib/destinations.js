
const xsenv = require('@sap/xsenv');
const axios = require('axios');
xsenv.loadEnv();

async function getAccessToken() {
  try {

    const dest_service = xsenv.getServices({ dest: { tag: 'destination' } }).dest;
    if (!dest_service) {
      throw new Error("Destination service Instance Binding is needed")
    }
    const response = await axios({
      method: 'post',
      url: `${dest_service.url}/oauth/token`,
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': `Basic ${Buffer.from(`${dest_service.clientid}:${dest_service.clientsecret}`).toString('base64')}`
      },
      data: 'grant_type=client_credentials'
    });

    return response.data.access_token;
  } catch (error) {
    console.error('Error fetching access token:', error.response.data);
    throw new Error("Error fetching access token" + error)
  }
}

module.exports = {
  async fetchfromDestination(destinationName) {
    const destService = xsenv.getServices({ dest: { tag: 'destination' } }).dest;
    try {

      const headers = {
        'Content-Type': 'application/json',
      };
      const token = await getAccessToken();
      headers.Authorization = `Bearer ${token}`;

      const response = await axios.get(`${destService.uri}/destination-configuration/v1/instanceDestinations/${destinationName}`, {
        headers,
      });
      const config = response.data;

      if (!config) {
        throw new Error(`Destination ${destinationName} not found in BTP Cockpit`);
      }

      const { container_name,  sas_token, container_uri } = config;

      if (!container_name || !sas_token || !container_uri) {
        throw new Error(`Azure configuration fields are missing in destination: ${destinationName}`);
      }
      return { container_name,  sas_token, container_uri }
    } catch (err) {
      throw new Error("Configuration of Azure Container is Missing" + err)
    }
  }
}