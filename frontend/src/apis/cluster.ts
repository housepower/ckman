import axios from 'axios';

const url = '/api/v1/ck';
const createUrl = '/api/v1/deploy/ck';

export const ClusterApi = {
  getCluster() {
    return axios.get(`${url}/cluster`);
  },
  importCluster(params) {
    return axios.post(`${url}/cluster`, params);
  },
  createCluster(params) {
    return axios.post(`${createUrl}/`, params);
  },
  updateCluster(params) {
    return axios.put(`${url}/cluster`, params);
  },
  deleteCluster(id) {
    return axios.delete(`${url}/cluster/${id}`);
  },
  manageCluster(type, params) {
    const { clusterName, packageVersion } = params;
    if(!packageVersion) {
      return axios.put(`${url}/${type}/${clusterName}`);
    } else {
      return axios.put(`${url}/${type}/${clusterName}`,{ packageVersion });
    }
  },
  getClusterInfo(id) {
    return axios.get(`${url}/get/${id}`);
  },
  addClusterNode(id, params) {
    return axios.post(`${url}/node/${id}`, params);
  },
  deleteClusterNode(id, params) {
    return axios.delete(`${url}/node/${id}`, { params });
  },
};
