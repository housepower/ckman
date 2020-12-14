import axios from 'axios';

const url = '/api/v1';

export const PackageApi = {
  getConfig() {
    return axios.get(`${url}/config`);
  },
  updateConfig(params) {
    return axios.put(`${url}/config`, params);
  },
  getList() {
    return axios.get(`${url}/package`);
  },
  upload(params, opt) {
    return axios.post(`${url}/package`, params, opt);
  },
  deletePackage(params) {
    return axios.delete(`${url}/package`, { params });
  },
};
