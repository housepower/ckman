import axios from 'axios';

const url = '/api/v1/package';

export const PackageApi = {
  getList() {
    return axios.get(`${url}`);
  },
  upload(params) {
    return axios.post(`${url}`, params);
  },
  deletePackage(params) {
    return axios.delete(`${url}`, { params });
  },
};
