import axios from 'axios';

const url = '/api/v1/zk/status';

export const TablesApi = {
  zkStatus(name: string) {
    return axios.get(`${url}/${name}`);
  },
};
