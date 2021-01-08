import axios from 'axios';

const url = '/api/v1';

export const TablesApi = {
  zkStatus(name: string) {
    return axios.get(`${url}/zk/status/${name}`);
  },
  tableMetrics(name: string) {
    return axios.get(`${url}/ck/table_metric/${name}`);
  },
  replicationStatus(name: string) {
    return axios.get(`${url}/zk/replicated_table/${name}`);
  },
};
