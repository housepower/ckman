import axios from 'axios';

const url = '/api/v1/metric';

export const MetricApi = {
  querymetric(params) {
    return axios.get(`${url}/query`, { params });
  },
  queryRangeMetric(params) {
    return axios.get(`${url}/query_range`, { params });
  },
};
