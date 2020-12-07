import axios from 'axios';

const url = '/api/v1/ck/query';

export const SqlQuery =  {
  query(params) {
    return axios.get(`${url}/${params.clusterName}`, { params });
  },
};
