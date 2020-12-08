import axios from 'axios';

const url = '/api/login';

export const LoginApi = {
  login(params) {
    return axios.post(`${url}`, params);
  },
};
