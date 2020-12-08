import axios from 'axios';

const url = '/login';

export const LoginApi = {
  login(params) {
    return axios.post(`${url}`, params);
  },
};
