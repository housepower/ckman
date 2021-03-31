import axios, { AxiosError, AxiosResponse } from 'axios';

import '@/common/app/run';
import { $root, $router, _updateVueInstance } from '@/services';
import { InvalidTokenCode } from '@/constants';

axios.interceptors.response.use((value: AxiosResponse) => {
  if (value.config.url.startsWith(`/api`)) {
    if(value.data) {
      if(value.data.retCode) {
        if (InvalidTokenCode.includes(+value.data.retCode)) {
          if ($router.currentRoute.name !== 'Login') {
            setTimeout(() => {
              $router.push({
                path: '/login',
                query: {redirect: $router.currentRoute.fullPath},
              });
            }, 1500);
          }
        }
        return Promise.reject({
          config: value.config,
          code: value.status,
          request: value.request,
          response: value,
          isAxiosError: true,
        });
      }
    }
  }
  return value;
}, (err: AxiosError) => {
  if (err.message === 'Network Error') {
    err.message = '网络错误，请检查网络配置!';
  } else if (!axios.isCancel(err) && err.response.status === 401) {
    err.message = '访问超时，请重新登录';
    if (self === top) {
      $root.userInfo = undefined;
      _updateVueInstance($root);
      $router.push({name: 'home'});
    } else {
      window.parent.location.href = err.response.data.entity.url;
    }
  }
  return Promise.reject(err);
});
