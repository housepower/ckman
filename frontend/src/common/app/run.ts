import './polyfill';

import axios, { AxiosError, AxiosResponse, Canceler, CancelToken } from 'axios';
import Vue from 'vue';
import VueRouter from 'vue-router';

import './validateMessges';

import './hackElementUI';

import { $router } from '@/services';
import { Project } from '../constants';
import { $message, $progress, $root, $notify } from '../services';

const cancelTokenSources = new Map<CancelToken, Canceler>();
let loading = 0;

if (Project.serviceName) {
  axios.interceptors.request.use(config => {
    if (config.url.startsWith('/')) config.url = `/${Project.serviceName}${config.url}`;
    return config;
  });
}

axios.interceptors.request.use(config => {
  const user = localStorage.getItem('user') || '{}';
  const token = JSON.parse(user).token;
  if(token) {
    config.headers = {
      token,
    };
  } else {
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    $router.replace('/login').catch(()=>{});
  }
  if (++loading) {
    $progress.start();
  } else {
    $progress.decrease(10);
  }

  if (!('cancelToken' in config)) { // 排除不需要cancel的请求
    const source = axios.CancelToken.source();
    (source.token as CancelToken & { url?: string }).url = config.url;
    cancelTokenSources.set(source.token, source.cancel); // 加入cancel队列
    config.cancelToken = source.token;
  }
  return config;
});
axios.interceptors.response.use((value: AxiosResponse) => {
  if (!--loading) $progress.finish();

  if (value.config.cancelToken) {
    cancelTokenSources.delete(value.config.cancelToken);
  }
  return value;
}, (err: AxiosError) => {
  if (!--loading) {
    $progress.fail();
    $progress.finish();
  }
  if (axios.isCancel(err)) {
    cancelTokenSources.delete(err.message as any);
  }
  return Promise.reject(err);
});

$router.afterEach(() => { // 路由跳转杀请求
  for (const [cancelToken, cancel] of cancelTokenSources) {
    cancel(cancelToken as any); // cancel 正在pending的请求
  }
});

$router.afterEach(() => { // 路由跳转时关闭所有弹窗
  $root.modals.forEach(x => x.resolve(Promise.reject('cancel')));
});

const errorHandler = event => {
  if (!event.reason) return;

  if (event.reason instanceof Error) {
    if (VueRouter.isNavigationFailure(event.reason)) {
      return;
    }

    $message.fuck(event.reason);
    return;
  }

  if (event.reason.isAxiosError) {
    // $message.fuck(event.reason);
    $notify.fuck(event.reason);
    return;
  }

  if (axios.isCancel(event.reason)) {
    event.preventDefault();
    return;
  }

  switch (event.reason && (event.reason.message || event.reason)) {
    case 'invalid':
    case 'canceled':
    case 'cancel':
      event.preventDefault();
      return;
  }
};

window.addEventListener('unhandledrejection', errorHandler);

Vue.config.errorHandler = reason => {
  let defaultPrevented = false;
  errorHandler({ reason, preventDefault: () => defaultPrevented = true });
  if (!defaultPrevented) throw reason;
};
