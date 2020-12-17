import axios from 'axios';
import { $root } from './root';

interface TopNotifyModel {
  title?: string;
  type?: string;
  showIcon?: boolean;
  popperClass?: string;
  close?: () => void;
}

export const $notify = {
  init(args: TopNotifyModel) {
    const { title, type = 'error', showIcon = true, popperClass = 'top-notify' } = args;
    const notifysProp = {
      title,
      type,
      showIcon,
      popperClass,
      close: () => {
        $root.notifys.splice($root.notifys.indexOf(notifysProp), 1);
      },
    };
    $root.notifys.push(notifysProp);
  },
  fuck(obj: any) {
    if (axios.isCancel(obj)) return console.info('Request canceled: ', obj.message.url);
    if (obj === 'cancel') return console.info('User canceled');

    console.error(obj);
    let errorInfo = '';
    if (obj == null) {
      errorInfo = '未知错误，请联系管理员';
    } else if (obj instanceof Error) {
      errorInfo = obj.message;
    } else if (typeof obj !== 'object') {
      errorInfo = obj + '';
    } else if (obj.data || obj.response && obj.response.data) {
      const data = obj.data || obj.response.data;
      errorInfo = data.detail || data.error || data.message || data.msg || data.retMsg || JSON.stringify(data);
    } else {
      errorInfo = obj.detail || obj.error || obj.message || obj.msg || obj.retMsg || JSON.stringify(obj);
    }
    return $notify.init({ title:  `错误提示： ${errorInfo}`});
  },
};

