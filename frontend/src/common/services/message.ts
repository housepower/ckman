import axios from 'axios';
import { Message } from 'element-ui';

export const $message = {
  info: (x: string) => Message.info(x),
  success: (x: string) => Message.success(x),
  warning: (x: string) => Message.warning(x),
  error: (x: string) => Message.error(x),
  fuck(obj: any) {
    if (axios.isCancel(obj)) return console.info('Request canceled: ', obj.message.url);
    if (obj === 'cancel') return console.info('User canceled');

    console.error(obj);
    if (obj == null) {
      return Message.error('未知错误，请联系管理员');
    } else if (obj instanceof Error) {
      return Message.error(obj.message);
    } else if (typeof obj !== 'object') {
      return Message.error(obj + '');
    } else if (obj.data || obj.response && obj.response.data) {
      const data = obj.data || obj.response.data;
      return Message.error(data.detail || data.error || data.message || data.msg || data.retMsg || JSON.stringify(data));
    } else {
      return Message.error(obj.detail || obj.error || obj.message || obj.msg || obj.retMsg || JSON.stringify(obj));
    }
  },
};
