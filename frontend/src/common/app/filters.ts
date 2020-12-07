import { lightFormat } from 'date-fns';
import Vue from 'vue';

const filters = {
  // 日期格式化
  formatDate(value: Date | number | string, d = 'yyyy-MM-dd HH:mm:ss') {
    return value == null || value === '' ? '-' : lightFormat(typeof value === 'string' ? new Date(value) : value, d);
  },
  formatTime(value: Date | number | string) {
    return value == null ? '-' : new Date(value).toISOString().slice(11, -5);
  },
  percent(value = 0, round = 0) {
    return (value * 100).toFixed(round) + '%';
  },
  trueFalse(val: boolean) {
    return val ? '真' : '假';
  },
  yesNo(val: boolean) {
    return val ? '是' : '否';
  },
};

Object.entries(filters).forEach(([key, value]) => Vue.filter(key, value));
