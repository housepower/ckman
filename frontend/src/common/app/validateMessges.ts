import AsyncValidator from 'async-validator';

Object.assign((AsyncValidator as any).messages, {
  default: () => '字段验证错误，请检查',
  required: () => 'Required fields',
  enum: (_, a) => `必须是 ${a} 其中之一`,
  whitespace: () => '该字段不能为空',
  date: {
    format: () => '非法的日期格式',
    parse: () => '非法的日期格式',
    invalid: () => '非法的日期格式',
  },
  types: {
    string: () => '必须是一个字符串',
    method: () => '必须是一个方法 (函数)',
    array: () => '必须是一个数组',
    object: () => '必须是一个对象',
    number: () => '必须是一个数字',
    date: () => '必须是一个日期',
    boolean: () => '必须是一个布尔值',
    integer: () => '必须是一个整数',
    float: () => '必须是一个浮点数',
    regexp: () => '必须是一个合法的正则表达式',
    email: () => '必须是一个合法的 Email 地址',
    url: () => '必须是一个合法的 URL',
    hex: () => '必须是一个合法的 16 进制数',
  },
  string: {
    len: (_, a) => `必须是 ${a} 个字符长度`,
    min: (_, a) => `必须至少是 ${a} 个字符长度`,
    max: (_, a) => `不能超过 ${a} 个字符长度`,
    range: (_, a, b) => `必须在 ${a} 和 ${b} 个字符长度之间`,
  },
  number: {
    len: (_, a) => `必须等于 ${a}`,
    min: (_, a) => `不能小于 ${a}`,
    max: (_, a) => `不能大于 ${a}`,
    range: (_, a, b) => `必须在 ${a} 和 ${b} 之间`,
  },
  array: {
    len: (_, a) => `必须包含 ${a} 个元素`,
    min: (_, a) => `必须至少包含 ${a} 个元素`,
    max: (_, a) => `至多包含 ${a} 个元素`,
    range: (_, a, b) => `长度必须在 ${a} 和 ${b} 之间`,
  },
  pattern: {
    mismatch: (_, a, b) => `必须匹配 ${b} 规则`,
  },
});
