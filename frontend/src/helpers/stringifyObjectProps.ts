/**
 * 将传入参数中的对象属性值序列化为 JSON 字符串便于保存数据库
 *
 * @param obj 原始对象
 * @param props 需要转化的属性名列表。如不传，则转换所有对象类型的属性值
 */
export function stringifyObjectProps(obj: any, props?: string[]) {
  const result = Object.assign({}, obj);
  if (Array.isArray(props)) {
    props.forEach(key => {
      if (typeof obj[key] === 'object') {
        result[key] = JSON.stringify(obj[key]);
      }
    });
  } else {
    Object.entries(result).forEach(([ key, value ]) => {
      if (typeof value === 'object') {
        result[key] = JSON.stringify(value);
      }
    });
  }

  return result;
}
