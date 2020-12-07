/**
 * 将对象中的 JSON 字符串属性值反序列化回对象
 *
 * @param obj 原始对象
 * @param props 需要转化的属性名列表
 */
export function parseObjectProps(obj: any, props: string[]) {
  const result = Object.assign({}, obj);
  if (Array.isArray(props)) {
    props.forEach(key => {
      try {
        result[key] = JSON.parse(obj[key]);
      } catch (e) {
        console.warn('解析 JSON 字符串失败！');
        result[key] = undefined;
      }
    });
  } else {
    throw new TypeError('props 必须为数组类型');
  }

  return result;
}
