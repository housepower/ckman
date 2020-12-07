export function jsonStableStringify(obj, space: any = 2) {
  const allKeys = [];
  JSON.stringify(obj, (key, value) => {
    allKeys.push(key);
    return value;
  });
  allKeys.sort();
  return JSON.stringify(obj, allKeys, space);
}
