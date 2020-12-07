export function minMax(arr: number[]) {
  const res = [Infinity, -Infinity];
  // eslint-disable-next-line @typescript-eslint/prefer-for-of
  for (let i = 0; i < arr.length; ++i) {
    const value = arr[i];
    res[0] = value < res[0] ? value : res[0];
    res[1] = value > res[1] ? value : res[1];
  }
  return res;
}
