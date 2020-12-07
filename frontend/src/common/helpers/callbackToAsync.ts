export function callbackToAsync(fn: (...args) => Promise<any> | any) {
  return (...argsWithCallback) => {
    const callback = argsWithCallback.reduce((prev, curr) => typeof curr === 'function' ? curr : prev) as () => void;
    Promise.resolve(fn(...argsWithCallback)).then(callback);
  };
}
