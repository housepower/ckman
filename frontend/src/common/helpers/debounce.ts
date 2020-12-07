import { Decorator } from '../VueComponentBase';

export function Debounce(wait: number): Decorator {
  return function decorate(clazz: any, fn: string) {
    const func = clazz[fn];
    const handleKey = `__${fn}__debounce_handle`;
    return {
      enumerable: false,
      value(...args) {
        if (this[handleKey]) clearTimeout(this[handleKey]);
        this[handleKey] = setTimeout(() => func.apply(this, args), wait);
        return () => {
          clearTimeout(this[handleKey]);
          this[handleKey] = undefined;
        };
      },
    };
  };
}
