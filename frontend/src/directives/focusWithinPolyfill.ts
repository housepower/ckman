import Vue from 'vue';

const nativeFocusWithinSupport = (() => {
  const style = document.createElement('style');
  style.innerHTML = ':focus-within{}';
  document.head.appendChild(style);
  const result = !!(style.sheet as unknown as CSSStyleSheet).cssRules.length;
  document.head.removeChild(style);
  return result;
})();

if (nativeFocusWithinSupport) {
  document.documentElement.classList.add('native-focus-within');

  Vue.directive('focusWithinPolyfill', {});
} else {
  document.documentElement.classList.add('polyfilled-focus-within');

  const useFocusIn = 'onfocusin' in document;
  Vue.directive('focusWithinPolyfill', {
    bind(el: HTMLElement, { value: className = 'focus-within' }) {
      const handleClass = (() => {
        let count = 0;
        let rafFlag = false;

        return (n: number) => {
          count += n;
          if (!rafFlag) {
            rafFlag = true;
            requestAnimationFrame(() => {
              rafFlag = false;
              if (count) {
                el.classList.add(className);
              } else {
                el.classList.remove(className);
              }
            });
          }
        };
      })();

      const handleFocusEvent = (el as any).handleFocusEvent = () => handleClass(1);
      const handleBlurEvent = (el as any).handleBlurEvent = () => handleClass(-1);

      if (useFocusIn) {
        el.addEventListener('focusin', handleFocusEvent);
        el.addEventListener('focusout', handleBlurEvent);
      } else {
        el.addEventListener('focus', handleFocusEvent, true);
        el.addEventListener('blur', handleBlurEvent, true);
      }
    },
    unbind(el: any) {
      const handleFocusEvent = el.handleFocusEvent;
      const handleBlurEvent = el.handleBlurEvent;

      if (useFocusIn) {
        el.removeEventListener('focusin', handleFocusEvent, true);
        el.removeEventListener('focusout', handleBlurEvent, true);
      } else {
        el.removeEventListener('focus', handleFocusEvent, true);
        el.removeEventListener('blur', handleBlurEvent, true);
      }
    },
  });
}
