import Vue from 'vue';

Vue.directive('ref', {
  bind(el: HTMLElement, { value }: { value?: (x: Vue) => void }, { componentInstance }: { componentInstance?: Vue }) {
    value(componentInstance);
  },
});
