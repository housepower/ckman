import { Component, VueComponentBase } from '../../VueComponentBase';

function preventScrolling(e: MouseWheelEvent) {
  e.preventDefault();
}

@Component()
export default class VLoading extends VueComponentBase {
  mounted() {
    document.body.addEventListener('wheel', preventScrolling, { passive: false, capture: true });
  }

  beforeDestroy() {
    document.body.removeEventListener('wheel', preventScrolling, { capture: true });
  }
}
