import ResizeObserver from 'resize-observer-polyfill';

import { Component, Prop, Ref, VueComponentBase, Inreactive } from '../../VueComponentBase';

@Component()
export default class VCollapseText extends VueComponentBase {
  @Ref() readonly contentBox: HTMLDivElement;

  @Prop() readonly content: string;
  @Prop({ default: 2 }) readonly lineClamp: number;

  @Inreactive ro: ResizeObserver;

  isExceed = false;
  isWarp = true;

  get computedClass() {
    return this.isWarp ? `text-line-clamp` : 'text-wrap';
  }

  async mounted() {
    await this.$nextTick();
    this.ro = new ResizeObserver(this.calcExceed);
    this.ro.observe(this.contentBox);
  }

  beforeDestroy() {
    this.ro.disconnect();
  }

  calcExceed([entry]: ResizeObserverEntry[]) {
    if (this.isWarp) this.isExceed = entry.contentRect.height < entry.target.scrollHeight;
  }

  collapseChange() {
    if (!this.isExceed) return;
    this.isWarp = !this.isWarp;
  }
}
