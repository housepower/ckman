import { Component, Prop, VueComponentBase, Watch } from '../VueComponentBase';

@Component()
export class NumberAnimation extends VueComponentBase {
  @Prop({ type: [Number, String] }) readonly value: any;

  handle: number;

  @Watch('value')
  valueChange(value, oldValue = 0) {
    if (this.handle) cancelAnimationFrame(this.handle);
    if (typeof value !== 'number') {
      this.$el.textContent = value;
    } else {
      let start;
      const str = value + '';
      const index = str.indexOf('.');
      const round = index > -1 ? str.length - index - 1 : 0;
      const step = t => {
        start = start || t;
        if (t - start >= 500) {
          this.$el.textContent = value + '';
        } else {
          this.$el.textContent = (oldValue + (value - oldValue) * (t - start) / 500).toFixed(round);
          this.handle = requestAnimationFrame(step);
        }
      };
      this.handle = requestAnimationFrame(step);
    }
  }

  render(h) {
    return <span class="font-mono"></span>;
  }

  mounted() {
    this.$el.textContent = this.value;
  }

  beforeDestroy() {
    if (this.handle) cancelAnimationFrame(this.handle);
  }
}
