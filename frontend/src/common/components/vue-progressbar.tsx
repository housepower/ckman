import { Component, Prop, VueComponentBase } from '../VueComponentBase';

export interface VueProgress {
  start(time?: number): void;
  setPercent(num: number): void;
  increase(num: number): void;
  decrease(num: number): void;
  hide(): void;
  pause(): void;
  finish(): void;
  fail(): void;
  isRunning(): boolean;
}

@Component({
  name: 'VueProgressbar',
})
export class VueProgressbar extends VueComponentBase implements VueProgress {
  @Prop() color = '#73ccec';
  @Prop() failedColor = 'red';
  @Prop() thickness = '2px';
  @Prop() transition = {
    speed: '0.5s',
    opacity: '1s',
    termination: 300,
  };
  @Prop() noPeg = false;

  canSuccess = true;
  show = false;
  percent = 0;
  timer = null;

  start(time = 3000) {
    this.setPercent(0);
    clearInterval(this.timer);
    this.timer = setInterval(() => {
      this.increase(10000 / time * Math.random());
    }, 100);
  }

  setPercent(num: number) {
    this.show = true;
    this.canSuccess = true;
    this.percent = num;
  }

  increase(num: number) {
    this.percent = Math.min(99, this.percent + num);
  }

  decrease(num: number) {
    this.percent = Math.max(0, this.percent - num);
  }

  hide() {
    this.pause();
    setTimeout(() => {
      this.show = false;
    }, this.transition.termination);
  }

  pause() {
    clearInterval(this.timer);
    this.timer = null;
  }

  finish() {
    this.percent = 100;
    this.hide();
  }

  fail() {
    this.canSuccess = false;
  }

  isRunning() {
    return !!this.timer;
  }

  get style() {
    return {
      zIndex: '999999',
      color: this.canSuccess ? this.color : this.failedColor,
      backgroundColor: 'currentColor',
      opacity: this.show ? '1' : '0',
      position: 'fixed',
      top: '0',
      left: '0',
      width: this.percent + '%',
      height: this.thickness,
      borderRadius: '0 999px 999px 0',
      transition: (this.show ? `width ${this.transition.speed}, ` : '') + `opacity ${this.transition.opacity} linear`,
      contain: 'layout size',
    } as unknown as CSSStyleDeclaration;
  }

  get pegStyle() {
    return {
      borderRadius: '100%',
      boxShadow: 'currentColor 1px 0 6px 1px',
      height: '100%',
      opacity: '.45',
      position: 'absolute',
      right: '0',
      top: '0',
      width: '70px',
    } as CSSStyleDeclaration;
  }

  render(h) {
    return <div style={this.style}>{this.noPeg ? null : <i style={this.pegStyle}></i>}</div>;
  }
}
