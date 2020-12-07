import Popper from 'popper.js';

import { Component, Inreactive, Prop, VueComponentBase } from '../VueComponentBase';

export interface TooltipPropModel {
  content: string;
  target: HTMLElement;
  popperClass: string;
  effect: 'dark' | 'light';
  placement: Popper.Placement;
}

@Component({
  name: 'SharpTooltip',
})
export class SharpTooltip extends VueComponentBase implements TooltipPropModel {
  @Prop() content: string;
  @Prop() target: HTMLElement;
  @Prop() popperClass: string;
  @Prop() effect: 'dark' | 'light' = 'dark';
  @Prop() placement: Popper.Placement = 'top';

  @Inreactive popper: Popper;

  doCreate(el: HTMLElement) {
    this.popper = new Popper(this.target, el, {
      placement: this.placement,
      modifiers: {
        preventOverflow: { enabled: false },
        hide: { enabled: false },
      },
    });
  }

  doDestroy() {
    this.popper.destroy();
  }

  render(h) {
    return (
      <transition
        name="el-fade-in-linear"
        appear={true}
        onBeforeEnter={this.doCreate}
        onAfterLeave={this.doDestroy}>
        <div
          role="tooltip"
          hidden={this.content == null}
          style={{zIndex: 99999, whiteSpace: 'pre-line'}}
          class={[
            'el-tooltip__popper',
            'is-' + this.effect,
            this.popperClass,
          ]}>
          <div class="popper__arrow" x-arrow="" />
          {this.content}
        </div>
      </transition>
    );
  }
}
