import { Component, Prop, VueComponentBase } from '../VueComponentBase';

export interface TopNotifyModel {
  title: string;
  type: string;
  showIcon: boolean;
  popperClass?: string;
  close: () => void;
}

@Component({
  name: 'SharpTooltip',
})
export class TopNotify extends VueComponentBase implements TopNotifyModel {
  @Prop() title: string;
  @Prop() type = 'error';
  @Prop() showIcon = true;
  @Prop() popperClass: string;
  @Prop() close: () => void;

  render(h) {
    return (
      <transition
        name="el-fade-in-linear"
        appear={true}><div
          role="top-notify"
          hidden={this.title === ''}
          style={{width: '100%', position: 'relative', zIndex: 99999, whiteSpace: 'pre-line', padding: 0}}
          class={[
            this.popperClass,
          ]}>
          <el-alert
            title={this.title}
            type={this.type}
            show-icon={this.showIcon}
            onClose={this.close}/>
        </div>
      </transition>
    );
  }
}
