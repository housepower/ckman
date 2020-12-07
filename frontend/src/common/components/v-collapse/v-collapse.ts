import { Component, Prop, VueComponentBase, Watch } from '../../VueComponentBase';

@Component()
export default class VCollapse extends VueComponentBase {
  @Prop() options: {
    title?: string;
    class?: string;
    style?: string;
  } = {};
  @Prop() title: string;
  @Prop() value = false;

  show = false;

  created() {
    this.show = this.value;
  }

  @Watch('value')
  watchShow(value: boolean) {
    this.show = value;
  }
}
