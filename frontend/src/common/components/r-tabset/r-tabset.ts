import { Component, Prop, VueComponentBase } from '../../VueComponentBase';

@Component()
export default class RTabset extends VueComponentBase {
  @Prop() readonly keepAlive: boolean;
  @Prop() readonly props: any;
  @Prop() readonly navStyle: any;
  @Prop({ type: [String, Number] }) readonly viewKey: string | number;
  @Prop() readonly customRouterView: boolean;
}
