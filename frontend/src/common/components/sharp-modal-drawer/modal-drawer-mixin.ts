import type { Dialog, Drawer } from 'element-ui';
import Vue from 'vue';
import type { VueConfiguration, VueConstructor } from 'vue/types/vue';

import { Component, Prop, Ref, VueComponentBase } from '../../VueComponentBase';

@Component()
export default class ModalDrawerMixin extends VueComponentBase {
  @Ref() readonly body: Vue & {
    onOk?(modal: ModalDrawerMixin): any;
    onCancel?(modal: ModalDrawerMixin): any;
    onBeforeClose?(modal: ModalDrawerMixin): any;
  };
  @Ref() readonly modal: Drawer | Dialog;

  @Prop() readonly props: Record<string, any>;
  @Prop() readonly data: { [key: string]: any };
  @Prop() readonly resolve: (value: any) => void;
  @Prop({ type: [Vue, Function, Object] }) readonly childComponent: Vue | VueConstructor | VueConfiguration;
  @Prop() readonly classes: string[];

  visible = false;
  loading = false;
  resultValue: any;
  isOk = false;

  mounted() {
    this.$nextTick(() => this.visible = true);
  }

  onBeforeClose(done: () => void) {
    if (!this.body.onBeforeClose) return done();

    const result = this.body.onBeforeClose(this);
    if (result && typeof result.then === 'function') {
      Promise.resolve(result).then(done);
    } else {
      done();
    }
  }

  close(value?: any, isOk = false) {
    this.visible = false;
    this.resultValue = value;
    this.isOk = isOk;
  }

  onHidden() {
    if (this.isOk) {
      this.resolve(this.resultValue);
    } else {
      this.resolve(Promise.reject(this.resultValue == null ? 'cancel' : this.resultValue));
    }
  }

  onOk() {
    const result = this.body.onOk ? this.body.onOk(this) : this.body;
    if (result && typeof result.then === 'function') {
      this.loading = true;
      Promise.resolve(result)
        .then(value => this.close(value, true))
        .finally(() => this.loading = false);
    } else {
      this.close(result);
      this.isOk = true;
    }
  }

  onCancel() {
    if (this.body.onCancel) {
      this.body.onCancel(this);
    } else {
      this.close('cancel');
    }
  }
}
