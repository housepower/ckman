import { CreateElement } from 'vue';

import { Component, VueComponentBase } from '../VueComponentBase';

@Component({ name: 'ChildViewHolder' })
export class ChildViewHolder extends VueComponentBase {
  render(h: CreateElement) {
    return <router-view key={+this.$route.params.id || 0} />;
  }
}
