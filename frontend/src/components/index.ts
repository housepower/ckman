import Vue from 'vue';

import 'font-awesome/scss/font-awesome.scss';

export * from '@/common/components';
import { TimeFilter } from './time-filter';
import { default as Breadcrumb } from './breadcrumb.vue';
import { default as OverviewBase } from './overview-base/overview-base.vue';

// eslint-disable-next-line
import VueDraggableResizable from 'vue-draggable-resizable';
import 'vue-draggable-resizable/dist/VueDraggableResizable.css';

Vue.component('vue-draggable-resizable', VueDraggableResizable);

Object.entries({
  TimeFilter,
  Breadcrumb,
  // 第三方库
  VueDraggableResizable,
  OverviewBase,
}).forEach(([name, component]) => Vue.component(name, component));

export {
  TimeFilter,
  Breadcrumb,
  OverviewBase,
};
