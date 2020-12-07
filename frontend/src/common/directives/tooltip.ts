import Vue from 'vue';

import { $root } from '../../services';

Vue.directive('tooltip', {
  inserted(el, { value }) {
    const model = value != null && typeof value === 'object' ? {
      ...value,
      target: el,
    } : {
      content: value,
      target: el,
    };

    el.addEventListener('mouseenter', () => {
      $root.tooltips.push(model);
    });
    el.addEventListener('mouseleave', () => {
      $root.tooltips.splice($root.tooltips.indexOf(model), 1);
    });
    el.addEventListener('blur', () => {
      $root.tooltips.splice($root.tooltips.indexOf(model), 1);
    });
    (el as any).__tooltipModel = model;
  },
  update(el, { value }) {
    const { __tooltipModel: model } = el as any;
    if (value != null && typeof value === 'object') {
      Object.assign(model, value);
    } else {
      model.content = value;
    }
  },
  unbind(el) {
    const { __tooltipModel: model } = el as any;
    const index = $root.tooltips.indexOf(model);
    if (index >= 0) $root.tooltips.splice(index, 1);
  },
});
