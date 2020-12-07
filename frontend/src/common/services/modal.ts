import Vue, { ComponentOptions, VueConstructor } from 'vue';

import { DrawerModel, ModalModel } from '../models';
import { $root } from './root';

export interface ModalPropModel {
  childComponent: Vue | VueConstructor<Vue> | ComponentOptions<any>;
  data: any;
  props: any;
  resolve: (value: any) => void;
}

function formatParams(
  { component: childComponent, data = {}, props = {} }: ModalModel | DrawerModel,
  modalList: ModalPropModel[],
) {
  if (!childComponent) {
    throw new TypeError('Field component is required');
  }

  if (props.cancelText === undefined) {
    props.cancelText = '取消';
  }
  if (props.okText === undefined) {
    props.okText = props.cancelText === null ? '关闭' : '确定';
  }

  const modalProp: ModalPropModel = {
    childComponent,
    data,
    props,
    resolve: null,
  };

  return new Promise<any>((resolve: () => void) => {
    modalProp.resolve = resolve;
    modalList.push(modalProp);
  }).finally(() => {
    modalList.splice(modalList.indexOf(modalProp), 1);
  });
}

export function $modal(args: ModalModel) {
  if (typeof args.props.width === 'number') args.props.width = args.props.width + 'px';
  return formatParams(args, $root.modals);
}

export function $drawer(args: DrawerModel) {
  if (typeof args.props.size === 'number') args.props.size = args.props.size + 'px';
  return formatParams(args, $root.drawers);
}
