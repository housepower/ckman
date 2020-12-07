import type { Root } from '@/models';

export let $root: Root;

export function _updateVueInstance(vue: Root) {
  $root = vue;
}
