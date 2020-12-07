import { $root } from '../services';

export const $loading = {
  increase(text = '') {
    const { loading } = $root;
    loading.status++;
    loading.text = text;
  },
  decrease() {
    setTimeout(() => $root.loading.status--);
  },
};
