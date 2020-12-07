import { VueProgress } from '../components/vue-progressbar';
import { $root } from './root';

export const $progress = {} as VueProgress;

[
  'start',
  'setPercent',
  'increase',
  'decrease',
  'hide',
  'pause',
  'finish',
  'fail',
  'isRunning',
].forEach(key => {
  $progress[key] = (...args) => {
    if (!$root || !$root.$refs) return;
    $root.$refs.progressbar[key](...args);
  };
});
