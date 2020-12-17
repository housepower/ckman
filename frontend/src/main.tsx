import 'core-js';
import Vue from 'vue';

import '@/app';
import '@/components';
import '@/directives';
import { Component } from '@/common/VueComponentBase';
import { $router, _updateVueInstance } from '@/services';

Component.registerHooks([
  'beforeRouteEnter',
  'beforeRouteUpdate',
  'beforeRouteLeave',
]);

import ElementUI from 'element-ui';
import locale from 'element-ui/lib/locale/lang/en';

Vue.use(ElementUI, { locale });

Vue.config.productionTip = false;


const data = {
  modals: [],
  loading: {
    status: 0,
    text: '',
  },
  clusterBench: null,
  tooltips: [],
  notifys: [],
  userInfo: null,
};
_updateVueInstance(data as any);
new Vue({
  data,
  router: $router,
  render() {
    return (
      <div id="app" style="height: 100%">
        <vue-progressbar ref="progressbar" />
        {this.notifys.map(attrs => <top-notify {...{ attrs }} />)}
        <router-view />
        {this.modals.map(attrs => <sharp-modal {...{ attrs }} />)}
        {this.loading.status ? <v-loading /> : null}
        {this.tooltips.map(attrs => <sharp-tooltip {...{ attrs }} />)}
      </div>
    );
  },
  created() {
    _updateVueInstance(this);
  },
}).$mount('#app');

// if ($router.currentRoute.name !== 'login') {
//   $router.push({name: 'Home'});
// }
