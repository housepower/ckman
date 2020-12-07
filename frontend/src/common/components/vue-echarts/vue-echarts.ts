import echarts from 'echarts';
import ResizeObserver from 'resize-observer-polyfill';
import type { CreateElement } from 'vue';

import { PrimaryColor } from '../../constants';
import { Component, Inreactive, Prop, VueComponentBase, Watch } from '../../VueComponentBase';

@Component()
export class VueEcharts extends VueComponentBase {
  @Prop() readonly option: Record<string, any>;
  @Prop() readonly theme: string = 'default';
  @Prop() readonly groupId: string;
  @Prop() readonly loadingOption = {
    text: '努力加载中',
    color: PrimaryColor || '#c23531',
    textColor: PrimaryColor || '#489CFF',
    maskColor: 'rgba(0, 0, 0, 0.1)',
    zlevel: 0,
  };
  @Prop() readonly initCfg: Record<string, any>;

  @Inreactive ro: ResizeObserver;
  @Inreactive resizing: boolean;
  @Inreactive chart: any;

  $el: HTMLDivElement;

  render(h: CreateElement) {
    return h('div', { staticClass: 'vue-echarts' });
  }

  mounted() {
    this.refreshChart();
    this.ro = new ResizeObserver(([entry]) => {
      if (entry.contentRect.width && entry.contentRect.height && this.chart && !this.resizing) {
        this.resizing = true;
        requestAnimationFrame(() => {
          if (this.chart) this.chart.resize(entry.contentRect);
          this.resizing = false;
        });
      }
    });

    this.ro.observe(this.$el);
  }

  beforeDestroy() {
    if (this.chart) {
      this.chart.dispose();
      this.chart = undefined;
    }
    if (this.ro) {
      this.ro.disconnect();
      this.ro = undefined;
    }
  }

  @Watch('option')
  refreshOption() {
    if (!this.chart) return;
    if (this.option && Object.keys(this.option).some(x => /^[a-z]/.test(x))) {
      this.chart.setOption(this.option, true);
      if (this.$el.clientHeight) this.chart.resize();
      this.chart.hideLoading();
    } else {
      this.chart.showLoading('default', this.loadingOption);
    }
  }

  @Watch('theme')
  refreshChart() {
    if (this.chart) {
      this.chart.dispose();
      this.chart = undefined;
    }

    const chart = echarts.init(this.$el, this.theme, this.initCfg);
    chart.group = this.groupId;

    this.chart = chart;

    this.refreshOption();

    // http://echarts.baidu.com/api.html#events
    [
      'click',
      'dblclick',
      'mousedown',
      'mouseup',
      'mousemove',
      'mouseover',
      'mouseout',
      'globalout',

      'legendselectchanged',
      'legendselected',
      'legendunselected',
      'legendscroll',
      'datazoom',
      'datarangeselected',
      'timelinechanged',
      'timelineplaychanged',
      'restore',
      'dataviewchanged',
      'magictypechanged',
      'geoselectchanged',
      'geoselected',
      'geounselected',
      'pieselectchanged',
      'pieselected',
      'pieunselected',
      'mapselectchanged',
      'mapselected',
      'mapunselected',
      'axisareaselected',
      'focusnodeadjacency',
      'unfocusnodeadjacency',
      'brush',
      'brushselected',
    ].forEach(x => {
      if (typeof this.$listeners[x] === 'function') {
        chart.on(x, event => this.$emit(x, event, this));
      }
    });
  }

  setOption(option: Record<string, any>, notMerge?: boolean, lazyUpdate?: boolean): void;
  setOption(option: Record<string, any>, opts?: Record<string, any>): void;

  setOption(option: Record<string, any>, ...args) {
    return this.chart.setOption(option, ...args);
  }

  dispatchAction(payload: Record<string, any>) {
    return this.chart.dispatchAction(payload);
  }
}
