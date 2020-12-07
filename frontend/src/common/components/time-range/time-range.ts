import ResizeObserver from 'resize-observer-polyfill';

import { Component, Inreactive, Prop, Ref, VueComponentBase, Watch } from '../../VueComponentBase';
import { VueEcharts } from '../vue-echarts';

@Component()
export default class TimeRange extends VueComponentBase {
  @Ref() readonly Chart: VueEcharts;
  @Ref() readonly ContentBox: HTMLDivElement;
  @Prop() readonly value: [Date | number, Date | number];
  @Prop() readonly dateRange: [Date | number, Date | number];
  @Prop() readonly disabledMinWindow: boolean = true;
  /** 最小时间窗口(秒) */
  @Prop() readonly minWindow: number;
  @Prop() readonly minWindowUnit: 'd' | 'h' | 'm' | 's' = 's';
  @Prop() readonly chartOption: Record<string, any>;
  @Prop() readonly chartTheme: string;
  @Prop() readonly styleObj = {
    content: '',
    chart: '',
    config: '',
  };
  @Prop() readonly showAggInfo = true;
  @Prop() readonly count = 1000;

  @Inreactive ro: ResizeObserver = null;
  @Inreactive readonly timeUnit = [
    { value: 'd', label: '天' },
    { value: 'h', label: '小时' },
    { value: 'm', label: '分钟' },
    { value: 's', label: '秒' },
  ];
  type: 'left' | 'right' = null;
  btnStyle = {
    left: 0,
    right: 1,
    oldLeft: 0,
    oldRight: 1,
    width: 8,
  };
  boxWidth = null;
  selectedWidth = null;
  moveX = 0;
  /** 聚合窗口(秒) */
  aggWindow: number = null;

  async mounted() {
    await this.$nextTick();
    this.updateBoxWidth();
    this.init(this.value, null);
    this.ro = new ResizeObserver(this.updateBoxWidth);
    this.ro.observe(this.ContentBox);
  }

  beforeDestroy() {
    this.ro.disconnect();
  }

  @Watch('dateRange')
  @Watch('value')
  init(newValue, oldValue) {
    if (oldValue && newValue[0] === oldValue[0] && newValue[1] === oldValue[1]) return;
    const minRange = +new Date(this.dateRange[0]);
    const maxRange = +new Date(this.dateRange[1]);
    const range = maxRange - minRange;
    const minValue = +new Date(this.value[0]);
    const maxValue = +new Date(this.value[1]);

    let flag = false;
    if (minRange > minValue) {
      this.btnStyle.left = 0;
      flag = true;
    } else {
      this.btnStyle.left = (minValue - minRange) / range;
    }
    if (maxRange < maxValue) {
      this.btnStyle.right = 1;
      flag = true;
    } else {
      this.btnStyle.right = (maxValue - minRange) / range;
    }
    if (flag) {
      this.$emit('input', [Math.max(minRange, minValue), Math.min(maxRange, maxValue)]);
    } else {
      this.updateSelectedWidth();
      this.updateAggWindow();
    }
  }

  get aggWindowText() {
    let time = null;
    if (!this.aggWindow || !this.minWindowUnit) return time;
    switch (this.minWindowUnit) {
      case 'd':
        time = this.aggWindow / (60 * 60 * 24);
        break;
      case 'h':
        time = this.aggWindow / (60 * 60);
        break;
      case 'm':
        time = this.aggWindow / 60;
        break;
      case 's':
        time = this.aggWindow;
        break;
    }
    return time + this.windowUnitLabel;
  }

  get windowUnitLabel() {
    return this.timeUnit.find(x => x.value === this.minWindowUnit)?.label || '';
  }

  removeEventListener() {
    removeEventListener('mousemove', this.btnMove);
    removeEventListener('mousemove', this.dragMove);
    removeEventListener('mouseup', this.removeEventListener);
    if (this.btnStyle.left !== this.btnStyle.oldLeft || this.btnStyle.right !== this.btnStyle.oldRight) {
      this.btnStyle.oldLeft = this.btnStyle.left;
      this.btnStyle.oldRight = this.btnStyle.right;

      const range = +new Date(this.dateRange[1]) - +new Date(this.dateRange[0]);
      const min = +new Date(this.dateRange[0]) + Math.ceil(range * this.btnStyle.left);
      const max = +new Date(this.dateRange[0]) + Math.floor(range * this.btnStyle.right);
      this.$emit('input', [min, max]);
    }
  }

  updateBoxWidth() {
    this.boxWidth = this.ContentBox.offsetWidth;
  }

  updateSelectedWidth() {
    this.selectedWidth = this.btnStyle.right - this.btnStyle.left - this.btnStyle.width / this.boxWidth;
  }

  btnMousedown(event: MouseEvent, type: 'left' | 'right') {
    this.type = type;
    this.moveX = event.pageX - { left: this.btnStyle.left, right: this.btnStyle.right }[type] * this.boxWidth;
    addEventListener('mousemove', this.btnMove);
    addEventListener('mouseup', this.removeEventListener);
  }

  btnMove(e: MouseEvent) {
    let x = e.pageX - this.moveX;
    if (x <= 0) x = 0;
    if (this.type === 'left') {
      if (x > this.btnStyle.right * this.boxWidth) {
        this.type = 'right';
      } else {
        const max = this.boxWidth + this.btnStyle.width;
        if (x >= max) x = max;
        this.btnStyle.left = x / this.boxWidth;
      }
    } else {
      if (x < this.btnStyle.left * this.boxWidth) {
        this.type = 'left';
      } else {
        if (x >= this.boxWidth) x = this.boxWidth;
        this.btnStyle.right = x / this.boxWidth;
      }
    }
    this.updateSelectedWidth();
  }

  drag(event) {
    this.moveX = event.pageX - this.btnStyle.left * this.boxWidth;
    addEventListener('mousemove', this.dragMove);
    addEventListener('mouseup', this.removeEventListener);
  }

  dragMove(e: MouseEvent) {
    let x = e.pageX - this.moveX;
    if (x <= 0) x = 0;
    const max = this.boxWidth - (this.selectedWidth * this.boxWidth + this.btnStyle.width);
    if (x >= max) x = max;
    this.btnStyle.left = x / this.boxWidth;
    this.btnStyle.right = this.btnStyle.left + this.selectedWidth + this.btnStyle.width / this.boxWidth;
  }

  async updateMinWindow(minWindow: number) {
    this.$emit('update:minWindow', minWindow);
    await this.$nextTick();
    this.updateAggWindow();
  }

  async updateMinWindowUnit(minWindowUnit: 'd' | 'h' | 'm' | 's' = 's') {
    this.$emit('update:minWindowUnit', minWindowUnit);
    await this.$nextTick();
    this.updateAggWindow();
  }

  updateAggWindow() {
    if (!this.minWindow) return;
    const min = +new Date(this.value[0]);
    const max = +new Date(this.value[1]);
    // 计算聚合窗口： 时间差 / (1000（需要的点数）* 1000(豪秒))  -->  (max - min) / 1e6
    // 聚合窗口必须是最小颗粒度的倍数
    let aggWindow = this.minWindow;
    if (max - min > 1000 * this.count) {
      const minWindow = {
        d: this.minWindow * 60 * 60 * 24,
        h: this.minWindow * 60 * 60,
        m: this.minWindow * 60,
        s: this.minWindow,
      }[this.minWindowUnit];
      const win = Math.ceil((max - min) / (1000 * this.count));
      aggWindow = Math.floor(win / minWindow) * minWindow || minWindow;
    }
    this.aggWindow = aggWindow;
    this.$emit('aggWindowChange', aggWindow);
  }
}
