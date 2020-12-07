import moment, { Duration } from 'moment';

import { Component, Prop, VueComponentBase, Watch } from '@/common/VueComponentBase';
import { TimeUnits } from '@/constants';
import { parseDuration, parseTime } from '@/helpers';

function newDateWithoutMilliseconds(args?: (number | string | Date)) {
  const date = args ? new Date(args) : new Date();
  return new Date(date.setMilliseconds(0));
}

function durationFilter(value: string | Duration) {
  if (typeof value === 'string') { // '5d'
    const [ , num, unit, trunc ] = /^(\d+)([smhdwMy])(?:\/(\2))?/.exec(value);
    if (trunc) {
      if (+num === 0) {
        if (trunc === 'd' || trunc === 'y') {
          return '今' + TimeUnits[trunc];
        } else {
          return '本' + TimeUnits[trunc];
        }
      } else {
        throw new TypeError('不支持的时间类型');
      }
    } else {
      return `${num} ${TimeUnits[unit]}`;
    }
  } else {
    if (value < moment.duration(1, 'm')) {
      return value.get('s') + ' 秒';
    } else {
      return value.humanize();
    }
  }
}

@Component({
  filters: {
    duration(value: Duration) {
      if (value == null) return;
      return durationFilter(value);
    },
    timefilterDuration(value: string) {
      if (value == null) return;
      if (value.includes('/')) {
        return durationFilter(value) + '迄今';
      } else {
        return '最近 ' + durationFilter(value);
      }
    },
    timeUnit(value: string) {
      return TimeUnits[value];
    },
  },
})
export default class TimeFilter extends VueComponentBase {
  @Prop() readonly hideRefresh: boolean;
  @Prop() readonly value: (string | number)[];
  @Prop() refreshDuration: string;

  readonly TimeUnits = TimeUnits;
  relativeTimes = [
    '15m',
    '30m',
    '1h',
    '4h',
    '12h',
    '24h',
    '7d',
    '1M',
    '3M',
    '6M',
    '1y',
    '2y',
    '0d/d',
    '0w/w',
    '0M/M',
    '0y/y',
  ];
  realTimes = [
    '5s',
    '10s',
    '30s',
    '45s',
    '1m',
    '5m',
    '15m',
    '30m',
    '1h',
    '2h',
    '12h',
    '1d',
  ];
  isDropdownOpen = false;
  display: {
    type?: string;
    value?: string | number;
    number?: number;
    unit?: string;
    moment?: number;
    start?: string | number;
    end?: string | number;
  } = { type: 'fast' };
  activeTab = 'fast';
  now = new Date();
  relativeValue = null;
  absoluteValue = [];
  refreshPromise = null;
  defaultValue = null;

  @Watch('value', { immediate: true })
  valueChange(val: any[]) {
    if (!val) return;
    if (!Array.isArray(val) || val.length !== 2) {
      throw new TypeError('玩法是：[ 开始时间, 结束时间 ]');
    }

    const [ start, end ] = val.map(parseTime);

    if (start.type === 'relative' && end.type === 'relative') {
      if (-start.moment > -end.moment) {
        throw new TypeError('开始时间在结束时间之后');
      }
      if (end.number) {
        throw new TypeError('不支持的结束时间（目前只支持结束时间为当前时间）');
      }
      this.display = {
        type: this.relativeTimes.includes(start.value as string) ? 'fast' : 'relative',
        value: start.value,
        number: start.number,
        unit: start.unit,
        moment: +moment.duration(start.moment as number),
      };
      // $state.go($state.current, {
      //   start: 'now' + (start.value !== '0d' ? `-${start.value}` : ''),
      //   end: 'now' + (end.value !== '0d' ? `-${end.value}` : ''),
      // });
    } else if (start.type === 'absolute' && end.type === 'absolute') {
      if (start.value >= end.value) {
        throw new TypeError('开始时间在结束时间之后');
      }
      this.display = {
        type: 'absolute',
        start: start.value,
        end: end.value,
      };
      // $state.go($state.current, {
      //   start: start.value,
      //   end: end.value,
      // });
    } else {
      throw new TypeError('不支持的时间段类型');
    }
  }

  destroyed() {
    clearInterval(this.refreshPromise);
  }

  setRefresh(t: string) {
    console.log(t)
    clearInterval(this.refreshPromise);

    if (t) {
      const duration = +parseDuration(t);
      this.$emit('update:refreshDuration', t);
      this.refreshPromise = setInterval($event => {
        this.$emit('on-refresh', $event);
      }, duration);
    } else {
      this.$emit('update:refreshDuration', null);
    }
  }

  setRelative(t: string) {
    this.$emit('input', ['now-' + t, 'now']);
  }

  initRelative() {
    this.now = newDateWithoutMilliseconds();
    switch (this.display.type) {
      case 'fast':
      case 'relative':
        this.defaultValue = this.display.number;
        break;

      case 'absolute': {
        const duration = moment.duration(this.display.end as number - (this.display.start as number));
        const unit = ['y', 'M', 'd', 'h', 'm', 's', 'ms'].find(u => duration.get(u as any));
        this.defaultValue = duration.get(unit as any);
        this.setRelative(this.defaultValue + unit);
        break;
      }
    }
    this.relativeValue = this.defaultValue;
  }

  setAbsolute() {
    this.$emit('input', this.absoluteValue.map(x => +new Date(x)));
  }

  initAbsolute() {
    this.now = newDateWithoutMilliseconds();
    switch (this.display.type) {
      case 'fast':
      case 'relative':
        this.absoluteValue = [
          newDateWithoutMilliseconds(+this.now - this.display.moment),
          newDateWithoutMilliseconds(this.now),
        ];
        this.setAbsolute();
        break;

      case 'absolute':
        this.absoluteValue = [
          newDateWithoutMilliseconds(this.display.start),
          newDateWithoutMilliseconds(this.display.end),
        ];
        break;
    }
  }

  switchTab(pane) {
    switch (pane.name) {
      case 'fast':
      case 'relative':
        this.initRelative();
        break;

      case 'absolute':
        this.initAbsolute();
        break;
    }
  }
}
