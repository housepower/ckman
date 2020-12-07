import { format } from 'date-fns';
import { groupBy } from 'lodash-es';
import { MetricData } from '@/models';
export const chartOption = (data: MetricData[], min: number, max: number) => ({
  tooltip: {
    trigger: 'axis',
    axisPointer: {
      lineStyle: {
        color: 'red',
      },
    },
    confine: true,
    enterable: true,
    formatter(params) {
      return `${format(params[0].axisValue, 'yyyy-MM-dd hh:mm:ss')}<br/>` + Object.entries(groupBy(params, 'seriesName')).map(([ip, arr]) => `${arr[0].marker}${ip}${arr.sort((a,b) => b.value[1] - a.value[1]).map(x=> `: ${x.value[1]}`).join('<br/>')}`).join('<br/>');
    },
  },
  legend: {
    left: 'center',
    type: 'scroll',
    top: -5,
  },
  dataZoom: [{
    show: true,
    height: 25,
    xAxisIndex: 0,
    bottom: 10,
  }],
  grid: {
    top: 'middle',
    left: '3%',
    right: '5.5%',
    bottom: '3%',
    height: '80%',
    containLabel: true,
  },
  xAxis: {
    type: 'time',
    name: '时间',
    axisLine: {
      lineStyle: {
        color: '#999',
      },
    },
    axisTick: {
      show: false,
    },
    min,
    max,
  },
  yAxis: {
    type: 'value',
    splitLine: {
      lineStyle: {
        type: 'dashed',
        color: '#DDD',
      },
    },
    nameTextStyle: {
      color: '#999',
    },
    name: '值',
  },
  series: data.map(({metric, values}) => ({
    name: (() => {
      if(metric.instance) {
        if(metric.device) return `${metric.instance}-${metric.device}`;
        if(metric.gc) return `${metric.instance}-${metric.gc}`;
        return `${metric.instance}`;
      } else {
        return `job:${metric.job}-task:${metric.task}`;
      }
    })(),
    data: values.map(item => ([item[0] * 1000, item[1]])),
    type: 'line',
    // sampling: 'average',
    symbol: 'none',
  })),
});
