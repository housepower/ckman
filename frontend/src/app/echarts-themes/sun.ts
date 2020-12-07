/* tslint:disable */
import echarts from 'echarts';

echarts.registerTheme('sun', {
  color: [
    '#00c8c1',
    '#323da8',
    '#60b00e',
    '#d15766',
    '#f0b23b',
    '#ffe073',
    '#6a58e0',
    '#24aae3',
    '#f0c471',
    '#e687dc',
  ],
  textStyle: {},
  title: {
    textStyle: {
      color: '#333333',
    },
    subtextStyle: {
      color: '#aaa',
    },
  },
  line: {
    itemStyle: {
      normal: {
        borderWidth: 1,
      },
    },
    lineStyle: {
      normal: {
        width: 2,
      },
    },
    symbolSize: 4,
    symbol: 'emptyCircle',
    smooth: false,
  },
  radar: {
    itemStyle: {
      normal: {
        borderWidth: 1,
      },
    },
    lineStyle: {
      normal: {
        width: 2,
      },
    },
    symbolSize: 4,
    symbol: 'emptyCircle',
    smooth: false,
  },
  bar: {
    itemStyle: {
      normal: {
        barBorderWidth: 0,
        barBorderColor: '#ccc',
      },
      emphasis: {
        barBorderWidth: 0,
        barBorderColor: '#ccc',
      },
    },
  },
  pie: {
    itemStyle: {
      normal: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
      emphasis: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
    },
  },
  scatter: {
    itemStyle: {
      normal: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
      emphasis: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
    },
  },
  boxplot: {
    itemStyle: {
      normal: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
      emphasis: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
    },
  },
  parallel: {
    itemStyle: {
      normal: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
      emphasis: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
    },
  },
  sankey: {
    itemStyle: {
      normal: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
      emphasis: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
    },
  },
  funnel: {
    itemStyle: {
      normal: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
      emphasis: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
    },
  },
  gauge: {
    itemStyle: {
      normal: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
      emphasis: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
    },
  },
  candlestick: {
    itemStyle: {
      normal: {
        color: '#c23531',
        color0: '#314656',
        borderColor: '#c23531',
        borderColor0: '#314656',
        borderWidth: 1,
      },
    },
  },
  graph: {
    itemStyle: {
      normal: {
        borderWidth: 0,
        borderColor: '#ccc',
      },
    },
    lineStyle: {
      normal: {
        width: 1,
        color: '#aaa',
      },
    },
    symbolSize: 4,
    symbol: 'emptyCircle',
    smooth: false,
    color: [
      '#00c8c1',
      '#323da8',
      '#60b00e',
      '#d15766',
      '#f0b23b',
      '#ffe073',
      '#6a58e0',
      '#24aae3',
      '#f0c471',
      '#e687dc',
    ],
    label: {
      normal: {
        textStyle: {
          color: '#eee',
        },
      },
    },
  },
  map: {
    itemStyle: {
      normal: {
        areaColor: '#e5eff0',
        borderColor: '#5d7876',
        borderWidth: 0.5,
      },
      emphasis: {
        areaColor: 'rgba(0,184,146,0.8)',
        borderColor: '#444',
        borderWidth: 1,
      },
    },
    label: {
      normal: {
        textStyle: {
          color: '#000',
        },
      },
      emphasis: {
        textStyle: {
          color: 'rgb(0,94,100)',
        },
      },
    },
  },
  geo: {
    itemStyle: {
      normal: {
        areaColor: '#e5eff0',
        borderColor: '#5d7876',
        borderWidth: 0.5,
      },
      emphasis: {
        areaColor: 'rgba(0,184,146,0.8)',
        borderColor: '#444',
        borderWidth: 1,
      },
    },
    label: {
      normal: {
        textStyle: {
          color: '#000',
        },
      },
      emphasis: {
        textStyle: {
          color: 'rgb(0,94,100)',
        },
      },
    },
  },
  categoryAxis: {
    axisLine: {
      show: true,
      lineStyle: {
        color: '#808080',
      },
    },
    axisTick: {
      show: false,
      lineStyle: {
        color: '#808080',
      },
    },
    axisLabel: {
      show: true,
      textStyle: {
        color: '#333',
      },
    },
    splitLine: {
      show: true,
      lineStyle: {
        color: [
          '#ededed',
        ],
      },
    },
    splitArea: {
      show: false,
      areaStyle: {
        color: [
          'rgba(250,250,250,0.3)',
          'rgba(200,200,200,0.3)',
        ],
      },
    },
  },
  valueAxis: {
    axisLine: {
      show: true,
      lineStyle: {
        color: '#808080',
      },
    },
    axisTick: {
      show: false,
      lineStyle: {
        color: '#808080',
      },
    },
    axisLabel: {
      show: true,
      textStyle: {
        color: '#333',
      },
    },
    splitLine: {
      show: true,
      lineStyle: {
        color: [
          '#ededed',
        ],
      },
    },
    splitArea: {
      show: false,
      areaStyle: {
        color: [
          'rgba(250,250,250,0.3)',
          'rgba(200,200,200,0.3)',
        ],
      },
    },
  },
  logAxis: {
    axisLine: {
      show: true,
      lineStyle: {
        color: '#808080',
      },
    },
    axisTick: {
      show: false,
      lineStyle: {
        color: '#808080',
      },
    },
    axisLabel: {
      show: true,
      textStyle: {
        color: '#333',
      },
    },
    splitLine: {
      show: true,
      lineStyle: {
        color: [
          '#ededed',
        ],
      },
    },
    splitArea: {
      show: false,
      areaStyle: {
        color: [
          'rgba(250,250,250,0.3)',
          'rgba(200,200,200,0.3)',
        ],
      },
    },
  },
  timeAxis: {
    axisLine: {
      show: true,
      lineStyle: {
        color: '#808080',
      },
    },
    axisTick: {
      show: false,
      lineStyle: {
        color: '#808080',
      },
    },
    axisLabel: {
      show: true,
      textStyle: {
        color: '#333',
      },
    },
    splitLine: {
      show: true,
      lineStyle: {
        color: [
          '#ededed',
        ],
      },
    },
    splitArea: {
      show: false,
      areaStyle: {
        color: [
          'rgba(250,250,250,0.3)',
          'rgba(200,200,200,0.3)',
        ],
      },
    },
  },
  toolbox: {
    iconStyle: {
      normal: {
        borderColor: '#999',
      },
      emphasis: {
        borderColor: '#666',
      },
    },
  },
  legend: {
    textStyle: {
      color: 'rgba(51,51,51,1)',
    },
  },
  tooltip: {
    axisPointer: {
      lineStyle: {
        color: '#ccc',
        width: 1,
      },
      crossStyle: {
        color: '#ccc',
        width: 1,
      },
    },
  },
  timeline: {
    lineStyle: {
      color: '#293c55',
      width: 1,
    },
    itemStyle: {
      normal: {
        color: '#293c55',
        borderWidth: 1,
      },
      emphasis: {
        color: '#a9334c',
      },
    },
    controlStyle: {
      normal: {
        color: '#293c55',
        borderColor: '#284469',
        borderWidth: 0.5,
      },
      emphasis: {
        color: '#293c55',
        borderColor: '#284469',
        borderWidth: 0.5,
      },
    },
    checkpointStyle: {
      color: '#e43c59',
      borderColor: 'rgba(194,53,49, 0.5)',
    },
    label: {
      normal: {
        textStyle: {
          color: '#293c55',
        },
      },
      emphasis: {
        textStyle: {
          color: '#293c55',
        },
      },
    },
  },
  visualMap: {
    color: [
      '#db3311',
      '#d88273',
      '#f6efa6',
    ],
  },
  dataZoom: {
    backgroundColor: 'rgba(255,255,255,0.1)',
    dataBackgroundColor: 'rgba(210,83,83,0.25)',
    fillerColor: 'rgba(0,200,193,0.1)',
    handleColor: '#00c8c1',
    handleSize: '100%',
    textStyle: {
      color: '#333333',
    },
  },
  markPoint: {
    label: {
      normal: {
        textStyle: {
          color: '#eee',
        },
      },
      emphasis: {
        textStyle: {
          color: '#eee',
        },
      },
    },
  },
});
