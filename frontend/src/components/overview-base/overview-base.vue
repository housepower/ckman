<template>
  <main class="settings">
    <breadcrumb :data="breadcrumbInfo"></breadcrumb>
    <section class="container">
      <div class="title flex flex-vcenter flex-between">
        <span class="fs-18 font-bold mtb-20">ClickHouse Node KPIs</span>
        <time-filter v-model="timeFilter"
                     :refreshDuration.sync="refresh"
                     @input="timeFilterChange"
                     @on-refresh="timeFilterRefresh" />
      </div>
      <ul class="charts flex flex-between flex-wrap">
        <li class="chart-item mb-50"
            v-for="(item, index) of chartMetrics"
            :key="index">
          <p class="mtb-10 fs-16 font-bold">{{ item.expect }}</p>
          <vue-echarts v-if="item.option"
                       ref="Charts"
                       :option="item.option"
                       @mousemove.native="mousemove('series', $event)" />
        </li>
      </ul>
    </section>
  </main>
</template>
<script>
import echarts from "echarts";
import { chartOption } from "./chartOption";
import { MetricApi } from "@/apis";
import { convertTimeBounds } from "@/helpers";

export default {
  props: {
    breadcrumbInfo: {
      type: Array,
      default: [],
    },
    metrics: {
      type: Array,
      default: [],
    },
  },
  data() {
    return {
      timeFilter: ["now-1h", "now"],
      refresh: null,
      chartOption: null,
      chartMetrics: [],
    };
  },
  mounted() {
    this.metrics.forEach(({ expect, metric }) => {
      this.chartMetrics.push({
        expect,
        metric,
        option: null,
      });
    });
    this.fetchData();
  },
  methods: {
    fetchData() {
      this.chartMetrics.forEach((item, index) => {
        this.fetchChartData(item, index);
      });
    },
    async fetchChartData(chart, index) {
      const { duration, min, max } = convertTimeBounds(this.timeFilter);
      const step = Math.floor(+duration / 360 / 1000);
      const {
        data: { data },
      } = await MetricApi.queryRangeMetric({
        metric: chart.metric,
        start: Math.floor(min / 1000),
        end: Math.floor(max / 1000),
        step,
      });
      this.$set(chart, "option", chartOption(data, min, max));
      this.$nextTick(() => {
        this.$refs.Charts[index] && this.$refs.Charts[index].refreshChart();
        const chartInstances = this.$refs.Charts.map((item) => item.chart);
        echarts.connect(chartInstances);
      });
    },
    mousemove(params, $event) {},
    timeFilterChange() {
      this.fetchData();
    },
    timeFilterRefresh() {
      this.fetchData();
    },
  },
  components: {},
};
</script>

<style lang="scss" scoped>
.chart-item {
  height: 500px;
  width: 49%;
}
</style>
