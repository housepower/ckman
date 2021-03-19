<template>
  <div class="zkTable">
    <div class="title flex flex-between flex-vcenter ptb-10">
      <span class="fs-20 font-bold">Zookeeper Status</span>
      <time-filter v-model="timeFilter"
                   :refreshDuration.sync="refresh"
                   @input="timeFilterChange"
                   @on-refresh="timeFilterRefresh" />
    </div>
    <el-table class="tb-edit"
              :data="tableData"
              border
              style="width: 100%">
      <el-table-column v-for="(col, index) in cols"
                       :key="index"
                       :label="col"
                       align="center">
        <template slot="header"
                  slot-scope="{ column }">
          <span>{{ column.label }}</span>
        </template>
        <template slot-scope="{ row, column }">
          <span v-if="index === 0">{{ Object.keys(row)[0] }}</span>
          <span v-else>{{ Object.values(row)[0][column.label] }}</span>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>
<script>
import { cloneDeep, pull, uniq } from "lodash-es";
import { TablesApi } from "@/apis";
export default {
  data() {
    return {
      cols: [""],
      keys: [""],
      tableData: [],
      timeFilter: null,
      refresh: null,
    };
  },
  mounted() {
    this.fetchData();
  },
  methods: {
    async fetchData() {
      const {
        data: { entity },
      } = await TablesApi.zkStatus(this.$route.params.id);
      this.cols = [""];
      this.keys = [""];
      this.tableData = [];
      entity.forEach((item) => {
        this.cols.push(item.host);
        this.keys = pull(Object.keys(item), "host");
      });
      this.keys.forEach((key) => {
        let tableItem = {
          [key]: {},
        };
        entity.forEach((item) => {
          tableItem[key][item["host"]] = item[key];
          this.tableData.push(tableItem);
        });
        this.tableData = uniq(this.tableData);
      });
    },
    timeFilterChange() {
      this.fetchData();
    },
    timeFilterRefresh() {
      this.fetchData();
    },
  },
};
</script>

<style lang="scss" scoped></style>
