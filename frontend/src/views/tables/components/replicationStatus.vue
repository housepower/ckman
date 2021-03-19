<template>
  <div class="replication-status pb-20">
    <div class="title flex flex-between flex-vcenter ptb-10">
      <span class="fs-20 font-bold">Table Replication Status</span>
      <time-filter v-model="timeFilter"
                   :refreshDuration.sync="refresh"
                   @input="timeFilterChange"
                   @on-refresh="timeFilterRefresh" />
    </div>
    <el-table class="tb-edit"
              :data="tableData"
              :header-cell-style="mergeTableHeader"
              border
              style="width: 100%">
      <el-table-column v-for="(col, index) in cols"
                       :key="index"
                       :label="col.label"
                       :prop="col.prop"
                       ref="tableColumn"
                       width="auto"
                       align="center">
        <template slot="header"
                  slot-scope="{ column }">
          <span>{{ column.label }}</span>
        </template>
        <template slot-scope="{ row, column }">
          <span v-if="index === 0">{{ Object.keys(row)[0] }}</span>
          <span v-else>{{ Object.values(row)[0][column.property] }}</span>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>
<script>
import { uniqWith, isEqual, cloneDeep } from "lodash-es";
import { TablesApi } from "@/apis";
export default {
  data() {
    return {
      cols: [],
      tableData: [],
      headerData: [],
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
        data: {
          entity: { header = [], tables = [] },
        },
      } = await TablesApi.replicationStatus(this.$route.params.id);
      this.cols = [{ prop: "", label: "" }];
      this.headerData = cloneDeep(header);
      this.tableData = [];
      let tableNameItem = {},
        tableItem = {};
      header.forEach((item, index) => {
        const shard = `shard${index + 1}`;
        item.forEach((v, index) => {
          tableNameItem[`${shard}_${index}`] = v;
          this.cols.push({
            prop: `${shard}_${index}`,
            label: shard,
          });
        });
      });
      this.tableData.push({
        ["Table Name"]: tableNameItem,
      });
      tables.forEach(({ name, values }) => {
        values.forEach((val, index) => {
          const shard = `shard${index + 1}`;
          val.forEach((v, index) => {
            tableItem[`${shard}_${index}`] = v;
          });
          this.tableData.push({
            [name]: tableItem,
          });
        });
        this.tableData = uniqWith(this.tableData, isEqual);
      });
    },
    mergeTableHeader({ row, column, rowIndex, columnIndex }) {
      const [len] = new Set(this.headerData.map((item) => item.length));
      if (rowIndex === 0) {
        if (columnIndex != 0) {
          if (columnIndex % len === 0) {
            return {
              display: "none",
            };
          } else {
            this.$nextTick(() => {
              const trList = document.querySelector(
                ".replication-status thead>tr"
              ).children;
              trList[columnIndex] && (trList[columnIndex].colSpan = 2);
            });
          }
        }
      }
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

<style lang="scss" scoped>
.replication-status {
  border-bottom: 1px solid var(--color-gray);
}
</style>
