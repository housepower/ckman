<template>
  <div class="table-metric pb-20">
    <div class="title flex flex-between flex-vcenter ptb-10">
      <span class="fs-20 font-bold">Table Metrics</span>
    </div>
    <el-table :data="tableData"
              cneter
              border>
      <template v-for="{ prop, label } of columns">
        <el-table-column :prop="prop"
                         :label="label"
                         :key="prop"
                         show-overflow-tooltip />
      </template>

    </el-table>
  </div>

</template>
<script>
import { TablesApi } from "@/apis";
export default {
  data() {
    return {
      tableData: [],
      columns: [
        {
          prop: "tableName",
          label: "Table Name",
        },
        {
          prop: "columns",
          label: "Columns",
        },
        {
          prop: "rows",
          label: "Rows",
        },
        {
          prop: "parts",
          label: "Parts",
        },
        {
          prop: "space",
          label: "Disk Space",
        },
        {
          prop: "completedQueries",
          label: "Completed Queries in last 24h",
        },
        {
          prop: "failedQueries",
          label: "Failed Queries in last 24h",
        },
        {
          prop: "queryCost",
          label: "Queries Cost(0.5, 0.99, max) in last 7 days",
        },
      ],
    };
  },
  mounted() {
    this.fetchData();
  },
  methods: {
    async fetchData() {
      const {
        data: { entity },
      } = await TablesApi.tableMetrics(this.$route.params.id);
      Object.entries(entity).forEach(([key, values]) => {
        const {
          columns,
          rows,
          space,
          completedQueries,
          failedQueries,
          parts,
          queryCost,
        } = values;
        this.tableData.push({
          tableName: key,
          columns,
          rows,
          space,
          completedQueries,
          failedQueries,
          parts,
          queryCost: Object.values(queryCost)
            .map((v) => `${v / 1000}s`)
            .join(","),
        });
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.table-metric {
  border-bottom: 1px solid var(--color-gray);
}
</style>