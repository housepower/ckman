<template>
  <el-table :data="list"
            border
            align="center">
    <el-table-column prop="startTime"
                     show-overflow-tooltip
                     label="Query Start Time"
                     align="center"
                     #default="{ row }">
      <span>{{ row.startTime * 1000 | formatDate }}</span>
    </el-table-column>
    <el-table-column prop="queryDuration"
                     show-overflow-tooltip
                     label="Query Duration"
                     width="120"
                     align="center" />
    <el-table-column prop="query"
                     show-overflow-tooltip
                     label="Query"
                     align="center" />
    <el-table-column prop="user"
                     show-overflow-tooltip
                     label="Initial User"
                     width="120"
                     align="center" />
    <el-table-column prop="queryId"
                     show-overflow-tooltip
                     label="Initial Query ID"
                     align="center" />
    <el-table-column prop="address"
                     show-overflow-tooltip
                     label="Initial Address"
                     align="center" />
    <el-table-column prop="threads"
                     show-overflow-tooltip
                     label="Thread Numbers"
                     width="120"
                     align="center" />
  </el-table>
</template>
<script>
export default {
  props: {
    list: {
      type: Array,
      default: [],
    },
  },
  data() {
    return {};
  },
  mounted() {},
  components: {},
};
</script>

<style></style>
