<template>
  <el-table :data="list" border align="center">
    <el-table-column prop="version" show-overflow-tooltip label="Query Start Time" align="center" />
    <el-table-column prop="files" show-overflow-tooltip label="Query Duration" align="center" />
    <el-table-column prop="version" show-overflow-tooltip label="Query" align="center" />
    <el-table-column prop="files" show-overflow-tooltip label="Initial User" align="center" />
    <el-table-column prop="version" show-overflow-tooltip label="Initial Query ID" align="center" />
    <el-table-column prop="files" show-overflow-tooltip label="Initial Address" align="center" />
    <el-table-column prop="files" show-overflow-tooltip label="Thread Numbers" align="center" />
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
