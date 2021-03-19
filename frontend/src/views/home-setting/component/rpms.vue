<template>
  <section class="rpms text-right">
    <el-button type="primary"
               class="mb-15"
               @click="chooseFile">Upload RPMs</el-button>
    <el-table :data="list"
              border>
      <el-table-column prop="version"
                       show-overflow-tooltip
                       label="Version"
                       align="center" />
      <el-table-column prop="files"
                       show-overflow-tooltip
                       label="Files"
                       align="center"
                       min-width="500" />
      <el-table-column label="Action"
                       #default="{ row }"
                       align="center">
        <template>
          <i class="fa fa-trash pointer fs-18"
             v-tooltip="'Delete'"
             @click="remove(row)" />
        </template>
      </el-table-column>
    </el-table>
  </section>
</template>
<script>
import { PackageApi } from "@/apis";
import { $loading, $modal } from "@/services";
import Upload from "./upload";
export default {
  data() {
    return {
      list: [],
    };
  },
  mounted() {
    this.fetchData();
  },
  methods: {
    async fetchData() {
      this.list = [];
      const {
        data: { entity },
      } = await PackageApi.getList();
      entity.forEach((item) => {
        this.list.push({
          version: item,
          files: `clickhouse-client-${item}-2.noarch.rpm,clickhouse-common-static-${item}-2.x86_64.rpm,clickhouse-server-${item}-2.noarch.rpm`,
        });
      });
    },
    async chooseFile() {
      await $modal({
        props: {
          title: "Upload File",
          width: "650px",
          cancelText: "Cancel",
          okText: "Upload",
        },
        component: Upload,
      });
      this.fetchData();
    },
    async remove(item) {
      await this.$confirm("Confirm whether to delete ?", "Tip", {
        confirmButtonText: "Delete",
        cancelButtonText: "Cancel",
        text: "warning",
      });
      await PackageApi.deletePackage({ packageVersion: item.version });
      this.$message.success(`${item.version}版本已删除成功`);
      this.fetchData();
    },
  },
};
</script>

<style></style>
