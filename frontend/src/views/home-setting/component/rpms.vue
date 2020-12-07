<template>
  <section class="rpms text-right">
    <el-upload class="ml-360"
               action=""
               multiple
               :show-file-list="false"
               :http-request="beforeUpload"
               accept=".rpm">
      <el-button type="primary"
                 class="mb-15">Upload RPMs</el-button>
    </el-upload>

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
import { $loading } from "@/services";
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
      const {
        data: { data },
      } = await PackageApi.getList();
      data.forEach((item) => {
        this.list.push({
          version: item,
          files: `clickhouse-client-${item}.noarch.rpm,clickhouse-common-static-${item}.x86_64.rpm,clickhouse-server-${item}.noarch.rpm`,
        });
      });
    },
    beforeUpload(file) {
      if (file.file.size > 200 * 1024 ** 2) {
        this.$message.error("文件不能超过200M");
        return;
      }
      let formData = new FormData();
      formData.append("package", file.file);
      $loading.increase();
      PackageApi.upload(formData)
        .then(() => {
          this.$message({
            type: "success",
            message: `${file.file.name}上传成功`,
            duration: 5000,
          });
        })
        .finally(() => $loading.decrease());
    },
    async remove(item) {
      console.log(item);
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
