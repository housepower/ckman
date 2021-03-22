<template>
  <main class="settings">
    <breadcrumb :data="['Clusters', $route.params.id, 'settings']"></breadcrumb>
    <el-form ref="Form"
             :model="formModel"
             label-width="150px">
      <el-form-item label="Replica">
        <el-switch v-model="formModel.isReplica"></el-switch>
      </el-form-item>
      <el-form-item label="Zookeeper Node List:"
                    prop="zkNodes">
        <el-input type="textarea"
                  :autosize="{ minRows: 2 }"
                  v-model="formModel.zkNodes"
                  placeholder="多个ip,请以逗号,分隔填写"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="Data path:"
                    prop="path">
        <el-input v-model="formModel.path"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="Cluster Username:"
                    prop="user">
        <el-input v-model="formModel.user"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="Cluster Password:"
                    prop="password">
        <el-input v-model="formModel.password"
                  class="width-350" />
      </el-form-item>
    </el-form>
    <el-button type="primary"
               class="ml-200"
               @click="save">Save & Reboot</el-button>
  </main>
</template>
<script>
import { ClusterApi } from "@/apis";
import { isNull } from "lodash-es";
import { lineFeed } from "@/helpers";

export default {
  data() {
    return {
      formModel: {
        zkNodes: "",
        user: "",
        path: "",
        password: "",
        isReplica: false,
      },
    };
  },
  mounted() {
    this.fetchData();
  },
  methods: {
    async fetchData() {
      const clusterName = this.$route.params.id;
      if (isNull(this.$root.clusterBench)) {
        const {
          data: { entity },
        } = await ClusterApi.getCluster();
        this.formModel = entity[`${clusterName}`];
      } else {
        this.formModel = this.$root.clusterBench;
      }
    },
    async save() {
      await ClusterApi.updateCluster(
        Object.assign({}, this.formModel, {
          zkNodes: lineFeed(this.formModel.zkNodes),
          hosts: lineFeed(this.formModel.hosts),
        })
      );
      this.$message.success("更新成功");
    },
  },
  components: {},
};
</script>

<style lang="scss" scoped>
.ml-200 {
  margin-left: 200px;
}
</style>
