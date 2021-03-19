<template>
  <main class="home-setting">
    <breadcrumb :data="['setting']"></breadcrumb>
    <el-form ref="Form"
             :model="formModel"
             label-width="280px"
             label-position="left">
      <el-form-item label="Ckman HA Pair Addresses:"
                    prop="peers">
        <el-input v-model="formModel.peers"
                  placeholder="多个ip逗号分隔"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="Prometheus Addresses:"
                    prop="prometheus">
        <el-input v-model="formModel.prometheus"
                  placeholder="多个ip逗号分隔"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="Alert Manager Addresses:"
                    prop="alertManagers">
        <el-input v-model="formModel.alertManagers"
                  placeholder="多个ip逗号分隔"
                  class="width-350" />
      </el-form-item>
    </el-form>
    <el-button type="primary"
               class="ml-360"
               @click="saveConfig">Save & Reboot</el-button>
    <section class="rpms">
      <el-divider content-position="left">ClickHouse RPMs</el-divider>
      <rpm-list />
    </section>
  </main>
</template>
<script>
import RpmList from "./component/rpms";
import { PackageApi } from "@/apis";
import { lineFeed } from "@/helpers";

export default {
  name: "HomeSetting",
  data() {
    return {
      formModel: {
        peers: "",
        prometheus: "",
        alertManagers: "",
      },
    };
  },
  mounted() {
    this.fetchData();
  },
  methods: {
    async fetchData() {
      const {
        data: { entity },
      } = await PackageApi.getConfig();
      Object.keys(entity).forEach((key) => {
        this.formModel[key] = entity[key] ? entity[key].join(",") : "";
      });
    },
    async saveConfig() {
      Object.keys(this.formModel).forEach((key) => {
        this.formModel[key] = this.formModel[key]
          ? lineFeed(this.formModel[key])
          : null;
      });
      await PackageApi.updateConfig(this.formModel);
      this.$message.success("配置已更新");
    },
  },
  components: {
    RpmList,
  },
};
</script>

<style lang="scss" scoped>
.home-setting {
  .ml-360 {
    margin-left: 360px;
  }
}
</style>
