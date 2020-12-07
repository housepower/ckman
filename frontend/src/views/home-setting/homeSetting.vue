<template>
  <main class="home-setting">
    <breadcrumb :data="['setting']"></breadcrumb>
    <el-form ref="Form" :model="formModel" label-width="280px" label-position="left">
      <el-form-item label="Ckman HA Pair Addresses:" prop="haAddress">
        <el-input v-model="formModel.haAddress" class="width-350" />
      </el-form-item>
      <el-form-item label="Short-term Prometheus Pair Addresses:" prop="shortAddress">
        <el-input v-model="formModel.shortAddress" class="width-350" />
      </el-form-item>
      <el-form-item label="long-term Prometheus Addresses:" prop="longAddress">
        <el-input v-model="formModel.longAddress" class="width-350" />
      </el-form-item>
      <el-form-item label="Alert Manager Addresses:" prop="alertAddress">
        <el-input v-model="formModel.alertAddress" class="width-350" />
      </el-form-item>
    </el-form>
    <el-button type="primary" class="ml-360">Save & Reboot</el-button>
    <section class="rpms">
      <el-divider content-position="left">ClickHouse RPMs</el-divider>
      <rpm-list />
    </section>
  </main>
</template>
<script>
import RpmList from "./component/rpms";
export default {
  name: "HomeSetting",
  data() {
    return {
      formModel: {
        haAddress: "",
        shortAddess: "",
        longAddress: "",
        alertAddress: "",
      },
    };
  },
  mounted() {},
  methods: {},
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
