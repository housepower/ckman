<template>
  <section class="add-node">
    <el-form ref="Form"
             :model="formModel"
             label-width="150px">
      <el-form-item label="New Node IP:"
                    prop="ip">
        <el-input v-model="formModel.ip"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="Node Node Shard:"
                    prop="shard">
        <el-input v-model="formModel.shard"
                  class="width-350" />
      </el-form-item>
    </el-form>
  </section>
</template>
<script>
import { ClusterApi } from "@/apis";
export default {
  data() {
    return {
      formModel: {
        ip: "",
        shard: "",
      },
    };
  },
  methods: {
    async onOk() {
      const { ip, shard } = this.formModel;
      await ClusterApi.addClusterNode(this.$route.params.id, {
        ip,
        shard: +shard,
      });
    },
  },
};
</script>

<style></style>
