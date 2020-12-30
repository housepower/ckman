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
      <el-form-item label="Node Shard:"
                    prop="shard">
        <el-input-number v-model="formModel.shard"
                         :step="1"
                         :min="numberRange[0]"
                         :max="numberRange[1]"></el-input-number>
      </el-form-item>
    </el-form>
  </section>
</template>
<script>
import { ClusterApi } from "@/apis";
export default {
  props: ["numberRange"],
  data() {
    return {
      formModel: {
        ip: "",
        shard: 1,
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
