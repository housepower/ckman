<template>
  <section class="createCk">
    <el-form ref="Form"
             :model="formModel"
             label-width="150px">
      <el-form-item label="ClickHouse Version:"
                    prop="packageVersion"
                    v-if="type">
        <el-input v-model="formModel.packageVersion"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="Cluster Name:"
                    prop="cluster">
        <el-input v-model="formModel.cluster"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="ClickHouse Node IP:"
                    prop="hosts"
                    v-if="!type">
        <el-input v-model="formModel.hosts"
                  type="textarea"
                  :autosize="{ minRows: 2 }"
                  placeholder="多个ip,请以逗号,分隔填写"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="ClickHouse TCP Port:"
                    prop="port">
        <el-input v-model="formModel.port"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="ClickHouse Node List:"
                    prop="hosts"
                    placeholder="多个ip,请以逗号,分隔填写"
                    v-if="type">
        <el-input type="textarea"
                  :autosize="{ minRows: 2 }"
                  v-model="formModel.hosts"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="Replica"
                    v-if="type">
        <el-switch v-model="formModel.replica"></el-switch>
      </el-form-item>
      <el-form-item label="Zookeeper Node List:"
                    prop="zkNodes">
        <el-input type="textarea"
                  :autosize="{ minRows: 2 }"
                  placeholder="多个ip,请以逗号,分隔填写"
                  v-model="formModel.zkNodes"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="ZooKeeper Port:"
                    prop="zkPort">
        <el-input v-model="formModel.zkPort"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="Data path:"
                    prop="path"
                    v-if="type">
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
                  type="password"
                  show-password
                  autocomplete="new-password"
                  class="width-350" />
      </el-form-item>

      <el-form-item label="SSH Username:"
                    prop="sshUser"
                    v-if="type">
        <el-input v-model="formModel.sshUser"
                  class="width-350" />
      </el-form-item>
      <el-form-item label="SSH Password:"
                    prop="sshPassword"
                    v-if="type">
        <el-input v-model="formModel.sshPassword"
                  type="password"
                  show-password
                  autocomplete="new-password"
                  class="width-350" />
      </el-form-item>

    </el-form>
  </section>
</template>
<script>
import { ClusterApi } from "@/apis";
export default {
  props: ["type"],
  data() {
    return {
      formModel: {
        packageVersion: "",
        cluster: "",
        hosts: "",
        zkNodes: "",
        user: "",
        password: "",
        sshUser: "",
        sshPassword: "",
        isReplica: false,
        port: 9000,
        zkPort: 2181,
        path: "",
      },
    };
  },
  mounted() {},
  methods: {
    async onOk() {
      const {
        packageVersion,
        cluster,
        hosts,
        zkNodes,
        user,
        password,
        sshUser,
        sshPassword,
        isReplica,
        port,
        zkPort,
        path,
      } = this.formModel;
      if (!this.type) {
        await ClusterApi.importCluster({
          cluster,
          hosts: hosts.split(","),
          port: +port,
          user,
          password,
          // sshPassword,
          // sshUser,
          zkNodes: zkNodes.split(","),
          zkPort: +zkPort,
        });
      } else {
        await ClusterApi.createCluster({
          clickhouse: {
            ckTcpPort: +port,
            clusterName: cluster,
            isReplica: isReplica,
            packageVersion,
            password,
            path,
            user,
            zkNodes: zkNodes.split(","),
            zkPort: +zkPort,
          },
          hosts: hosts.split(","),
          password: sshPassword,
          user: sshUser,
        });
      }
    },
  },
  components: {},
};
</script>

<style lang="scss" scoped></style>
