<template>
  <section class="home">
    <section class="flex flex-wrap">
      <div class="flex-1">
        <div class="title flex flex-column mb-30">
          <p class="mb-5"
             style="font-size: 40px">
            <i class="fa fa-database"></i>
            <span class="inline-block ml-10 font-bold">Provision</span>
          </p>
          <p class="fs-14 font-bold">Create or import a ClickHouse Cluster</p>
        </div>
        <div class="btns flex flex-column width-6">
          <el-button type="primary"
                     size="large"
                     class="mb-20 fs-18"
                     @click="importCk(1)">Create a ClickHouse Cluster</el-button>
          <el-button type="primary"
                     size="large"
                     class="fs-18"
                     @click="importCk(0)">Import a ClickHouse Cluster</el-button>
        </div>
      </div>
      <div class="flex-1">
        <div class="title flex flex-column mb-30">
          <p class="mb-5"
             style="font-size: 40px">
            <i class="fa fa-database"></i>
            <span class="inline-block ml-10 font-bold">Loader</span>
          </p>
          <p class="fs-14 font-bold">Data loader management</p>
        </div>
        <div class="btns flex flex-column width-6">
          <router-link to="/loader"
                       class="el-button mb-20 fs-18 el-button--primary el-button--large">Data Loader Management</router-link>
        </div>
      </div>
    </section>

    <div class="list mt-50">
      <p class="font-bold mb-10 fs-18">All ClickHouse Clusters</p>
      <el-table :data="list"
                border
                header-cell-class-name="header-cell-class-name">
        <el-table-column prop="cluster"
                         show-overflow-tooltip
                         label="Cluster Name" />
        <el-table-column prop="isReplica"
                         show-overflow-tooltip
                         label="Replica" />
        <el-table-column prop="hosts"
                         show-overflow-tooltip
                         label="ClickHouse Node IP" />
        <el-table-column prop="count"
                         show-overflow-tooltip
                         label="ClickHouse Node Count" />
        <el-table-column prop="zkNodes"
                         show-overflow-tooltip
                         label="Zookeeper Node List" />
        <el-table-column label="Actions"
                         #default="{ row }">
          <template>
            <el-link type="primary"
                     underline
                     @click.prevent="toCluster(row)">
              <router-link :to="'/clusters/' + row.cluster">Go to cluster</router-link>
            </el-link>

            <i class="fa fa-trash pointer fs-18 ml-15"
               v-tooltip="'Delete'"
               @click="remove(row)" />
          </template>
        </el-table-column>
      </el-table>
    </div>
  </section>
</template>
<script>
import CreateCk from "./modals/createCk";
import { $modal } from "@/services";
import { ClusterApi, PackageApi } from "@/apis";
export default {
  name: "Home",
  data() {
    return {
      list: [],
      versionOptions: [],
    };
  },
  mounted() {
    this.fetchVersionData();
    this.fetchData();
  },
  methods: {
    async fetchData() {
      this.list = [];
      const {
        data: { entity },
      } = await ClusterApi.getCluster();
      Object.entries(entity).forEach(([name, item]) => {
        item.count = item.hosts.length;
        item.hosts = item.hosts.join(",");
        item.zkNodes = item.zkNodes.join(",");
        this.list.unshift(item);
      });
    },
    async fetchVersionData() {
      const {
        data: { entity },
      } = await PackageApi.getList();
      this.versionOptions = entity.map((item) => ({
        value: item,
        label: item,
      }));
    },
    async importCk(type) {
      await $modal({
        component: CreateCk,
        props: {
          title: type
            ? "Create a new ClickHouse Cluster"
            : "Import a ClickHouse Cluster",
          width: 600,
          cancelText: "Cancel",
          okText: type ? "Create" : "Import",
        },
        data: {
          type,
          versionOptions: this.versionOptions,
        },
      });
      const tip = this.type ? "创建成功" : "导入成功";
      this.$message.success(`${tip}`);
      this.fetchData();
    },
    toCluster(item) {
      this.$root.clusterBench = item;
      delete this.$root.clusterBench.count;
    },
    async remove(item) {
      await this.$confirm("Confirm whether to delete ?", "Tip", {
        confirmButtonText: "Delete",
        cancelButtonText: "Cancel",
        text: "warning",
      });
      await ClusterApi.deleteCluster(`${item.cluster}`);
      this.$message.success(`${item.cluster} Cluster 已删除`);
      this.fetchData();
    },
  },
};
</script>

<style lang="scss" scoped>
.title {
  color: var(--color-black);
}
.btns {
  width: 300px;
  .el-button {
    margin: 0;
    height: 42px;
  }
}
.list {
  ::v-deep .header-cell-class-name {
    background: var(--primary-color);
  }
}
</style>
