<template>
  <main class="settings">
    <breadcrumb :data="['Clusters', $route.params.id, 'tables']"></breadcrumb>
    <section>
      <table-metric />
      <replication-table />
      <zk-table />
    </section>
  </main>
</template>
<script>
import ZkTable from "./components/zkTable";
import ReplicationTable from "./components/replicationStatus";
import TableMetric from "./components/tableMetrics";
export default {
  data() {
    return {};
  },
  mounted() {},
  methods: {},
  components: {
    ZkTable,
    ReplicationTable,
    TableMetric,
  },
};
</script>

<style lang="scss" scoped></style>
