<template>
  <main class="settings">
    <breadcrumb :data="['Clusters', $route.params.id, 'tables']"></breadcrumb>
    <section>
      <ZkTable />
    </section>
  </main>
</template>
<script>
import ZkTable from "./components/zkTable";
export default {
  data() {
    return {};
  },
  mounted() {},
  methods: {},
  components: {
    ZkTable,
  },
};
</script>

<style lang="scss" scoped></style>
