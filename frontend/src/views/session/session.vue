<template>
  <main class="settings">
    <breadcrumb :data="['Clusters', $route.params.id, 'session']"></breadcrumb>
    <section class="container">
      <div class="total flex flex-between pb-10 flex-vcenter">
        <div class="left flex-column flex-center ml-30">
          <span class="fs-28 font-bold">36</span>
          <span class="fs-18 font-bold">Total Open Sessions</span>
        </div>
        <div class="right">
          <time-filter :refreshDuration.sync="refresh"
                       @on-refresh="timeFilterRefresh" />
        </div>
      </div>
      <div class="tables mt-50">
        <h3 class="mb-10">Open Sessions</h3>
        <session-table :list="list" />
        <h3 class="mb-10 mt-50">Slow Sessions</h3>
        <session-table :list="list" />
      </div>
      </div>
    </section>
  </main>
</template>
<script>
import SessionTable from "./component/sessionTable";
export default {
  data() {
    return {
      refresh: "30s",
      list: [],
    };
  },
  mounted() {},
  methods: {
    timeFilterRefresh() {},
  },
  components: { SessionTable },
};
</script>

<style lang="scss" scoped>
.total {
  border-bottom: 1px solid #eaeef4;
}
</style>
