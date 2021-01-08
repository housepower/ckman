<template>
  <main class="settings">
    <breadcrumb :data="['Clusters', $route.params.id, 'session']"></breadcrumb>
    <section class="container">
      <div class="total flex flex-end pb-10 flex-vcenter">
        <!-- <div class="left flex-column flex-center ml-30">
          <span class="fs-28 font-bold">36</span>
          <span class="fs-18 font-bold">Total Open Sessions</span>
        </div> -->
        <div class="right">
          <time-filter :refreshDuration.sync="refresh"
                       @on-refresh="timeFilterRefresh" />
        </div>
      </div>
      <div class="tables">
        <h3 class="mb-10">Open Sessions</h3>
        <session-table :list="openList" />
        <h3 class="mb-10 mt-50">Slow Sessions</h3>
        <session-table :list="closeList" />
      </div>
    </section>
  </main>
</template>
<script>
import SessionTable from "./component/sessionTable";
import { SessionApi } from "@/apis";
export default {
  data() {
    return {
      refresh: null,
      openList: [],
      closeList: [],
    };
  },
  mounted() {
    this.fetchData();
  },
  methods: {
    async fetchData() {
      const id = this.$route.params.id;
      const {
        data: { data: openList },
      } = await SessionApi.open(id);
      const {
        data: { data: closeList },
      } = await SessionApi.close(id);
      this.openList = openList;
      this.closeList = closeList;
    },
    timeFilterRefresh() {
      this.fetchData();
    },
  },
  components: { SessionTable },
};
</script>

<style lang="scss" scoped>
.total {
  // border-bottom: 1px solid #eaeef4;
}
</style>
