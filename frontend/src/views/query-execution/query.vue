<template>
  <main class="query">
    <breadcrumb :data="['Clusters', $route.params.id, 'query-execution']" />
    <left-aside :list="list"
                @selectSql="selectSql" />
    <section class="container custom-scrollbar">
      <tags :list="list"
            :selectSql="selectedSql"
            @updateData="updateData" />
    </section>
  </main>
</template>
<script>
import { isNull, uniqBy } from "lodash-es";
import LeftAside from "./components/leftAside";
import Tags from "./components/rightSql";

export default {
  data() {
    return {
      list: [],
      selectedSql: {},
    };
  },
  created() {
    this.fetchData();
  },
  methods: {
    fetchData() {
      const splData = localStorage.getItem("sqlHisToryData");
      this.list =
        splData !== "undefined" && !isNull(splData) ? JSON.parse(splData) : [];
    },
    selectSql(item) {
      this.selectedSql = item;
    },
    updateData(addSql) {
      this.list.unshift(addSql);
      console.log(this.list);
      this.list = uniqBy(this.list, "value");
      localStorage.setItem(
        "sqlHisToryData",
        JSON.stringify(this.list.slice(0, 20))
      );
      this.fetchData();
    },
  },
  components: { LeftAside, Tags },
};
</script>

<style lang="scss" scoped>
.container {
  position: absolute;
  left: 262px;
  top: 115px;
  bottom: 65px;
  right: 20px;
  z-index: 50;
}
</style>
