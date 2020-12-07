<template>
  <overview-base :metrics="metrics"
                 :breadcrumbInfo="breadcrumbInfo" />
</template>
<script>
import { Metrics } from "@/constants";

export default {
  data() {
    return {
      metrics: Metrics,
      breadcrumbInfo: [],
    };
  },
  mounted() {
    this.breadcrumbInfo = ["Clusters", this.$route.params.id, "overview"];
  },
};
</script>
