<template>
  <overview-base :metrics="metrics"
                 :breadcrumbInfo="breadcrumbInfo" />
</template>
<script>
import { LoaderMetrics } from "@/constants";

export default {
  data() {
    return {
      metrics: LoaderMetrics,
      breadcrumbInfo: [],
    };
  },
  mounted() {
    this.breadcrumbInfo = ["loader", "overview"];
  },
};
</script>
