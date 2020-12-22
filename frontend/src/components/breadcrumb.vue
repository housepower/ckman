<template>
  <section class="breadcrum mb-10 flex-vcenter flex-between plr-10">
    <el-breadcrumb separator-class="el-icon-arrow-right">
      <el-breadcrumb-item>
        <router-link :to="{ path: '/' }"><i class="fa fa-home"></i></router-link>
      </el-breadcrumb-item>
      <el-breadcrumb-item v-for="(item, index) of data"
                          :key="index"
                          :class="{ active: index === data.length - 1 }">{{ item }}</el-breadcrumb-item>
    </el-breadcrumb>
    <div class="slot">
      <slot></slot>
    </div>
  </section>
</template>
<script>
export default {
  name: "Breadcrumb",
  props: {
    data: {
      type: Array,
      default: [],
    },
  },
  data() {
    return {};
  },
  mounted() {},
};
</script>

<style lang="scss" scoped>
.breadcrum {
  height: 45px;
  background: #fcfcfc;
  border: 1px solid #e8eaec;

  ::v-deep .el-breadcrumb__inner {
    font-size: 18px;
  }
  .active {
    ::v-deep .el-breadcrumb__inner {
      color: var(--primary-color);
    }
  }
}
</style>
