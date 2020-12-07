<template>
  <div>
    <nav :style="navStyle"><slot /></nav>
    <template v-if="!customRouterView">
      <keep-alive v-if="keepAlive">
        <router-view :key="viewKey" v-bind="props" ref="view" />
      </keep-alive>
      <router-view v-else :key="viewKey" v-bind="props" ref="view" />
    </template>
  </div>
</template>
<script lang="ts" src="./r-tabset.ts"></script>
<style lang="scss" src="./r-tabset.scss" scoped></style>
