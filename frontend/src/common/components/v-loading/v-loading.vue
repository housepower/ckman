<template>
  <div class="loading">
    <div class="content">
      <div class="rotate">
        <i></i>
        <i></i>
        <i></i>
        <i></i>
      </div>
      <div v-if="$root.loading"
           class="text fc-primary"
           v-text="$root.loading.text || '加载中...'"></div>
    </div>
  </div>
</template>
<script src="./v-loading.ts"></script>
<style lang="scss" src="./v-loading.scss" scoped></style>
