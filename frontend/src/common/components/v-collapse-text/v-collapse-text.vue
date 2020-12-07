<template>
  <div class="pointer header flex" @click="collapseChange()">
    <div class="flex-1"
         :class="computedClass"
         :style="'--line-clamp-value:' + lineClamp"
         ref="contentBox"><slot>{{ content }}</slot></div>
    <i :class="`fa fa-angle-${isWarp ? 'down' : 'up'}`"
       class="angle-icon"
       v-if="isExceed"></i>
  </div>
</template>
<script src="./v-collapse-text.ts" />
<style lang="scss" scoped>
.header {
  align-items: flex-start;
}

.angle-icon {
  width: 16px;
  text-align: right;
  margin-top: 6px;
}
</style>
