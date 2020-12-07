<template>
  <el-popover ref="popover" placement="bottom-start">
    <a slot="reference" class="ss-tag-text">
      <i class="el-icon-arrow-down"></i>
      {{item.detail.title}}
      <span v-if="item.value">（{{item.value}}）</span>
    </a>
    <form @submit.prevent="$refs.popover.doClose()">
      <el-input :type="item.detail.inputType || 'text'"
                :size="size"
                v-model="item.tempValue"
                placeholder="请输入内容"
                @blur="$emit('call', 'selectInput', item)" />
    </form>
  </el-popover>
</template>
<script>
export default {
  props: {
    item: Object,
    size: String,
  },
};
</script>
