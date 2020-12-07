<template>
  <el-dropdown trigger="click" placement="bottom-start">
    <a class="ss-tag-text"><i class="el-icon-arrow-down"></i>
      {{item.detail.title}}<span v-if="item.displayText">（{{item.displayText}}）</span>
    </a>
    <el-dropdown-menu slot="dropdown" class="checkable maxheight-list">
      <el-dropdown-item v-for="option of item.options"
                        :key="option.value"
                        class="dropdown-item"
                        :class="{selected: item.value === option.value}"
                        @click.native="$emit('call', 'selectSingle', item, option)">{{option.label}}
      </el-dropdown-item>
    </el-dropdown-menu>
  </el-dropdown>
</template>
<script>
export default {
  props: {
    item: Object,
    size: String,
  },
};
</script>
