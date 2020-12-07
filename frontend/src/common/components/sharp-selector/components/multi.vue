<template>
  <el-dropdown trigger="click" :hide-on-click="false" placement="bottom-start">
    <a class="ss-tag-text">
      <i class="el-icon-arrow-down"></i>
      {{item.detail.title}}<span v-if="item.displayTextArr && item.displayTextArr.length">（
        <span v-for="(x, i) of item.displayTextArr"
              :key="x.value"
              @click.stop="$emit('call', 'selectMulti', item, x)">
          {{x.label}}<template v-if="i !== item.displayTextArr.length - 1">、</template>
        </span>
      ）</span>
    </a>
    <el-dropdown-menu slot="dropdown" class="checkable maxheight-list">
      <el-dropdown-item v-for="option of item.options"
                        :key="option.value"
                        class="dropdown-item"
                        :class="{selected: option.selected}"
                        @click.native="$emit('call', 'selectMulti', item, option)">
        {{option.label}}
      </el-dropdown-item>
      <div v-if="!item.options.length"
           class="text-center fs-12 fc-gray"
           style="height: 30px; line-height: 30px">无数据</div>
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
