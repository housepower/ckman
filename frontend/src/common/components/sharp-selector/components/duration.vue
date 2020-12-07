<template>
  <label class="ss-tag-text"><i class="el-icon-arrow-down"></i>
    <el-date-picker v-model="item.tempValue"
                    :editable="false"
                    type="datetimerange"
                    placement="bottom-start"
                    :size="size"
                    range-separator="至"
                    :picker-options="item.detail.pickerOptions"
                    start-placeholder="开始日期"
                    end-placeholder="结束日期"
                    @change="$emit('call', 'selectDuration', item)" />
    {{item.detail.title}}<span v-if="item.displayText">（{{item.displayText}}）</span>
  </label>
</template>
<script>
export default {
  props: {
    item: Object,
    size: String,
  },
};
</script>
