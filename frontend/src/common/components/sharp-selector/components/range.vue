<template>
  <el-popover ref="popover" placement="bottom-start">
    <a slot="reference" class="ss-tag-text">
      <i class="el-icon-arrow-down"></i>
      {{item.detail.title}}<span v-if="item.displayText">（{{item.displayText}}）</span>
    </a>
    <form @submit.prevent="$emit('call', 'selectRange', item)">
      <div class="flex width-350">
        <el-select :size="size" value-key="key" v-model="item.tempValue.verb" class="width-100" placeholder="请选择">
          <el-option v-for="item in operators"
                     :key="item.key"
                     :label="item.label"
                     :value="item" />
        </el-select>&nbsp;
        <el-input-number class="flex-1"
                         :size="size"
                         controls-position="right"
                         v-model="item.tempValue.value1"
                         :disabled="!item.tempValue.verb.key" />
        <template v-if="item.tempValue.verb.key === '<=>'">
          <span class="mlr-10 mt-5">～</span>
          <el-input-number class="flex-1"
                           :size="size"
                           controls-position="right"
                           v-model="item.tempValue.value2"
                           :min="item.tempValue.value1" />
        </template>
      </div>
      <footer class="text-right mt-10">
        <el-button size="mini" @click="item.visible = false; $refs.popover.doClose()">取消</el-button>&nbsp;
        <el-button size="mini"
                   type="primary"
                   html-type="submit"
                   :disabled="shouldDisableRange()"
                   @click="$emit('call', 'selectRange', item); $refs.popover.doClose()">确定</el-button>
      </footer>
    </form>
  </el-popover>
</template>
<script>
  export default {
    props: {
      item: Object,
      size: String,
      operators: Array,
    },
    methods: {
      shouldDisableRange() {
        return this.item.tempValue.value1 == null && // 普通情况
            (this.item.tempValue.verb !== '<=>' || this.item.tempValue.value2 > this.item.tempValue.value1); // 区间
      },
    },
  };
</script>
