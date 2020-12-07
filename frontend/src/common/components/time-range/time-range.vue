<template>
  <div class="time-range">
    <div class="content" ref="ContentBox" :style="styleObj.content">
      <span class="btn"
            :style="{left: (btnStyle.left - btnStyle.width / boxWidth) * 100 + '%'}"
            @mousedown="btnMousedown($event, 'left')"></span>
      <span class="btn"
            style="transform: rotate(180deg)"
            :style="{left: btnStyle.right * 100 + '%'}"
            @mousedown="btnMousedown($event, 'right')"></span>
      <div class="selected"
           @mousedown="drag"
           :style="{width: (selectedWidth + btnStyle.width / boxWidth) * 100 + '%',
                    left: btnStyle.left * 100 + '%'}"></div>
      <div class="others" :style="{width: btnStyle.left * 100 + '%', left: 0}"></div>
      <div class="others" :style="{width: (1 - btnStyle.right) * 100 + '%', right: 0}"></div>
    </div>
    <vue-echarts class="chart"
                 ref="Chart"
                 :style="styleObj.chart"
                 v-if="chartOption"
                 :option="chartOption"
                 :theme="chartTheme" />
    <div :style="styleObj.config" class="agg-info" v-if="showAggInfo">
      <span class="mr-30">聚合粒度：{{aggWindowText}}</span>
      最小颗粒度：
      <template v-if="disabledMinWindow">
        {{minWindow}}{{windowUnitLabel}}
      </template>
      <template v-else>
        <el-input-number class="width-100"
                         :value="minWindow"
                         @change="updateMinWindow"
                         :min="1"
                         :default-value="1"
                         step-strictly />
        <el-select class="width-100 ml-5"
                   :value="minWindowUnit"
                   @input="updateMinWindowUnit">
          <el-option v-for="item of timeUnit"
                     :key="item.value"
                     :value="item.value"
                     :label="item.label" />
        </el-select>
      </template>
    </div>
  </div>
</template>
<script src="./time-range.ts"></script>
<style lang="scss" src="./time-range.scss" scoped></style>
