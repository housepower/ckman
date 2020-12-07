<template>
  <div class="time-filter">
    <el-popover ref="timefilterTemplate" placement="bottom" popper-class="timefilter-popover" title="时间选择器" :width="hideRefresh ? 560 : 780" trigger="click" v-model="isDropdownOpen">
      <el-tabs class="timefilter-tabset" v-model="activeTab" type="card" tab-position="left" @tab-click="switchTab">
        <el-tab-pane label="快速" name="fast">
          <div class="pane-fast">
            <section class="pane-fast-left">
              <header>相对</header>
              <ul>
                <li :class="{ active: display.value === t }" v-for="t in relativeTimes" :key="t">
                  <a class="pointer" @click="setRelative(t)">{{ t | timefilterDuration }}</a>
                </li>
              </ul>
            </section>
            <section class="pane-fast-right" v-if="!hideRefresh">
              <header>实时</header>
              <ul style="margin-bottom: 0">
                <li :class="{ active: refreshDuration == null }">
                  <a class="pointer" @click="setRefresh(null)">关闭实时</a>
                </li>
              </ul>
              <ul>
                <li :class="{ active: refreshDuration === t }" v-for="t in realTimes" :key="t">
                  <a class="pointer" @click="setRefresh(t)">{{ t | duration }}</a>
                </li>
              </ul>
            </section>
          </div>
        </el-tab-pane>
        <el-tab-pane label="相对" name="relative" v-if="value">
          <div class="flex" v-if="activeTab === 'relative'">
            <section class="width-200">
              <header>从：{{ (now - display.moment) | formatDate }}</header>
              <el-input type="number" min="0" v-model="relativeValue" :validate-event="false" @input="setRelative((relativeValue || defaultValue) + display.unit)">
                <el-select v-model="display.unit" style="width: 88px" slot="append" placeholder="请选择" @change="setRelative(relativeValue + display.unit)">
                  <el-option v-for="(value, key) in TimeUnits" :key="key" :label="value + '前'" :value="key" />
                </el-select>
              </el-input>
            </section>
            <section class="width-200">
              <header>到：现在</header>
              <el-date-picker v-model="now" class="width-200" type="datetime" placeholder="选择日期时间" disabled />
            </section>
          </div>
        </el-tab-pane>
        <el-tab-pane label="绝对" name="absolute" v-if="value">
          <div class="pane-absolute">
            <el-date-picker v-model="absoluteValue" @change="setAbsolute(absoluteValue)" :clearable="false" type="datetimerange" range-separator="至" start-placeholder="开始日期" end-placeholder="结束日期" />
          </div>
        </el-tab-pane>
      </el-tabs>
    </el-popover>
    <div class="timefilter-button-group">
      <a class="timefilter-button" v-show="!hideRefresh && (isDropdownOpen || refreshDuration)" @click="setRefresh(null)">
        <i class="fa" :class="refreshDuration ? 'fa-play' : 'fa-pause'"></i>
        <span class="ml-5" v-if="refreshDuration">{{ refreshDuration | duration }}</span>
      </a>
      <a class="timefilter-button" v-popover:timefilterTemplate>
        <i class="fa fa-clock-o"></i>&nbsp;
        <span class="time-text">
          <template v-if="['fast', 'relative'].includes(display.type)">{{ display.value | timefilterDuration }}</template>
          <template v-else>{{ display.start | formatDate }} 至 {{ display.end | formatDate }}</template>
        </span>
      </a>
      <slot />
    </div>
  </div>
</template>
<script lang="ts" src="./time-filter.ts"></script>
<style lang="scss" src="./time-filter.scss"></style>
