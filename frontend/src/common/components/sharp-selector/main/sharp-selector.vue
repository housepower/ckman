<template>
  <div class="sharp-selector flex flex-vcenter">
    <div tabindex="0"
         class="el-input__inner select-selection flex-1">
      <i :class="filterIcon"></i>
      <div v-for="(item, index) of data" :key="index" class="ss-tag text-ellipsis">
        <component :is="components[item.detail.filterType]"
                   :item="item"
                   :size="size"
                   :operators="operators"
                   @call="callMethod" />
        <i class="el-icon-close pointer" v-if="!item.detail.hideClose" @click="remove(item)"></i>
      </div>
      <el-dropdown trigger="click"
                   class="ss-tag"
                   placement="bottom-start"
                   v-if="data.length < newOptions.length">
        <i class="el-icon-plus pointer"></i>
        <el-dropdown-menu slot="dropdown" class="maxheight-list">
          <el-dropdown-item v-for="item of newOptions"
                            :key="item.key"
                            :disabled="item.filterDisabled"
                            @click.native.stop="addNewOption(item)">{{item.title}}</el-dropdown-item>
        </el-dropdown-menu>
      </el-dropdown>
    </div>
    <a class="ml-10 fc-primary pointer" v-if="!hideClearable" @click="removeAll()">清空</a>
  </div>
</template>
<script lang="ts" src="./sharp-selector.ts" />
<style lang="scss" src="./sharp-selector.scss" />
