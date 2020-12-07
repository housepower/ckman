<template>
  <div class="v-collapse">
    <div class="title mb-15" :class="options.class" :style="options.style">
      <a class="pointer" @click="show=!show; $emit('input', show)">
        <slot name="title">{{title || options.title}}</slot>
        <i :class="'el-icon-arrow-' + (show?'up':'down')"></i>
      </a>
    </div>
    <el-collapse-transition>
      <div v-show="show">
        <slot />
      </div>
    </el-collapse-transition>
  </div>
</template>
<script src="./v-collapse.ts" />
