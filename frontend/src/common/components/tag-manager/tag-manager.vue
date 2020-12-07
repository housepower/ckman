<template>
  <div class="tag-manager flex" style="flex-wrap: wrap">
    <el-tag v-for="(tag, index) in tagList"
            :key="index"
            :closable="editable"
            disable-transitions
            :title="tag"
            @close="handleClose(index)">{{tag}}</el-tag>
    <template v-if="editable && tagList.length < maxSize">
      <el-autocomplete class="input-new-tag width-150"
                       v-if="inputValue != null"
                       size="mini"
                       placeholder="输入新标签或选择已有"
                       ref="saveTagInput"
                       :maxlength="maxTextLength"
                       v-model="inputValue"
                       @keypress.enter.native="handleInputConfirm"
                       @select="handleInputConfirm"
                       @blur="blur"
                       :fetch-suggestions="selectTagsFilter" />
      <a v-else
         class="pointer fc-primary"
         style="margin-top: 2px"
         @click="showInput"><i class="el-icon-plus"></i>新标签</a>
    </template>
  </div>
</template>
<style lang="scss" src="./tag-manager.scss"></style>
<script src="./tag-manager.ts"></script>
