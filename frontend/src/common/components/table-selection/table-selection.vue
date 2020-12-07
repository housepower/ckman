<template>
  <el-table-column :width="width" :fixed="fixed">
    <template #header>
      <el-checkbox :indeterminate="selectAllStatus === null"
                   :value="!!selectAllStatus"
                   @input="selectAllChange" />
    </template>
    <template #default="{ row, $index }">
      <el-checkbox :value="selectedIds[getRowId(row)]"
                   @input="selectChange($event, row, $index)" />
    </template>
  </el-table-column>
</template>
<script src="./table-selection.ts" />
