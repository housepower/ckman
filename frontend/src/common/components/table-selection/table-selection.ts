import type { Table } from 'element-ui';

import { Component, Prop, VueComponentBase } from '../../VueComponentBase';

// 用法：
// <table-selection :selectedIds="selectedIds"
//                  fixed="left"
//                  @select-all="onSelectionChange"
//                  @select="onSelectionChange" />

/**
 * 此组件用于表格分页多选功能，需配合el-table中的row-key属性使用
 *
 * @event select-all 点击全选会触发 返回全选框状态 value
 * @event select 点击单选触发 返回 {value, row, $index} 分别表示，当前选框状态，当前行信息，当前行下标
 */
@Component()
export default class TableSelection extends VueComponentBase {
  /** 记录表格选择项 */
  @Prop({ required: true }) selectedIds: Record<string, boolean>;
  /** 固定位置  可选填 ‘left’，‘right’ */
  @Prop() readonly fixed: string;
  /** 选择列宽度 */
  @Prop({ default: 35 }) readonly width: number;

  $parent: Table;

  get selectAllStatus() {
    const { data } = this.$parent;
    if (!data.length) return false;

    const status = this.selectedIds[this.getRowId(data[0])] || false;
    for (const item of data) {
      if ((this.selectedIds[this.getRowId(item)] || false) !== status) return null;
    }
    return status;
  }

  getRowId(row: Record<string, any>) {
    const rowKey = this.$parent.rowKey;
    if (typeof rowKey === 'function') {
      return rowKey(row);
    } else {
      return row[rowKey];
    }
  }

  selectAllChange(value: boolean) {
    this.$parent.data.forEach((x: Record<string, any>) => {
      this.$set(this.selectedIds, this.getRowId(x), value);
    });
    // 避免因计算等操作，使部分数据在自定义事件触发时未完成更新的问题
    this.$nextTick(() => {
      this.$emit('select-all', value);
    });
  }

  selectChange(value: boolean, row: Record<string, any>, $index: number) {
    this.$set(this.selectedIds, this.getRowId(row), value);
    this.$nextTick(() => {
      this.$emit('select', { value, row, $index });
    });
  }
}
