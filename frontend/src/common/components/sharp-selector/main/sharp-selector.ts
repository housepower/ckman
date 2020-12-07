import { cloneDeep } from 'lodash-es';
import Vue from 'vue';

import type {
  SharpSelectorDataModel,
  SharpSelectorNewOptionsModel,
  SharpSelectorOptionDetailModel,
} from '../../../models';
import { Component, Inreactive, Prop, VueComponentBase, Watch } from '../../../VueComponentBase';

import * as components from '../components';

@Component()
export default class SharpSelector extends VueComponentBase {
  @Prop() optionDetail: SharpSelectorOptionDetailModel;
  @Prop() newOptions: SharpSelectorNewOptionsModel[];
  @Prop() data: SharpSelectorDataModel[];
  @Prop() defaultData: { key: string; value: any }[] = [];
  @Prop() hideClearable: boolean;
  @Prop() size = 'small';
  @Prop() filterIcon = 'el-icon-search';

  today = new Date();
  operators = [
    { key: '<', label: '小于' },
    { key: '<=', label: '小于等于' },
    { key: '<=>', label: '区间' },
    { key: '>', label: '大于' },
    { key: '>=', label: '大于等于' },
    { key: '==', label: '等于' },
    { key: '!=', label: '不等于' },
  ];

  @Inreactive readonly components = components;

  @Watch('optionDetail', { deep: true })
  optionDetailChange(val: SharpSelectorOptionDetailModel) {
    this.data.forEach(x => {
      if (x.options) {
        const selected = new Set(x.options.filter(y => y.selected).map(y => y.value));
        x.options = (val[x.key] || []).map(y => ({ ...y, selected: selected.has(y.value) }));

        switch (x.detail.filterType) {
          case 'single': {
            const target = x.options.find(y => y.selected);
            x.value = target?.value ?? null;
            x.displayText = target?.label || '';
            break;
          }
          case 'filter':
          case 'multi': {
            x.displayTextArr = x.options.filter(y => y.selected);
            x.value = x.displayTextArr.map(y => y.value);
            break;
          }
        }
      }
    });
  }

  mounted() {
    this.defaultData.forEach(x => this.addSelectedOption(x));
  }

  /** 添加已经有值的选项，用于代码中 */
  addSelectedOption(selectedOption: { key: string; value: any }) {
    const target = this.newOptions.find(o => o.key === selectedOption.key);
    if (!target) return;
    this.addNewOption(target, false);
    const tag = this.data.find(x => x.key === selectedOption.key);
    switch (tag.detail.filterType) { // TODO: 待测试 filter
      case 'single': {
        const option = tag.options.find(o => selectedOption.value === o.value);
        if (option) this.selectSingle(tag, option, false);
        break;
      }
      case 'filter':
      case 'multi': {
        selectedOption.value.forEach(value => {
          const option = tag.options.find(o => value === o.value);
          if (option) this.selectMulti(tag, option, false);
        });
        break;
      }
      case 'textarea':
      case 'input': {
        tag.tempValue = selectedOption.value;
        this.selectInput(tag, false);
        break;
      }
      case 'duration': {
        tag.tempValue = selectedOption.value;
        this.selectDuration(tag, false);
        break;
      }
      case 'range': {
        tag.tempValue = selectedOption.value;
        this.selectRange(tag, false);
        break;
      }
    }
  }

  /** 添加待选选项，用于用户点击触发 */
  async addNewOption(newOption: SharpSelectorNewOptionsModel, openPopover = true) {
    if (newOption.filterDisabled || this.data.some(x => x.key === newOption.key)) return;

    const datum = {
      detail: newOption,   // column 对象
      key: newOption.key,  // 对应搜索字段
      value: null,         // 显示文字
      tempValue: {},       // 显示文字
      visible: false,       // 下拉框是否可见
      options: null,
      searchText: '',
      displayText: '',
    };

    switch (newOption.filterType) {
      case 'single': {
        const options = this.optionDetail[newOption.key];
        datum.options = cloneDeep(options);
        datum.value = null;
        break;
      }
      case 'filter': {
        datum.searchText = '';
      }
      // eslint-disable-next-line no-fallthrough
      case 'multi': {
        const options = this.optionDetail[newOption.key];
        datum.options = cloneDeep(options);
        datum.value = [];
        break;
      }
      case 'textarea':
      case 'input': {
        datum.tempValue = '';
        break;
      }
      case 'duration': {
        datum.tempValue = [];
        break;
      }
      case 'range': {
        datum.tempValue = {
          verb: {},
          value1: null,
          value2: null,
        };
        break;
      }
    }
    this.$set(newOption, 'filterDisabled', true);
    this.data.push(datum as SharpSelectorDataModel);

    if (openPopover) {
      await this.$nextTick();
      const tags = this.$el.querySelectorAll('.ss-tag-text') as unknown as HTMLElement[];
      const target = tags[tags.length - 1];
      if (target.click) target.click();
      datum.visible = true;
    }
  }

  selectSingle(item: SharpSelectorDataModel, option, shouldEmit = true) {
    item.value = option.value;
    item.displayText = option.label;
    item.visible = false;
    option.selected = true;
    if (shouldEmit) this.$emit('change', { type: 'change', key: item.key });
  }

  selectMulti(item: SharpSelectorDataModel, option, shouldEmit = true) {
    this.$set(option, 'selected', !option.selected);
    const allSelected = item.options.filter(x => x.selected);
    item.value = allSelected.map(x => x.value);
    item.displayTextArr = allSelected;
    if (shouldEmit) this.$emit('change', { type: 'change', key: item.key });
  }

  selectDuration(item: SharpSelectorDataModel, shouldEmit = true) {
    item.visible = false;
    const formatDate = Vue.filter('formatDate');
    if (item.tempValue) {
      item.displayText = item.tempValue.map(x => formatDate(x).replace(' 00:00:00', '')).join(' 至 ');
      item.value = item.tempValue.map(x => +x);
    } else {
      item.displayText = '';
      item.value = [];
    }
    if (shouldEmit) this.$emit('change', { type: 'change', key: item.key });
  }

  selectInput(item: SharpSelectorDataModel, shouldEmit = true) {
    item.value = item.tempValue;
    item.visible = false;
    if (shouldEmit) this.$emit('change', { type: 'change', key: item.key });
  }

  selectRange(item: SharpSelectorDataModel, shouldEmit = true) {
    const itemValue = item.value = item.tempValue;
    if (itemValue.verb === '<=>') {
      if (itemValue.value1 > itemValue.value2) {
        [itemValue.value1, itemValue.value2] = [itemValue.value2, itemValue.value1];
      }
      item.displayText = `${itemValue.value1} 至 ${itemValue.value2}`;
    } else {
      item.displayText = `${this.operators.find(x => x.key === itemValue.verb.key).label} ${itemValue.value1}`;
    }
    item.visible = false;
    if (shouldEmit) this.$emit('change', { type: 'change', key: item.key });
  }

  remove(item: SharpSelectorDataModel, shouldEmit = true) {
    this.data.splice(this.data.indexOf(item), 1);
    item.detail.filterDisabled = false;
    if (shouldEmit) this.$emit('change', { type: 'remove', key: item.key });
  }

  removeAll(shouldEmit = true) {
    if (!this.data.length) return;
    this.data.filter(x => !x.detail.hideClose).forEach(x => this.remove(x, false));
    if (shouldEmit) this.$emit('change', { type: 'clear' });
  }

  showPopover(item: SharpSelectorDataModel) {
    this.hidePopover();
    item.visible = true;
  }

  hidePopover() {
    this.data.forEach(x => x.visible = false);
  }

  callMethod(method: string, ...args: any[]) {
    return this[method](...args);
  }
}
