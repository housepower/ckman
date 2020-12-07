import type { Input } from 'element-ui';

import { $message } from '../../services';
import { Component, Prop, Ref, VueComponentBase } from '../../VueComponentBase';

@Component()
export default class TagManager extends VueComponentBase {
  @Prop() tags: any[];
  @Prop() labelKey: string;
  @Prop() selectTags: string[] = [];
  @Prop() editable = true;
  @Prop() maxSize = Infinity;
  @Prop() maxTextLength = 128;

  @Ref() saveTagInput: Input & { suggestionVisible: boolean; activated: boolean };

  inputValue: string = null;

  get tagList(): string[] {
    return this.labelKey ? this.tags.map(x => x[this.labelKey]) : this.tags;
  }

  selectTagsFilter(query: string, cb: (tags: { value: string }[]) => void) {
    let data = this.selectTags.filter(x => !this.tagList.includes(x)).map(x => ({ value: x }));
    if (query) {
      const text = query.toLowerCase();
      data = data.filter(x => x.value.toLowerCase().includes(text));
    }
    cb(data);
  }

  handleClose(index: number) {
    this.tags.splice(index, 1);
    this.$emit('removeTag');
  }

  showInput() {
    this.inputValue = '';
    this.$nextTick(() => {
      this.saveTagInput.focus();
      const suggestions = this.saveTagInput.$refs.suggestions as any;
      const { doDestroy } = suggestions;
      suggestions.doDestroy = (...args) => {
        if (!this.saveTagInput || !this.saveTagInput.activated) this.handleInputConfirm();
        doDestroy(...args);
      };
    });
  }

  handleInputConfirm() {
    if (this.inputValue == null) return;
    let value: any = this.inputValue.trim();
    if (!value) return this.inputValue = null;
    if (this.tagList.includes(value)) {
      this.inputValue = null;
      return $message.warning('该标签已存在');
    }
    if (this.labelKey) value = { [this.labelKey]: value };
    this.tags.push(value);
    if (this.inputValue) this.$emit('addTag', value);
    this.inputValue = null;
  }

  blur() {
    if (!this.saveTagInput.suggestionVisible) this.handleInputConfirm();
  }
}
