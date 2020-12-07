import ace from 'ace-builds';
import type { Ace } from 'ace-builds';
import ResizeObserver from 'resize-observer-polyfill';
import type { CreateElement } from 'vue';

// 默认只引用 json 语法和 textmate 主题插件，其他语法和主题插件需要手工引入
import 'ace-builds/src-noconflict/mode-json';
import 'ace-builds/src-noconflict/theme-textmate';

import { Component, Inreactive, Prop, VueComponentBase, Watch } from '../VueComponentBase';

const Events = [
  'blur',
  'change',
  'changeSelectionStyle',
  'changeSession',
  'copy',
  'focus',
  'paste',
];

@Component()
export class VueAceEditor extends VueComponentBase {
  @Prop({ required: true }) readonly value: string;
  @Prop({ default: 'json' }) readonly lang: string;
  @Prop({ default: 'textmate' }) readonly theme: string;
  @Prop() readonly options: Partial<Ace.EditorOptions>;
  @Prop() readonly placeholder: string;
  @Prop() readonly readonly: boolean;

  @Inreactive editor: Ace.Editor;
  @Inreactive contentBackup: string;
  @Inreactive ro: ResizeObserver;

  $el: HTMLDivElement;

  render(h: CreateElement) {
    return h('div');
  }

  mounted() {
    const editor = this.editor = ace.edit(this.$el, {
      placeholder: this.placeholder,
      readOnly: this.readonly,
      value: this.value,
      mode: 'ace/mode/' + this.lang,
      theme: 'ace/theme/' + this.theme,
      ...this.options,
    });
    this.contentBackup = this.value;
    editor.on('change', () => {
      const content = editor.getValue();
      this.contentBackup = content;
      this.$emit('input', content);
    });
    Events.forEach(x => {
      if (typeof this.$listeners[x] === 'function') {
        editor.on(x as any, this.$emit.bind(this, x));
      }
    });
    this.ro = new ResizeObserver(() => editor.resize());
    this.ro.observe(this.$el);
    this.$emit('init', editor);
  }

  beforeUnmount() {
    this.ro?.disconnect();
    this.editor?.destroy();
  }

  focus() {
    this.editor.focus();
  }

  blur() {
    this.editor.blur();
  }

  selectAll() {
    this.editor.selectAll();
  }

  @Watch('value')
  updateValue(val: string) {
    if (this.contentBackup !== val) {
      this.editor.setValue(val, 1);
      this.contentBackup = val;
    }
  }

  @Watch('theme')
  updateTheme(val: string) {
    this.editor.setTheme('ace/theme/' + val);
  }

  @Watch('lang')
  updateLang(val: string) {
    this.editor.getSession().setMode('ace/mode/' + val);
  }

  @Watch('options')
  updateOptions(val: Partial<Ace.EditorOptions>) {
    this.editor.setOptions(val);
  }

  @Watch('readonly')
  updateReadonly(val: boolean) {
    this.editor.setReadOnly(val);
  }

  @Watch('placeholder')
  updatePlaceholder(val: string) {
    this.editor.setOption('placeholder', val);
  }
}
