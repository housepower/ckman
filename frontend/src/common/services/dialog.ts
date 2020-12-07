import { MessageBox } from 'element-ui';
import type { ElMessageBoxOptions, MessageBoxInputData } from 'element-ui/types/message-box';
import type { VNode } from 'vue';

export const $dialog = {
  confirm: (message: string | VNode, title = '提示', options?: ElMessageBoxOptions) => MessageBox.confirm(message as any, title, options),
  alert: (message: string | VNode, title = '提示', options?: ElMessageBoxOptions) => MessageBox.alert(message as any, title, options),
  prompt: (message: string | VNode, title = '提示', options?: ElMessageBoxOptions) => MessageBox.prompt(message as any, title, options) as Promise<MessageBoxInputData>,
  msgbox: (options: ElMessageBoxOptions) => MessageBox(options),
  close: () => MessageBox.close(),
};
