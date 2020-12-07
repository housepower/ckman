import Vue, { VueConstructor } from 'vue';

export interface DrawerModel {
  /** The child component definition */
  component: VueConstructor | Vue;
  /** Properties to be passed into the child component */
  data?: { [key: string]: any };
  /**
   * @see https://element.eleme.io/#/zh-CN/component/drawer
   */
  props?: {
    /** Title of Drawer */
    title?: string;

    /** Size of Drawer */
    size?: string | number;

    /** Drawer's opening direction */
    direction?: 'rtl' | 'ltr' | 'ttb' | 'btt';

    /** Should show shadowing layer */
    modal?: boolean;

    /** Whether to append modal to body element. If false, the modal will be appended to Dialog's parent element */
    modalAppendToBody?: boolean;

    /** If set, closing procedure will be halted */
    beforeClose?: (done: () => void) => void;

    /** Extra class names for Drawer */
    customClass?: string;

    /** Whether the Drawer can be closed by clicking the mask */
    wrapperClosable?: boolean;

    /** Whether the Dialog can be closed by pressing ESC */
    closeOnPressEscape?: boolean;

    /** Whether to show a close button */
    showClose?: boolean;

    /** 控制是否默认给第一个可获取焦点的元素设置焦点 */
    focusFirst?: boolean;

    /**
     * Text of Cancel button, or null to remove it
     *
     * @default '取消'
     * @description if both cancelText and okText are null, the footer part is removed
     */
    cancelText?: string;

    /**
     * Text of OK button, or null to remove it
     *
     * @default '确定'
     * @description if both cancelText and okText are null, the footer part is removed
     */
    okText?: string;
  };
}
