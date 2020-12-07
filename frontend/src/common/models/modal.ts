import Vue, { ComponentOptions, VueConstructor } from 'vue';

export interface ModalModel {
  /** The child component definition */
  component: VueConstructor | Vue | ComponentOptions<any>;
  /** Properties to be passed into the child component */
  data?: { [key: string]: any };
  /**
   * @see https://element.eleme.io/#/zh-CN/component/dialog
   */
  props?: {
    /** Title of Dialog */
    title?: string;

    /** Width of Dialog */
    width?: string | number;

    /** Whether the Dialog takes up full screen */
    fullscreen?: boolean;

    /** Value for margin-top of Dialog CSS */
    top?: string;

    /** Whether a mask is displayed */
    modal?: boolean;

    /** Whether to append modal to body element. If false, the modal will be appended to Dialog's parent element */
    modalAppendToBody?: boolean;

    /** Whether scroll of body is disabled while Dialog is displayed */
    lockScroll?: boolean;

    /** Custom class names for Dialog */
    customClass?: string;

    /** Whether the Dialog can be closed by clicking the mask */
    closeOnClickModal?: boolean;

    /** Whether the Dialog can be closed by pressing ESC */
    closeOnPressEscape?: boolean;

    /** Whether to show a close button */
    showClose?: boolean;

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
