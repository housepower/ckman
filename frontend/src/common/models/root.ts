import Vue from 'vue';

export interface RootBase extends Vue {
  modals?: import ('../services/modal').ModalPropModel[];
  tooltips?: import ('../components/sharp-tooltip').TooltipPropModel[];
  drawers?: import('../services/modal').ModalPropModel[];
  loading: {
    status: number;
    text: string;
  };
}
