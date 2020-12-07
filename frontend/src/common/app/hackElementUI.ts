import { Dropdown, InputNumber, Link, MessageBox, Slider } from 'element-ui';

(Link as any).props.underline.default = false;
(InputNumber as any).props.controlsPosition.default = 'right';
(Slider as any).props.inputControlsPosition.default = 'right';
(Dropdown as any).props.trigger.default = 'click';
MessageBox.setDefaults({ closeOnClickModal: false });
