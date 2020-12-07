import { isEmpty } from 'lodash-es';
import 'reflect-metadata';
import Vue, { ComponentOptions, CreateElement, PropOptions, VNode, WatchOptions, WatchOptionsWithHandler } from 'vue';
import { LIFECYCLE_HOOKS } from 'vue/src/shared/constants.js';

import { hasOwn } from './helpers/hasOwn';
import { pushOrCreate } from './helpers/pushOrCreate';

const $internalHooks = new Set<string>(LIFECYCLE_HOOKS);

interface LifeCycleHook {
  /** Called synchronously immediately after the instance has been initialized, before data observation and event/watcher setup. */
  beforeCreate?(): void;
  /** Called synchronously after the instance is created. */
  created?(): void;
  /** Called right before the mounting begins: the render function is about to be called for the first time. */
  beforeMount?(): void;
  /** Called after the instance has been mounted, where el is replaced by the newly created vm.$el. */
  mounted?(): void;
  /** Called when data changes, before the DOM is patched. */
  beforeUpdate?(): void;
  /** Called after a data change causes the virtual DOM to be re-rendered and patched. */
  updated?(): void;
  /** Called when a kept-alive component is activated. */
  activated?(): void;
  /** Called when a kept-alive component is deactivated. */
  deactivated?(): void;
  /** Called right before a Vue instance is destroyed. */
  beforeDestroy?(): void;
  /** Called after a Vue instance has been destroyed. */
  destroyed?(): void;
  /** Called when an error from any descendent component is captured. */
  errorCaptured?(err: Error, vm: Vue, info: string): boolean | void;
  serverPrefetch?(): Promise<void>;
}

interface VueComponentBase extends Vue, LifeCycleHook {
  // eslint-disable-next-line @typescript-eslint/no-misused-new
  new(): VueComponentBase;

  render?(createElement: CreateElement): VNode;
  renderError?(createElement: CreateElement, err: Error): VNode;
}

let currentComponentProps: Record<string, any>;

/** A fake base class generally for static type checking of TypeScript */
export const VueComponentBase = class {
  constructor() { return Object.create(currentComponentProps); }
} as any as VueComponentBase;

const VueInreactive = Symbol('vue-inreactive');

export function Inreactive(prototype: any, prop: string) {
  pushOrCreate(prototype, VueInreactive, prop);
}

const VueField = Symbol('vue-decorate-field');

function decorateField(type: string, data?: any) {
  return function decorate(prototype: any, field: string) {
    if (!prototype[VueField]) prototype[VueField] = {};
    pushOrCreate(prototype[VueField], type, [field, data]);
    Inreactive(prototype, field);
  };
}

export type Decorator = (prototype: any, prop: string) => void;

export const Prop: (config?: PropOptions) => Decorator = decorateField.bind(null, 'props');
export const Ref: (refKey?: string) => Decorator = decorateField.bind(null, 'refs');

const VueWatches = Symbol('vue-decorate-watch');

export function Watch(prop?: string, option?: WatchOptions): Decorator {
  return function decorate(clazz: any, fn: string) {
    pushOrCreate(clazz[fn], VueWatches, { prop, option });
  };
}

const VueHooks = Symbol('vue-decorate-hook');

export function Hook(type: keyof LifeCycleHook) {
  return function decorate(clazz: any, fn: string) {
    pushOrCreate(clazz[fn], VueHooks, type);
  };
}

export function Component(...mixins: ComponentOptions<any>[]) {
  return (clazz: VueComponentBase) => {
    const { prototype } = clazz;
    if (!prototype[VueField]) prototype[VueField] = {};

    const lifeCycles: Record<string, ((this: Vue) => void)[]> = {
      beforeCreate: [function beforeCreate() {
        currentComponentProps = isEmpty(this.$options.propsData)
          ? Object.prototype
          : this.$options.propsData;
        const instance = new clazz();
        if (currentComponentProps !== Object.prototype) Object.setPrototypeOf(instance, Object.prototype);

        const inreactiveProps = [];

        // 找到子类与所有父类的VueInreactive
        for (let proto = clazz.prototype; proto && proto[VueInreactive]; proto = Object.getPrototypeOf(proto)) {
          if (hasOwn(proto, VueInreactive)) { // 避免找到原型链上的VueInreactive出现重复
            inreactiveProps.push(...proto[VueInreactive]);
          }
        }

        for (const prop of inreactiveProps) {
          if (prop in instance) {
            if (instance[prop] !== undefined) {
              const propDef: PropOptions<any> = this.$options.props[prop];
              if (propDef) {
                const val = instance[prop];
                propDef.default = () => val; // Silence Vue dev warnings
                if (propDef.type === Object) {
                  propDef.type = Object(val).constructor;
                }
              } else {
                this[prop] = instance[prop];
              }
            }
            delete instance[prop];
          }
        }

        this.$options.data = instance;

        Object.defineProperties(this, Object.fromEntries<PropertyDescriptor>(
          (prototype[VueField].refs || []).map(([field, refKey = field]) => [field, {
            get() { return this.$refs[refKey]; },
          }]),
        ));
      }],
      created: [function created() {
        Object.setPrototypeOf(this.$data, this);
      }],
    };

    const opts: ComponentOptions<any> = {
      mixins: [...mixins, lifeCycles],
      props: Object.fromEntries<PropOptions>(
        (prototype[VueField].props || []).map(([field, config = {} as any]) => [field,
          !config.type
            ? Object.assign({ type: Reflect.getMetadata('design:type', prototype, field) }, config)
            : config,
        ]),
      ),
      computed: {},
      methods: {},
      filters: {},
      watch: {},
    };

    const lifeCycleHook = new Set();
    // Forwards class methods to vue methods
    for (let proto = prototype; proto && proto !== Object.prototype; proto = Object.getPrototypeOf(proto)) {
      for (const [property, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(proto))) {
        if (property === 'constructor') continue;
        console.assert(typeof (descriptor.value || descriptor.get || descriptor.set) === 'function', `Don't set normal properties on prototype of the class`);

        if ($internalHooks.has(property)) {
          if (!lifeCycleHook.has(property)) {
            pushOrCreate(lifeCycles, property, descriptor.value);
            lifeCycleHook.add(property);
          }
        } else if (property === 'render' || property === 'renderError') {
          // render fn Cannot be an array
          opts[property] = opts[property] || descriptor.value;
        } else if (descriptor.get || descriptor.set) {
          opts.computed[property] = opts.computed[property] || {
            get: descriptor.get,
            set: descriptor.set,
          };
        } else {
          const watches = descriptor.value[VueWatches] as {
            prop: string;
            option: WatchOptions;
          }[];
          if (watches) {
            watches.forEach(({ prop, option }) => {
              pushOrCreate(opts.watch, prop, {
                ...option,
                handler: descriptor.value,
              } as WatchOptionsWithHandler<any>);
            });
          }
          const hooks = descriptor.value[VueHooks] as string[];
          hooks?.forEach(hook => pushOrCreate(lifeCycles, hook, descriptor.value));
          opts.methods[property] = opts.methods[property] || descriptor.value as (this: any, ...args: any[]) => any;
        }
      }
    }

    const resultFn = Vue.extend(opts) as any;
    // Copies static properties.
    // Note: Object.assign only copies enumerable properties, `name` and `prototype` are not enumerable.
    // WARNING: Vue has its own predefined static properties. Please make sure that they are not conflict.
    Object.assign(resultFn, clazz);
    return resultFn;
  };
}

Component.registerHooks = (hooks: string[]) => {
  hooks.forEach(x => $internalHooks.add(x));
};
