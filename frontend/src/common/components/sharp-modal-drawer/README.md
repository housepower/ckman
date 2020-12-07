# 引入方式

* main.tsx 修改

```typescript jsx
const data = {
  modals: [],
  drawers: [],
};
// render 中添加

{this.modals.map(attrs => <sharp-modal {...{ attrs }} />)}
{this.drawers.map(attrs => <sharp-drawer {...{ attrs }} />)}
```

# 使用方式

```ts
$modal({
  props: {
    title: 'xxx',
    width: '500px',
  },
  component: ComponentXxx,
}).then();

$drawer({
  props: {
    title: 'xxx',
    size: '500px',
  },
  component: ComponentXxx,
}).then();
```
