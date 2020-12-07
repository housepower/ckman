# 引入方式

* main.tsx 修改

```typescript jsx
const data = { 
  loading: { 
    status: 0,
    text: '', 
  },
};

// render 中添加
{this.loading.status ? <v-loading /> : null}
```

# 使用方式

1. $loading.increase();
1. $loading.decrease();
