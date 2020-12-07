# 前端文件公共库

支持 element-ui、@vue/cli@4

## 迁移方式

1. 添加子模块 <https://git.eoitek.net/fe/common.git> 至 src/common 目录
1. 升级 @vue/cli 依赖到 4 版本，删除 yarn.lock 后重新安装依赖
1. 删除 `@/app/detectFeatures，更改` app/index 里的引用为 `import '@/common/app/detectFeatures'`
1. 删除 `@/app/filter` 里相关过滤器，添加引用 `import '@/common/app/filters'`
1. 删除 `@/app/run`（看情况保留登录验证逻辑），添加引用 `import '@/common/app/runs'`
1. 删除 `@/constants/Project`，`index` 中添加引用 `export * from '@/common/constants'`
1. 删除 `@/helpers` 里相关文件，`index` 中添加引用 `export * from '@/common/helpers'`
1. 删除 `@/services` 里相关文件（除 router），`index` 中添加引用 `export * from '@/common/services'`
1. `@/services/router` 中删除 `cancelToken` 相关逻辑
1. 删除 `@/typings`
1. 删除 `@/views/components` 里相关文件，`index` 中添加引用 `export * from '@/common/components'`
1. 添加 `@/app/variables.scss` 内容如：`$primary-color: #00C8C1;`
1. 添加 `@/app/global.scss` 内容：`@import "~@/common/app/global";`
1. 删除 `@/models` 里相关文件，`index` 中添加引用 `export * from '@/common/models'`
1. `@/VueComponentBase` 删除内容，添加引用 `export * from '@/common/VueComponentBase'`
1. `babel.config.js` 修改为 `['@vue/app', { useBuiltIns: 'entry' }]`
1. 删除 `git-repo-info.js`
1. 删除 `vue.config.js` 中 `configureWebpack` 块，添加引用 `require('./src/common/configureWebpack')`，并将其放入导出的对象中
1. `main.tsx` 的 `render` 函数中添加引用 `<VueProgressbar ref="progressbar" />`
