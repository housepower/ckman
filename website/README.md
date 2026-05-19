# CKMAN 文档站

基于 [VitePress](https://vitepress.dev/) 的 CKMAN 在线文档。

## 本地预览

```bash
cd website
npm install
npm run docs:dev
```

打开 http://localhost:5173/docs/ 查看效果。

## 构建

```bash
npm run docs:build
```

产物输出到 `.vitepress/dist/`。

## 目录约定

```
website/
├── .vitepress/config.ts   # 站点配置（导航、侧边栏、搜索）
├── index.md               # 首页（hero + features）
├── guide/                 # 入门指南
├── deploy/                # 部署/升级/高可用
├── config/                # 配置文件说明
├── features/              # 功能介绍（按 IA 分组）
├── reference/             # API、错误码、Changelog、FAQ
└── public/                # 静态资源（图片放在 public/img/）
```

## 截图待补

截图占位约定：

```md
![描述](/img/section/page-name.png)
<!-- TODO(screenshot): 描述需要框选的关键字段 -->
```

补图后删除对应 `TODO(screenshot)` 注释即可。可全局搜 `TODO(screenshot)` 找出所有待补点。
