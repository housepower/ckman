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
├── .vitepress/config.mts  # 站点配置（导航、侧边栏、搜索、主题）
├── .vitepress/theme/      # 自定义主题（CKMAN 金色品牌色覆盖）
├── index.md               # 首页（hero + 企业风版式）
├── guide/                 # 入门指南
├── deploy/                # 部署 / 升级 / 高可用
├── config/                # 配置文件说明
├── features/              # 功能介绍（按 IA 分组）
├── reference/             # API、ckmanctl、错误码、Changelog、FAQ
└── public/                # 静态资源（图片放在 public/img/<section>/<page>.png）
```

## 图片约定

- 路径与 markdown 路径对齐：`features/cluster/deploy.md` 引用 `/img/features/cluster/deploy-form.png`
- 优先 PNG（截图）；示意图可用 SVG
- 单图建议 < 500KB，超大时用 `pngquant` 或 Pillow `quantize(256)` 压一遍
