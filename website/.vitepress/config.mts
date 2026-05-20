import { defineConfig } from 'vitepress';

const currentYear = new Date().getFullYear();
// base 通过环境变量切换，默认走 embed 进 ckman 二进制的 /docs/。
// CI 部署到 GitHub Pages 时设 DOCS_BASE=/<repo>/docs/。
const base = process.env.DOCS_BASE || '/docs/';

export default defineConfig({
  lang: 'zh-CN',
  title: 'CKMAN',
  description: 'ClickHouse 集群管理与监控平台',
  // 关闭 cleanUrls：URL 形如 <base>guide/quick-start.html
  // 与 Go 端 server/embed.go 的静态文件 Serve 中间件保持一致（要求文件名精确命中）
  cleanUrls: false,
  lastUpdated: true,
  base,
  srcExclude: ['README.md'],

  head: [
    ['link', { rel: 'icon', href: `${base}favicon.ico` }],
  ],

  themeConfig: {
    logo: '/favicon.ico',
    siteTitle: 'CKMAN',

    nav: [
      { text: '指南', link: '/guide/introduction', activeMatch: '/guide/' },
      { text: '下载', link: '/download', activeMatch: '/download' },
      { text: '部署', link: '/deploy/install', activeMatch: '/deploy/' },
      { text: '配置', link: '/config/overview', activeMatch: '/config/' },
      { text: '功能', link: '/features/cluster/deploy', activeMatch: '/features/' },
      { text: '参考', link: '/reference/api', activeMatch: '/reference/' },
    ],

    sidebar: {
      '/guide/': [
        {
          text: '入门',
          items: [
            { text: '什么是 CKMAN', link: '/guide/introduction' },
            { text: '快速开始', link: '/guide/quick-start' },
            { text: '架构设计', link: '/guide/architecture' },
            { text: '核心概念', link: '/guide/concepts' },
          ],
        },
      ],
      '/deploy/': [
        {
          text: '部署',
          items: [
            { text: '安装', link: '/deploy/install' },
            { text: '升级 CKMAN', link: '/deploy/upgrade' },
            { text: '高可用部署', link: '/deploy/high-availability' },
          ],
        },
      ],
      '/config/': [
        {
          text: '配置',
          items: [
            { text: '配置总览', link: '/config/overview' },
            { text: 'server', link: '/config/server' },
            { text: 'clickhouse', link: '/config/clickhouse' },
            { text: 'log', link: '/config/log' },
            { text: 'cron', link: '/config/cron' },
            { text: 'persistent_config', link: '/config/persistent' },
            { text: 'nacos', link: '/config/nacos' },
          ],
        },
      ],
      '/features/': [
        {
          text: '集群管理',
          collapsed: false,
          items: [
            { text: '部署集群', link: '/features/cluster/deploy' },
            { text: '导入集群', link: '/features/cluster/import' },
            { text: '升级集群', link: '/features/cluster/upgrade' },
            { text: '销毁集群', link: '/features/cluster/destroy' },
            { text: '节点管理', link: '/features/cluster/nodes' },
          ],
        },
        {
          text: '监控与会话',
          collapsed: false,
          items: [
            { text: '监控指标', link: '/features/monitoring/metrics' },
            { text: '表 & 会话管理', link: '/features/tables/overview' },
          ],
        },
        {
          text: '数据操作',
          collapsed: false,
          items: [
            { text: 'Query 管理', link: '/features/query/overview' },
            { text: '分区管理', link: '/features/partition/overview' },
            { text: '物化视图', link: '/features/materialized-view/overview' },
            { text: '数据备份与恢复', link: '/features/backup/overview' },
          ],
        },
        {
          text: '系统管理',
          collapsed: false,
          items: [
            { text: '任务管理', link: '/features/tasks/overview' },
            { text: '用户与角色权限', link: '/features/users/overview' },
            { text: '节点日志', link: '/features/logs/overview' },
            { text: '配置管理', link: '/features/settings/overview' },
          ],
        },
      ],
      '/reference/': [
        {
          text: '参考',
          items: [
            { text: 'API 接口', link: '/reference/api' },
            { text: 'API Playground', link: '/reference/api-playground' },
            { text: 'ckmanctl CLI', link: '/reference/ckmanctl' },
            { text: '错误码', link: '/reference/error-codes' },
            { text: '更新日志', link: '/reference/changelog' },
            { text: '常见问题', link: '/reference/troubleshooting' },
          ],
        },
      ],
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/housepower/ckman' },
    ],

    search: {
      provider: 'local',
      options: {
        translations: {
          button: { buttonText: '搜索', buttonAriaLabel: '搜索文档' },
          modal: {
            displayDetails: '显示详情',
            resetButtonTitle: '清除搜索',
            backButtonTitle: '关闭搜索',
            noResultsText: '无结果',
            footer: {
              selectText: '选择',
              navigateText: '切换',
              closeText: '关闭',
            },
          },
        },
      },
    },

    outline: {
      level: [2, 3],
      label: '本页目录',
    },

    docFooter: {
      prev: '上一页',
      next: '下一页',
    },

    lastUpdated: {
      text: '最后更新',
      formatOptions: { dateStyle: 'short', timeStyle: 'short' },
    },

    editLink: {
      pattern: 'https://github.com/housepower/ckman/edit/main/website/:path',
      text: '在 GitHub 上编辑此页',
    },

    footer: {
      message: '基于 Apache 2.0 协议发布',
      copyright: `Copyright © 2016–${currentYear} 上海擎创信息技术有限公司`,
    },
  },
});
