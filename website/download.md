---
title: 下载
layout: page
sidebar: false
---

<script setup>
import { ref, onMounted, computed } from 'vue';
import { withBase } from 'vitepress';

const RELEASE_LATEST = 'https://github.com/housepower/ckman/releases/latest';
const RELEASE_ALL = 'https://github.com/housepower/ckman/releases';
const DOCKER_REPO = 'https://quay.io/repository/housepower/ckman';
const API_LATEST = 'https://api.github.com/repos/housepower/ckman/releases/latest';
const CACHE_KEY = 'ckman-latest-release';
const CACHE_TTL = 60 * 60 * 1000; // 1 小时

const release = ref(null);
const error = ref('');
const loading = ref(true);

// 把 release.assets 按格式/架构归类
function pickAsset(assets, predicate) {
  return assets.find(predicate) || null;
}

// 每个发行格式的 SVG 图标（lucide.dev 风格的描线图，统一 24×24）
const ICON_TARGZ  = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg>';
const ICON_RPM    = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m7.5 4.27 9 5.15"/><path d="M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z"/><path d="m3.3 7 8.7 5 8.7-5"/><path d="M12 22V12"/></svg>';
const ICON_DEB    = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="4" width="20" height="5" rx="2"/><path d="M4 9v9a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9"/><path d="M10 13h4"/></svg>';
const ICON_DOCKER = '<svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor"><rect x="2" y="14" width="3" height="3" rx="0.4"/><rect x="6" y="14" width="3" height="3" rx="0.4"/><rect x="10" y="14" width="3" height="3" rx="0.4"/><rect x="14" y="14" width="3" height="3" rx="0.4"/><rect x="6" y="10" width="3" height="3" rx="0.4"/><rect x="10" y="10" width="3" height="3" rx="0.4"/><rect x="14" y="10" width="3" height="3" rx="0.4"/><rect x="10" y="6" width="3" height="3" rx="0.4"/><path d="M18 14c1.5 0 3-1 3.5-2.5-1-.5-2 0-2.5.5-.3-1.5-1.5-2-2.5-2-.5 1.5 0 3 1.5 4z" stroke="none"/></svg>';

const buckets = computed(() => {
  if (!release.value) return [];
  const a = release.value.assets || [];
  const has = (n, ...subs) => subs.every(s => n.toLowerCase().includes(s));
  return [
    {
      key: 'tar-x86', tag: '.tar.gz', icon: ICON_TARGZ, os: 'Linux', arch: 'x86_64',
      asset: pickAsset(a, x => /\.tar\.gz$|\.tgz$/i.test(x.name) && has(x.name, 'x86_64') || /\.tar\.gz$|\.tgz$/i.test(x.name) && has(x.name, 'amd64')),
    },
    {
      key: 'tar-arm', tag: '.tar.gz', icon: ICON_TARGZ, os: 'Linux', arch: 'arm64',
      asset: pickAsset(a, x => /\.tar\.gz$|\.tgz$/i.test(x.name) && (has(x.name, 'arm64') || has(x.name, 'aarch64'))),
    },
    {
      key: 'rpm-x86', tag: '.rpm',    icon: ICON_RPM,   os: 'RHEL / CentOS / Rocky', arch: 'x86_64',
      asset: pickAsset(a, x => /\.rpm$/i.test(x.name) && has(x.name, 'x86_64')),
    },
    {
      key: 'rpm-arm', tag: '.rpm',    icon: ICON_RPM,   os: 'RHEL / CentOS / Rocky', arch: 'aarch64',
      asset: pickAsset(a, x => /\.rpm$/i.test(x.name) && has(x.name, 'aarch64')),
    },
    {
      key: 'deb-x86', tag: '.deb',    icon: ICON_DEB,   os: 'Debian / Ubuntu', arch: 'amd64',
      asset: pickAsset(a, x => /\.deb$/i.test(x.name) && has(x.name, 'amd64')),
    },
    {
      key: 'deb-arm', tag: '.deb',    icon: ICON_DEB,   os: 'Debian / Ubuntu', arch: 'arm64',
      asset: pickAsset(a, x => /\.deb$/i.test(x.name) && has(x.name, 'arm64')),
    },
  ];
});

function fmtSize(bytes) {
  if (!bytes) return '';
  const mb = bytes / 1024 / 1024;
  if (mb >= 1) return mb.toFixed(1) + ' MB';
  return Math.round(bytes / 1024) + ' KB';
}

function fmtDate(iso) {
  if (!iso) return '';
  try {
    return new Date(iso).toLocaleDateString('zh-CN', { year: 'numeric', month: 'long', day: 'numeric' });
  } catch { return iso; }
}

onMounted(async () => {
  // 客户端缓存，避免每次访问都打 GitHub API（匿名限流 60/小时）
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    if (raw) {
      const { ts, data } = JSON.parse(raw);
      if (Date.now() - ts < CACHE_TTL) {
        release.value = data;
        loading.value = false;
        return;
      }
    }
  } catch (_) { /* localStorage 不可用就跳过 */ }

  try {
    const resp = await fetch(API_LATEST, { headers: { Accept: 'application/vnd.github+json' } });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    release.value = data;
    try { localStorage.setItem(CACHE_KEY, JSON.stringify({ ts: Date.now(), data })); } catch (_) {}
  } catch (e) {
    error.value = (e && e.message) || String(e);
  } finally {
    loading.value = false;
  }
});
</script>

<div class="dl-page">
  <section class="dl-hero">
    <div class="dl-container">
      <p class="dl-hero__eyebrow">CKMAN 发行版</p>
      <h1 class="dl-hero__title">选一种方式开始使用</h1>
      <p class="dl-hero__sub">
        <span v-if="loading">正在获取最新版本…</span>
        <span v-else-if="release">
          最新版本：<strong class="dl-hero__ver">{{ release.tag_name }}</strong><span v-if="release.published_at">，发布于 {{ fmtDate(release.published_at) }}</span>
        </span>
        <span v-else>
          无法连接 GitHub API（{{ error }}）。请直接前往 GitHub Releases。
        </span>
      </p>
      <div class="dl-hero__actions">
        <a v-if="release" class="dl-btn dl-btn--primary" :href="release.html_url" target="_blank" rel="noopener">
          查看本次发布说明
        </a>
        <a class="dl-btn dl-btn--ghost" :href="RELEASE_ALL" target="_blank" rel="noopener">
          浏览所有版本
        </a>
      </div>
    </div>
  </section>
  <section class="dl-section">
    <div class="dl-container">
      <h2 class="dl-section__title">按平台选择 · 点击直接下载</h2>
      <div class="dl-grid">
        <div v-for="b in buckets" :key="b.key" class="dl-card-wrap">
          <a v-if="b.asset" class="dl-card dl-card--ready" :href="b.asset.browser_download_url" :download="b.asset.name">
            <div class="dl-card__head">
              <div class="dl-card__tag"><span class="dl-card__tag-icon" v-html="b.icon"></span><span>{{ b.tag }}</span></div>
              <div class="dl-card__arch">{{ b.arch }}</div>
            </div>
            <h3>{{ b.os }}</h3>
            <p class="dl-card__name" :title="b.asset.name">{{ b.asset.name }}</p>
            <div class="dl-card__meta">
              <span>{{ fmtSize(b.asset.size) }}</span>
              <span v-if="b.asset.download_count != null">· 下载 {{ b.asset.download_count }} 次</span>
            </div>
            <span class="dl-card__cta">立即下载 ↓</span>
          </a>
          <div v-else class="dl-card dl-card--unavailable">
            <div class="dl-card__head">
              <div class="dl-card__tag"><span class="dl-card__tag-icon" v-html="b.icon"></span><span>{{ b.tag }}</span></div>
              <div class="dl-card__arch">{{ b.arch }}</div>
            </div>
            <h3>{{ b.os }}</h3>
            <p class="dl-card__name">
              <span v-if="loading">加载中…</span>
              <span v-else>该平台本次未发布对应安装包</span>
            </p>
            <a class="dl-card__cta dl-card__cta--alt" :href="RELEASE_LATEST" target="_blank" rel="noopener">
              前往 Releases →
            </a>
          </div>
        </div>
        <a class="dl-card dl-card--ready" :href="DOCKER_REPO" target="_blank" rel="noopener">
          <div class="dl-card__head">
            <div class="dl-card__tag"><span class="dl-card__tag-icon" v-html="ICON_DOCKER"></span><span>Docker</span></div>
            <div class="dl-card__arch">multi-arch</div>
          </div>
          <h3>容器镜像</h3>
          <p class="dl-card__name"><code>quay.io/housepower/ckman:latest</code></p>
          <div class="dl-card__meta">
            <span>quay.io 托管</span>
          </div>
          <span class="dl-card__cta">查看镜像 →</span>
        </a>
      </div>
    </div>
  </section>
  <section class="dl-section dl-section--alt">
    <div class="dl-container">
      <div class="dl-info">
        <div>
          <h3>系统要求</h3>
          <ul>
            <li>Linux <code>x86_64</code> 或 <code>arm64</code></li>
            <li>CentOS 7+ / RHEL 8+ / Rocky 8+ / Ubuntu 18+ / Debian 10+</li>
            <li>最低 2 核 CPU / 2 GB 内存 / 5 GB 磁盘</li>
            <li>建议 4 核 / 4 GB+（管理 10 节点以上集群时）</li>
          </ul>
        </div>
        <div>
          <h3>开始使用</h3>
          <ul>
            <li><a :href="withBase('/guide/quick-start.html')">5 分钟快速开始</a></li>
            <li><a :href="withBase('/deploy/install.html')">完整安装指南（含 Kubernetes）</a></li>
            <li><a :href="withBase('/deploy/upgrade.html')">从旧版升级</a></li>
            <li><a :href="withBase('/deploy/high-availability.html')">高可用部署</a></li>
          </ul>
        </div>
      </div>
    </div>
  </section>
</div>

<style>
.dl-page {
  padding-bottom: 64px;
}
.dl-container {
  width: 100%;
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 24px;
  box-sizing: border-box;
}

/* Hero */
.dl-hero {
  padding: 80px 0 56px;
  text-align: center;
  border-bottom: 1px solid var(--vp-c-divider);
  background: linear-gradient(180deg, var(--vp-c-bg-soft) 0%, transparent 100%);
}
.dl-hero__eyebrow {
  margin: 0 0 12px;
  font-size: 12px;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--vp-c-brand-1);
  font-weight: 700;
}
.dl-hero__title {
  margin: 0 0 16px;
  font-size: 40px;
  font-weight: 800;
  letter-spacing: -0.02em;
  color: var(--vp-c-text-1);
}
.dl-hero__sub {
  margin: 0 auto 28px;
  max-width: 720px;
  font-size: 16px;
  line-height: 1.7;
  color: var(--vp-c-text-2);
}
.dl-hero__ver {
  padding: 2px 10px;
  border-radius: 999px;
  background: var(--vp-c-brand-soft);
  color: var(--vp-c-brand-1);
  font-family: var(--vp-font-family-mono, monospace);
  font-weight: 700;
}
.dl-hero__actions {
  display: flex;
  justify-content: center;
  gap: 14px;
  flex-wrap: wrap;
}

.dl-btn {
  display: inline-flex;
  align-items: center;
  padding: 10px 22px;
  border-radius: 8px;
  font-weight: 700;
  text-decoration: none !important;
  transition: filter 0.15s ease, transform 0.15s ease;
  border: 1px solid transparent;
}
.dl-btn:hover {
  transform: translateY(-1px);
}
.dl-btn--primary {
  background: var(--vp-button-brand-bg);
  color: var(--vp-button-brand-text) !important;
}
.dl-btn--primary:hover {
  filter: brightness(0.92);
}
.dl-btn--ghost {
  background: transparent;
  color: var(--vp-c-text-1) !important;
  border-color: var(--vp-c-divider);
}
.dl-btn--ghost:hover {
  border-color: var(--vp-c-brand-1);
  color: var(--vp-c-brand-1) !important;
}

/* Sections */
.dl-section {
  padding: 64px 0;
}
.dl-section--alt {
  background: var(--vp-c-bg-soft);
  border-top: 1px solid var(--vp-c-divider);
  border-bottom: 1px solid var(--vp-c-divider);
}
.dl-section__title {
  margin: 0 0 32px;
  font-size: 24px;
  font-weight: 700;
  color: var(--vp-c-text-1);
}

/* Cards grid (3 列响应) */
.dl-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 18px;
}

.dl-card {
  position: relative;
  display: flex;
  flex-direction: column;
  padding: 22px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  background: var(--vp-c-bg);
  text-decoration: none !important;
  color: inherit !important;
  transition: border-color 0.18s ease, transform 0.18s ease, box-shadow 0.18s ease;
}
.dl-card--ready:hover {
  border-color: var(--vp-c-brand-3);
  transform: translateY(-2px);
  box-shadow: 0 12px 32px -16px rgba(201, 161, 0, 0.25);
}
.dl-card--unavailable {
  opacity: 0.65;
}

.dl-card__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}
.dl-card__tag {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 5px 12px 5px 8px;
  border-radius: 999px;
  background: var(--vp-c-brand-soft);
  color: var(--vp-c-brand-1);
  font-family: var(--vp-font-family-mono, monospace);
  font-size: 12.5px;
  font-weight: 700;
  letter-spacing: 0.01em;
  border: 1px solid color-mix(in srgb, var(--vp-c-brand-1) 25%, transparent);
}
.dl-card__tag-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 18px;
  height: 18px;
  border-radius: 4px;
  background: color-mix(in srgb, var(--vp-c-brand-1) 14%, transparent);
  color: var(--vp-c-brand-1);
}
.dl-card__tag-icon svg {
  width: 12px;
  height: 12px;
}
.dl-card__arch {
  font-family: var(--vp-font-family-mono, monospace);
  font-size: 12px;
  color: var(--vp-c-text-3);
  letter-spacing: 0.02em;
}
.dl-card h3 {
  margin: 0 0 6px;
  font-size: 17px;
  font-weight: 700;
  color: var(--vp-c-text-1);
}
.dl-card__name {
  margin: 0 0 10px;
  font-family: var(--vp-font-family-mono, monospace);
  font-size: 12px;
  line-height: 1.5;
  color: var(--vp-c-text-2);
  word-break: break-all;
}
.dl-card__name code {
  padding: 0;
  background: transparent;
  font-size: 12px;
}
.dl-card__meta {
  display: flex;
  gap: 6px;
  margin-bottom: 14px;
  font-size: 12.5px;
  color: var(--vp-c-text-3);
}
.dl-card__cta {
  margin-top: auto;
  font-size: 13.5px;
  font-weight: 700;
  color: var(--vp-c-brand-1);
}
.dl-card__cta--alt {
  text-decoration: none;
}

/* Info block */
.dl-info {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 48px;
}
.dl-info h3 {
  margin: 0 0 16px;
  font-size: 18px;
  font-weight: 700;
  color: var(--vp-c-text-1);
}
.dl-info ul {
  margin: 0;
  padding-left: 20px;
  color: var(--vp-c-text-2);
  font-size: 14.5px;
  line-height: 1.9;
}
.dl-info code {
  padding: 1px 6px;
  border-radius: 4px;
  background: var(--vp-c-bg);
  font-size: 12.5px;
}
.dl-info a {
  color: var(--vp-c-brand-1);
  text-decoration: none;
}
.dl-info a:hover {
  text-decoration: underline;
}

/* Responsive */
@media (max-width: 960px) {
  .dl-grid { grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 640px) {
  .dl-hero { padding: 56px 0 40px; }
  .dl-hero__title { font-size: 28px; }
  .dl-section { padding: 48px 0; }
  .dl-grid { grid-template-columns: 1fr; }
  .dl-info { grid-template-columns: 1fr; gap: 32px; }
}
</style>
