---
layout: home

hero:
  name: CKMAN
  text: ClickHouse 集群管理平台
  tagline: 面向企业级数据团队的一站式 Web 控制台 —— 让部署、运维、监控与备份变得可视、可控、可靠。
  image:
    src: /hero.svg
    alt: CKMAN
  actions:
    - theme: brand
      text: 快速开始
      link: /guide/quick-start
    - theme: alt
      text: 下载
      link: /download
    - theme: alt
      text: 架构设计
      link: /guide/architecture
    - theme: alt
      text: GitHub
      link: https://github.com/housepower/ckman
---

<script setup>
import { withBase } from 'vitepress';
</script>

<section class="emp-trust">
  <div class="emp-container">
    <p class="emp-trust__title">为 ClickHouse 而生 · 企业级运维平台</p>
    <ul class="emp-trust__list">
      <li><span class="emp-trust__num">Apache 2.0</span><span>开源协议</span></li>
      <li><span class="emp-trust__num">10<sup>2</sup>+</span><span>节点规模实战</span></li>
      <li><span class="emp-trust__num">4</span><span>持久层后端</span></li>
      <li><span class="emp-trust__num">RBAC</span><span>三级权限模型</span></li>
    </ul>
  </div>
</section>

<section class="emp-section">
  <div class="emp-container">
    <header class="emp-section__head">
      <p class="emp-section__eyebrow">核心能力</p>
      <h2 class="emp-section__title">一个平台，覆盖 ClickHouse 全生命周期</h2>
      <p class="emp-section__sub">从集群部署到数据备份，CKMAN 把原本需要 SSH 操作的运维动作整合到一个 Web 界面，并通过统一 API 对接你的内部系统。</p>
    </header>
    <div class="emp-grid emp-grid--3">
      <article class="emp-card">
        <div class="emp-card__num">01</div>
        <h3>集群生命周期</h3>
        <p>部署 · 升级 · 销毁 · 节点增删全部可视化执行，支持滚动升级、失败重试与任务回看，无需逐台 SSH。</p>
      </article>
      <article class="emp-card">
        <div class="emp-card__num">02</div>
        <h3>数据治理</h3>
        <p>分布式表、分区、TTL、物化视图、DML、归档、purge —— 表层全套运维操作直接在界面完成。</p>
      </article>
      <article class="emp-card">
        <div class="emp-card__num">03</div>
        <h3>备份与恢复</h3>
        <p>定时策略 + 增量去重，支持本地与 S3 多目标；恢复操作走异步任务，可追踪每个节点的进度。</p>
      </article>
      <article class="emp-card">
        <div class="emp-card__num">04</div>
        <h3>原生监控</h3>
        <p>直读 ClickHouse 系统表，开箱即看 KPI · 慢查询 · 表合并 · DDL 队列；可选接入 Prometheus 做长期存储。</p>
      </article>
      <article class="emp-card">
        <div class="emp-card__num">05</div>
        <h3>权限与安全</h3>
        <p>三级 RBAC + JWT + 客户端 IP 绑定，支持统一门户 Token 集成，所有敏感字段端到端加密。</p>
      </article>
      <article class="emp-card">
        <div class="emp-card__num">06</div>
        <h3>高可用部署</h3>
        <p>多实例水平扩展 + Nacos 服务发现，配合 MySQL / PostgreSQL / DM8 多种持久层后端，无单点。</p>
      </article>
    </div>
  </div>
</section>

<section class="emp-section emp-section--alt">
  <div class="emp-container">
    <header class="emp-section__head">
      <p class="emp-section__eyebrow">为什么选择 CKMAN</p>
      <h2 class="emp-section__title">让 DBA 与运维同学告别"手工时代"</h2>
    </header>
    <div class="emp-compare">
      <div class="emp-compare__col emp-compare__col--bad">
        <div class="emp-compare__tag">传统手工运维</div>
        <ul>
          <li>逐台 SSH 修改 <code>config.xml</code> / <code>users.xml</code> / <code>metrika.xml</code></li>
          <li>升级 / 缩容缺乏统一流程，回滚靠手动</li>
          <li>监控数据散落在 Prometheus / Grafana / 命令行</li>
          <li>权限粗放，难做账号级审计</li>
          <li>跨集群操作需要重复劳动</li>
        </ul>
      </div>
      <div class="emp-compare__col emp-compare__col--good">
        <div class="emp-compare__tag">CKMAN 一站式管控</div>
        <ul>
          <li>Web 界面 / API 一键完成全集群部署与配置变更</li>
          <li>滚动升级 + 任务跟踪 + 失败重试，过程可审计</li>
          <li>监控、查询、备份、节点日志统一入口</li>
          <li>管理员 / 普通用户 / 游客 三级权限，每一次操作有记录</li>
          <li>多集群统一纳管，跨集群任务批量执行</li>
        </ul>
      </div>
    </div>
  </div>
</section>

<section class="emp-section">
  <div class="emp-container">
    <header class="emp-section__head">
      <p class="emp-section__eyebrow">适用场景</p>
      <h2 class="emp-section__title">无论 1 个还是 N 个集群，都能轻松管控</h2>
    </header>
    <div class="emp-grid emp-grid--3">
      <article class="emp-usecase">
        <h3>多业务线共享平台</h3>
        <p>不同业务团队各自管理 ClickHouse 集群，统一接入 CKMAN 后由平台运维做集中托管，账号级隔离。</p>
      </article>
      <article class="emp-usecase">
        <h3>私有云 / 内部数据平台</h3>
        <p>作为大数据中台的一部分，通过 API 对接调度系统、CMDB、统一门户，纳入企业 IT 工作流。</p>
      </article>
      <article class="emp-usecase">
        <h3>DBA 团队中心化运维</h3>
        <p>少量 DBA 管理大量集群，通过 CKMAN 降低重复操作，把精力释放给容量规划、性能优化等更有价值的工作。</p>
      </article>
    </div>
  </div>
</section>

<section class="emp-cta">
  <div class="emp-container">
    <h2>立即在 5 分钟内跑起来</h2>
    <p>提供 RPM · DEB · tar.gz · Docker · Kubernetes 五种发行方式，单个二进制即可启动，再大的集群也只需要一个入口。</p>
    <div class="emp-cta__actions">
      <a class="emp-btn emp-btn--primary" :href="withBase('/guide/quick-start.html')">开始部署</a>
      <a class="emp-btn emp-btn--ghost" :href="withBase('/download.html')">下载安装包</a>
      <a class="emp-btn emp-btn--ghost" :href="withBase('/deploy/install.html')">安装手册</a>
    </div>
  </div>
</section>

<style>
.emp-container {
  width: 100%;
  max-width: 1240px;
  margin: 0 auto;
  padding: 0 24px;
  box-sizing: border-box;
}

/* ===== Trust strip ===== */
.emp-trust {
  border-top: 1px solid var(--vp-c-divider);
  border-bottom: 1px solid var(--vp-c-divider);
  background: var(--vp-c-bg-soft);
  padding: 36px 0;
  margin-top: 24px;
}
.emp-trust__title {
  margin: 0 0 20px;
  text-align: center;
  font-size: 13px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--vp-c-text-2);
  font-weight: 600;
}
.emp-trust__list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 24px;
  text-align: center;
}
.emp-trust__list li {
  display: flex;
  flex-direction: column;
  gap: 4px;
  align-items: center;
}
.emp-trust__num {
  font-size: 28px;
  font-weight: 800;
  line-height: 1.1;
  color: var(--vp-c-brand-1);
  letter-spacing: -0.01em;
}
.emp-trust__num sup {
  font-size: 18px;
  font-weight: 700;
}
.emp-trust__list span:last-child {
  font-size: 13px;
  color: var(--vp-c-text-2);
}

/* ===== Section ===== */
.emp-section {
  padding: 96px 0;
}
.emp-section--alt {
  background: var(--vp-c-bg-soft);
}
.emp-section__head {
  text-align: center;
  max-width: 760px;
  margin: 0 auto 56px;
}
.emp-section__eyebrow {
  margin: 0 0 12px;
  font-size: 12px;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--vp-c-brand-1);
  font-weight: 700;
}
.emp-section__title {
  margin: 0;
  font-size: 36px;
  font-weight: 800;
  letter-spacing: -0.02em;
  line-height: 1.2;
  color: var(--vp-c-text-1);
}
.emp-section__sub {
  margin: 16px auto 0;
  max-width: 640px;
  font-size: 16px;
  line-height: 1.7;
  color: var(--vp-c-text-2);
}

/* ===== Grid ===== */
.emp-grid {
  display: grid;
  gap: 20px;
}
.emp-grid--3 {
  grid-template-columns: repeat(3, 1fr);
}

/* ===== Cards ===== */
.emp-card {
  position: relative;
  padding: 32px 28px 28px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  background: var(--vp-c-bg);
  transition: border-color 0.18s ease, transform 0.18s ease, box-shadow 0.18s ease;
}
.emp-card:hover {
  border-color: var(--vp-c-brand-3);
  transform: translateY(-2px);
  box-shadow: 0 12px 32px -16px rgba(201, 161, 0, 0.25);
}
.emp-card__num {
  position: absolute;
  top: 18px;
  right: 22px;
  font-size: 13px;
  font-weight: 700;
  letter-spacing: 0.05em;
  color: var(--vp-c-brand-1);
  opacity: 0.7;
}
.emp-card h3 {
  margin: 0 0 12px;
  font-size: 19px;
  font-weight: 700;
  color: var(--vp-c-text-1);
  letter-spacing: -0.01em;
}
.emp-card p {
  margin: 0;
  font-size: 14.5px;
  line-height: 1.7;
  color: var(--vp-c-text-2);
}

/* ===== Compare ===== */
.emp-compare {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 24px;
}
.emp-compare__col {
  padding: 32px;
  border-radius: 12px;
  background: var(--vp-c-bg);
  border: 1px solid var(--vp-c-divider);
}
.emp-compare__col--good {
  border-color: var(--vp-c-brand-3);
  background: linear-gradient(180deg, var(--vp-c-bg) 0%, var(--vp-c-brand-soft) 200%);
}
.emp-compare__tag {
  display: inline-block;
  padding: 4px 12px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  margin-bottom: 20px;
}
.emp-compare__col--bad .emp-compare__tag {
  background: var(--vp-c-default-soft);
  color: var(--vp-c-text-2);
}
.emp-compare__col--good .emp-compare__tag {
  background: var(--vp-c-brand-1);
  color: #ffffff;
}
.emp-compare ul {
  margin: 0;
  padding: 0;
  list-style: none;
}
.emp-compare li {
  position: relative;
  padding: 10px 0 10px 28px;
  font-size: 14.5px;
  line-height: 1.6;
  color: var(--vp-c-text-1);
  border-bottom: 1px solid var(--vp-c-divider);
}
.emp-compare li:last-child {
  border-bottom: none;
}
.emp-compare__col--bad li::before {
  content: "×";
  position: absolute;
  left: 0;
  top: 9px;
  width: 20px;
  height: 20px;
  line-height: 18px;
  text-align: center;
  border-radius: 50%;
  background: var(--vp-c-default-soft);
  color: var(--vp-c-text-3);
  font-size: 14px;
  font-weight: 700;
}
.emp-compare__col--good li::before {
  content: "✓";
  position: absolute;
  left: 0;
  top: 9px;
  width: 20px;
  height: 20px;
  line-height: 19px;
  text-align: center;
  border-radius: 50%;
  background: var(--vp-c-brand-1);
  color: #ffffff;
  font-size: 12px;
  font-weight: 700;
}
.emp-compare code {
  padding: 1px 6px;
  border-radius: 4px;
  background: var(--vp-c-bg-soft);
  font-size: 12.5px;
  color: var(--vp-c-brand-1);
}

/* ===== Use cases ===== */
.emp-usecase {
  padding: 28px;
  border-radius: 12px;
  background: var(--vp-c-bg);
  border-top: 3px solid var(--vp-c-brand-2);
  border-right: 1px solid var(--vp-c-divider);
  border-bottom: 1px solid var(--vp-c-divider);
  border-left: 1px solid var(--vp-c-divider);
}
.emp-usecase h3 {
  margin: 0 0 12px;
  font-size: 18px;
  font-weight: 700;
  color: var(--vp-c-text-1);
}
.emp-usecase p {
  margin: 0;
  font-size: 14.5px;
  line-height: 1.7;
  color: var(--vp-c-text-2);
}

/* ===== CTA ===== */
.emp-cta {
  position: relative;
  padding: 96px 0;
  background: linear-gradient(135deg, #C9A100 0%, #856900 100%);
  color: #ffffff;
  overflow: hidden;
}
.emp-cta::before {
  content: "";
  position: absolute;
  inset: 0;
  background-image:
    radial-gradient(circle at 20% 30%, rgba(255, 255, 255, 0.08) 0%, transparent 40%),
    radial-gradient(circle at 80% 70%, rgba(255, 255, 255, 0.06) 0%, transparent 40%);
  pointer-events: none;
}
.emp-cta .emp-container {
  position: relative;
  text-align: center;
}
.emp-cta h2 {
  margin: 0 0 16px;
  font-size: 36px;
  font-weight: 800;
  letter-spacing: -0.02em;
  color: #ffffff;
}
.emp-cta p {
  margin: 0 auto 36px;
  max-width: 640px;
  font-size: 16px;
  line-height: 1.7;
  color: rgba(255, 255, 255, 0.85);
}
.emp-cta__actions {
  display: flex;
  justify-content: center;
  gap: 16px;
  flex-wrap: wrap;
}
.emp-btn {
  display: inline-flex;
  align-items: center;
  padding: 12px 28px;
  border-radius: 8px;
  font-weight: 700;
  text-decoration: none !important;
  transition: transform 0.15s ease, background-color 0.15s ease, color 0.15s ease;
  border: 1px solid transparent;
}
.emp-btn:hover {
  transform: translateY(-1px);
}
.emp-btn--primary {
  background: #ffffff;
  color: #856900 !important;
}
.emp-btn--primary:hover {
  background: #FDF9E6;
}
.emp-btn--ghost {
  background: transparent;
  color: #ffffff !important;
  border-color: rgba(255, 255, 255, 0.5);
}
.emp-btn--ghost:hover {
  background: rgba(255, 255, 255, 0.12);
  border-color: rgba(255, 255, 255, 0.8);
}

/* ===== Dark theme tweaks ===== */
.dark .emp-card:hover {
  box-shadow: 0 12px 32px -16px rgba(224, 186, 50, 0.3);
}
.dark .emp-cta {
  background: linear-gradient(135deg, #856900 0%, #423300 100%);
}
.dark .emp-btn--primary {
  background: #ECCF57;
  color: #1a1500 !important;
}
.dark .emp-btn--primary:hover {
  background: #F4E388;
}

/* ===== Responsive ===== */
@media (max-width: 960px) {
  .emp-section { padding: 64px 0; }
  .emp-section__title, .emp-cta h2 { font-size: 28px; }
  .emp-section__head { margin-bottom: 40px; }
  .emp-grid--3 { grid-template-columns: 1fr 1fr; }
  .emp-trust__list { grid-template-columns: repeat(2, 1fr); gap: 24px 16px; }
  .emp-compare { grid-template-columns: 1fr; }
}
@media (max-width: 640px) {
  .emp-grid--3 { grid-template-columns: 1fr; }
  .emp-trust__num { font-size: 22px; }
  .emp-section { padding: 56px 0; }
  .emp-cta { padding: 64px 0; }
}
</style>
