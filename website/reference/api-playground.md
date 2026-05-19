---
layout: page
sidebar: false
aside: false
---

<script setup>
import { withBase } from 'vitepress';
</script>

<div class="playground">
  <header class="playground__head">
    <h1>API Playground</h1>
    <p>
      基于 Swagger 自动生成的接口浏览器（只读）。需要 try-it-out 请在自己的
      ckman 实例打开 <code>swagger_enable</code> 后访问
      <code>http://&lt;ckman-host&gt;:8808/swagger/index.html</code>。
    </p>
  </header>
  <iframe
    class="playground__frame"
    :src="withBase('/api-playground/index.html')"
    title="CKMAN API Playground"
    loading="lazy"
  ></iframe>
</div>

<style>
.playground {
  width: 100%;
  max-width: 1400px;
  margin: 0 auto;
  padding: 24px 24px 0;
  box-sizing: border-box;
}
.playground__head h1 {
  margin: 0 0 8px;
  font-size: 28px;
  font-weight: 700;
  color: var(--vp-c-text-1);
}
.playground__head p {
  margin: 0 0 16px;
  color: var(--vp-c-text-2);
  line-height: 1.6;
}
.playground__head code {
  padding: 2px 6px;
  border-radius: 4px;
  background: var(--vp-c-bg-soft);
  color: var(--vp-c-text-1);
  font-size: 13px;
}
.playground__frame {
  width: 100%;
  height: calc(100vh - 220px);
  min-height: 720px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  background: #fff;
}
</style>
