// 构建期抓取 GitHub Releases /latest 并写入 public/release-latest.json，
// 让下载页直接读站内静态文件，不再让访客打 api.github.com（避免 403 限流）。
//
// 容错策略：
//   - 网络失败 / 限流 / 非 200 → 打印 warning 后跳过，构建继续
//   - 写到 public/release-latest.json；download.md 找不到该文件时回退到实时 API
//   - 若环境变量 GITHUB_TOKEN 存在则带上以提高限流（5000/小时）
import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const REPO = 'housepower/ckman';
const here = dirname(fileURLToPath(import.meta.url));
const outPath = join(here, '..', 'public', 'release-latest.json');

async function main() {
  const headers = { Accept: 'application/vnd.github+json' };
  if (process.env.GITHUB_TOKEN) {
    headers.Authorization = `token ${process.env.GITHUB_TOKEN}`;
  }

  try {
    const resp = await fetch(
      `https://api.github.com/repos/${REPO}/releases/latest`,
      { headers, signal: AbortSignal.timeout(15000) },
    );
    if (!resp.ok) {
      console.warn(`[fetch-latest-release] HTTP ${resp.status} — skip snapshot`);
      return;
    }
    const data = await resp.json();

    // 只保留下载页用到的字段，避免快照过大
    const snapshot = {
      tag_name: data.tag_name,
      name: data.name,
      html_url: data.html_url,
      published_at: data.published_at,
      body: data.body,
      assets: (data.assets || []).map(a => ({
        name: a.name,
        size: a.size,
        browser_download_url: a.browser_download_url,
      })),
      _fetched_at: new Date().toISOString(),
    };

    mkdirSync(dirname(outPath), { recursive: true });
    writeFileSync(outPath, JSON.stringify(snapshot, null, 2));
    console.log(`[fetch-latest-release] snapshot for ${data.tag_name} -> ${outPath}`);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    console.warn(`[fetch-latest-release] failed: ${msg} — skip snapshot`);
  }
}

main();
