# LLM 与 AI 诊断

LLM 只负责解释 CKMAN 已生成的结构化诊断报告。确定性采集和规则判断不依赖模型，因此模型不可用不会影响基础诊断能力。

## 基础配置

CKMAN 使用 OpenAI-compatible Chat Completions 接口，可连接硅基流动等兼容服务：

```hjson
{
  "llm": {
    "enabled": true,
    "base_url": "https://api.siliconflow.cn/v1",
    "model": "deepseek-ai/DeepSeek-V4.1-Flash",
    "api_key_env": "CKMAN_LLM_API_KEY",
    "api_key_file": "",
    "timeout_seconds": 120,
    "max_turns": 4,
    "max_context_bytes": 32768,
    "max_output_tokens": 2048,
    "send_raw_exceptions": false
  }
}
```

| 配置项 | 说明 |
| --- | --- |
| `enabled` | 是否启用 AI 解释；关闭后仍保留结构化诊断 |
| `base_url` | OpenAI-compatible API 根地址，通常包含 `/v1` |
| `model` | 服务商公布的完整模型 ID |
| `api_key_env` | 读取 API key 的环境变量名，默认 `CKMAN_LLM_API_KEY` |
| `api_key_file` | 可选的 secret 文件绝对路径；环境变量为空时读取 |
| `timeout_seconds` | 单次模型请求超时，默认 120 秒 |
| `max_context_bytes` | 发送给模型的诊断上下文上限，默认 32 KiB |
| `max_output_tokens` | 单次结论最大输出 token 数，默认 2048 |
| `send_raw_exceptions` | 是否发送原始异常文本；建议保持 `false` |

`max_turns` 为编排兼容字段。当前产品交互固定为一次诊断、一轮解释，不开放多轮追问。

## 配置 API key

推荐把 key 写在主配置同目录的 `.ckman.env`，不要写进 `ckman.hjson`：

```bash
umask 077
printf '%s\n' 'CKMAN_LLM_API_KEY=替换为实际密钥' >> /etc/ckman/conf/.ckman.env
chown ckman:ckman /etc/ckman/conf/.ckman.env
chmod 600 /etc/ckman/conf/.ckman.env
systemctl restart ckman
```

如果同名变量同时存在于进程环境和 `.ckman.env`，进程环境优先。也可以使用由 secret manager 挂载的文件：

```hjson
"api_key_env": "",
"api_key_file": "/run/secrets/ckman_llm_api_key"
```

服务进程必须拥有该文件的读取权限。

::: danger 不要提交密钥
`.ckman.env`、secret 文件和真实 API key 不应提交到 Git、写入截图或粘贴到问题单。修改 key 或 LLM 配置后需要重启 CKMAN。
:::

## 网络与反向代理

CKMAN 服务端直接访问 `base_url`，浏览器不会接触模型密钥。AI 解释通过 CKMAN 的 SSE 接口流式返回；如果外层代理强制缓冲，需要对 `/api/v1/diagnostics/` 关闭响应缓冲并延长读取超时。

## 验证

重启后打开任一带“AI 诊断”的页面：

1. 发起诊断并等待结构化检查完成。
2. 页面出现“AI 正在生成一次性结论”。
3. 结论以 Markdown 流式展示，完成后保留在当前诊断抽屉。

如果只有结构化结果而没有 AI 解释，依次检查 `enabled`、`base_url`、模型 ID、API key 可读权限以及 CKMAN 到模型 endpoint 的网络连通性。
