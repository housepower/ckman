package nova

import (
	"context"
	"errors"
	"strings"

	"github.com/AutumnsGrove/nova/provider"
	"github.com/housepower/ckman/config"
)

const (
	StateDisabled      = "disabled"
	StateMisconfigured = "misconfigured"
	StateConfigured    = "configured"
)

type Status struct {
	State   string `json:"state"`
	Message string `json:"message"`
}

// ConfigStatus validates only local configuration and credentials. Remote
// endpoint, model, quota, and key validity are verified by the actual request.
func ConfigStatus(cfg config.LLMConfig) Status {
	if !cfg.Enabled {
		return Status{State: StateDisabled, Message: "AI 解释未启用，当前仅展示确定性诊断结果"}
	}
	if _, err := New(cfg); err != nil {
		return Status{State: StateMisconfigured, Message: FriendlyError(err)}
	}
	return Status{State: StateConfigured, Message: "AI 解释已配置，将在诊断完成后验证模型连接"}
}

// FriendlyError converts provider and configuration failures into stable,
// actionable user-facing messages. Detailed errors remain available to logs.
func FriendlyError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "AI 服务响应超时，请检查 endpoint 网络连通性或增大 timeout_seconds"
	}

	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "llm disabled"):
		return "AI 解释未启用，当前仅展示确定性诊断结果"
	case strings.Contains(lower, "base_url and model are required"):
		return "AI 配置不完整，请检查 llm.base_url 和 llm.model"
	case strings.Contains(lower, "api key is not configured"):
		return "AI 凭据未配置，请设置 API key 环境变量或 api_key_file"
	case strings.Contains(lower, "permission denied"):
		return "AI 凭据文件不可读，请检查 api_key_file 路径和权限"
	}

	var providerErr *provider.Error
	if errors.As(err, &providerErr) {
		providerText := strings.ToLower(providerErr.Code + " " + providerErr.Message)
		switch {
		case (strings.Contains(providerText, "api_key") || strings.Contains(providerText, "authentication") || strings.Contains(providerText, "unauthorized")) && providerErr.Status < 500:
			return "AI 服务鉴权失败，请检查 API key 是否填写错误或已经过期"
		case strings.Contains(providerText, "quota") || strings.Contains(providerText, "balance") || strings.Contains(providerText, "credit"):
			return "AI 服务账户余额或可用额度不足，请前往模型服务商检查账户"
		case providerErr.Status == 401:
			return "AI 服务鉴权失败，请检查 API key 是否填写错误或已经过期"
		case providerErr.Status == 402:
			return "AI 服务账户余额或可用额度不足，请前往模型服务商检查账户"
		case providerErr.Status == 403:
			return "AI 服务拒绝访问，请检查 API key 权限、账户状态和模型访问权限"
		case providerErr.Status == 404:
			return "AI 服务接口或模型不存在，请检查 base_url 和 model"
		case providerErr.Status == 429:
			return "AI 服务请求受限，请稍后重试并检查调用频率或账户额度"
		case providerErr.Status == 400 && strings.Contains(providerText, "model"):
			return "AI 模型配置无效或当前账户无权使用，请检查 model"
		case providerErr.Status == 400:
			return "AI 服务拒绝了请求，请检查 model、endpoint 及服务兼容性"
		case providerErr.Status >= 500:
			return "AI 服务暂时不可用，请稍后重试"
		case strings.Contains(providerText, "connect timeout"):
			return "连接 AI 服务超时，请检查 endpoint 和网络连通性"
		case strings.Contains(providerText, "request failed"):
			return "无法连接 AI 服务，请检查 endpoint、DNS 和网络连通性"
		}
	}

	if strings.Contains(lower, "connect timeout") || strings.Contains(lower, "timeout") {
		return "连接 AI 服务超时，请检查 endpoint 和网络连通性"
	}
	if strings.Contains(lower, "build request") || strings.Contains(lower, "unsupported protocol scheme") || strings.Contains(lower, "invalid url") {
		return "AI endpoint 格式无效，请填写包含 http:// 或 https:// 的 OpenAI-compatible API 根地址"
	}
	if strings.Contains(lower, "request failed") || strings.Contains(lower, "dial tcp") || strings.Contains(lower, "no such host") {
		return "无法连接 AI 服务，请检查 endpoint、DNS 和网络连通性"
	}
	return "AI 服务调用失败，请检查 endpoint、model 和账户状态"
}
