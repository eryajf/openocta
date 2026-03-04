/**
 * Built-in model providers from src/pkg/agent/model_factory.go and model-providers.md.
 * Used to list supported providers and their default models.
 */

export type BuiltInProvider = {
  id: string;
  label: string;
  envKey: string;
  defaultModel: string;
  baseUrl: string;
};

export const BUILTIN_PROVIDERS: BuiltInProvider[] = [
  { id: "anthropic", label: "Anthropic", envKey: "ANTHROPIC_API_KEY", defaultModel: "claude-sonnet-4-5-20250929", baseUrl: "(官方)" },
  { id: "openai", label: "OpenAI", envKey: "OPENAI_API_KEY", defaultModel: "gpt-4", baseUrl: "(官方)" },
  { id: "openrouter", label: "OpenRouter", envKey: "OPENROUTER_API_KEY", defaultModel: "auto", baseUrl: "https://openrouter.ai/api/v1" },
  { id: "litellm", label: "LiteLLM", envKey: "LITELLM_API_KEY", defaultModel: "", baseUrl: "http://localhost:4000" },
  { id: "moonshot", label: "Moonshot", envKey: "MOONSHOT_API_KEY", defaultModel: "kimi-k2.5", baseUrl: "https://api.moonshot.ai/v1" },
  { id: "moonshot-cn", label: "Moonshot-CN", envKey: "MOONSHOT_API_KEY", defaultModel: "kimi-k2.5", baseUrl: "https://api.moonshot.cn/v1" },
  { id: "kimi-coding", label: "Kimi Coding", envKey: "KIMI_API_KEY", defaultModel: "k2p5", baseUrl: "https://api.moonshot.ai/anthropic" },
  { id: "opencode", label: "OpenCode", envKey: "OPENCODE_API_KEY", defaultModel: "claude-opus-4-6", baseUrl: "https://opencode.ai/zen/v1" },
  { id: "zai", label: "Z.ai (智谱)", envKey: "ZAI_API_KEY", defaultModel: "glm-5", baseUrl: "https://api.z.ai/api/paas/v4" },
  { id: "xai", label: "xAI (Grok)", envKey: "XAI_API_KEY", defaultModel: "grok-3-mini", baseUrl: "https://api.x.ai/v1" },
  { id: "together", label: "Together AI", envKey: "TOGETHER_API_KEY", defaultModel: "meta-llama/Llama-3.3-70B-Instruct-Turbo", baseUrl: "https://api.together.xyz/v1" },
  { id: "venice", label: "Venice AI", envKey: "VENICE_API_KEY", defaultModel: "falcon-3.1-70b", baseUrl: "https://api.venice.ai/api/v1" },
  { id: "synthetic", label: "Synthetic", envKey: "SYNTHETIC_API_KEY", defaultModel: "hf:MiniMaxAI/MiniMax-M2.1", baseUrl: "https://api.synthetic.new/anthropic" },
  { id: "qianfan", label: "千帆 (百度)", envKey: "QIANFAN_API_KEY", defaultModel: "deepseek-v3-2-251201", baseUrl: "https://qianfan.baidubce.com/v2" },
  { id: "huggingface", label: "Hugging Face", envKey: "HUGGINGFACE_HUB_TOKEN", defaultModel: "", baseUrl: "https://router.huggingface.co/v1" },
  { id: "xiaomi", label: "小米 Mimo", envKey: "XIAOMI_API_KEY", defaultModel: "mimo-v2-flash", baseUrl: "https://api.xiaomimimo.com/anthropic" },
  { id: "minimax", label: "MiniMax", envKey: "MINIMAX_API_KEY", defaultModel: "MiniMax-M2.1", baseUrl: "https://api.minimax.io/anthropic" },
  { id: "mistral", label: "Mistral", envKey: "MISTRAL_API_KEY", defaultModel: "mistral-large-latest", baseUrl: "https://api.mistral.ai/v1" },
  { id: "groq", label: "Groq", envKey: "GROQ_API_KEY", defaultModel: "llama-3.3-70b-versatile", baseUrl: "https://api.groq.com/openai/v1" },
  { id: "cerebras", label: "Cerebras", envKey: "CEREBRAS_API_KEY", defaultModel: "llama-4-scout-17b-16e-instruct", baseUrl: "https://api.cerebras.ai/v1" },
  { id: "ollama", label: "Ollama", envKey: "OLLAMA_API_KEY", defaultModel: "llama3.3", baseUrl: "http://127.0.0.1:11434/v1" },
  { id: "vllm", label: "vLLM", envKey: "VLLM_API_KEY", defaultModel: "", baseUrl: "http://127.0.0.1:8000/v1" },
  { id: "vercel-ai-gateway", label: "Vercel AI Gateway", envKey: "AI_GATEWAY_API_KEY", defaultModel: "", baseUrl: "https://api.vercel.ai/v1" },
  { id: "bailian", label: "百炼 (阿里云)", envKey: "DASHSCOPE_API_KEY", defaultModel: "qwen3.5-flash", baseUrl: "https://dashscope.aliyuncs.com/compatible-mode/v1" },
];

export function parseModelRef(ref: string | null | undefined): { provider: string; modelId: string } | null {
  if (!ref || typeof ref !== "string") return null;
  const parts = ref.trim().split("/", 2);
  if (parts.length === 2) return { provider: parts[0].trim(), modelId: parts[1].trim() };
  return { provider: "anthropic", modelId: ref.trim() };
}

export function formatModelRef(provider: string, modelId: string): string {
  return `${provider}/${modelId}`;
}
