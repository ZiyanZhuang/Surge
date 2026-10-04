/**
 * DSH Host `llm.stream` → Anthropic Messages SSE bridge (loopback only).
 *
 * 设计论证
 * --------
 * 目标是让 Python 调度器"直连" DSH Host 的 `llm.stream` business Service。可选路径里，
 * 让本插件把 `StreamChunk` 映射成 **Anthropic Messages SSE**，Python 侧就能继续使用
 * 已经过 20 项单测与真实反代验证的 `HttpWorkerAdapter`，无需第二套传输与 fail-closed 逻辑。
 * 换一种自定义线协议会复制那套逻辑，并让两侧的校验规则各自漂移。
 *
 * 不变量
 * ------
 * I1 只监听 127.0.0.1，并且只接受来自回环地址的连接；
 * I2 每次请求必须携带 `x-api-key`（或 `Authorization: Bearer`）且等于 token 文件内容；
 * I3 上游没有报告 usage 时不伪造 usage：`message_delta` 省略 usage，客户端据此 fail-closed；
 * I4 上游以 `error`/`aborted` 结束时发出 SSE `error` 事件并且**不发** `message_stop`，
 *    因此客户端不会把它当成完整回答；
 * I5 并发超过上限时返回 HTTP 429，不排队、不静默丢弃；
 * I6 日志不写 prompt、不写 token。
 *
 * 已确认的 DSH 契约（来自 @deepseek-ai/dsh-llm 的 README 与 typert 声明）
 * ------------------------------------------------------------------
 * - `ctx.llm.stream({provider, model, messages, system?, maxTokens?, temperature?, signal?})`
 *   返回 `AsyncIterable<StreamChunk>`；
 * - `messages` 接受 request-only 形式 `{role:'user', content:[{type:'text', text}]}`；
 * - chunk 类型：block-start / text-delta / reasoning-delta / tool-call-delta / block-end /
 *   usage / finish；`usage` 字段是 inputTokens/outputTokens；
 * - 每个流恰好以一个 `finish` 结束，失败时 `{kind:'error', failure}`。
 *
 * 配置：优先取插件 `config`（loader 直通，无 Config schema），其次环境变量。
 *   provider       必填（config.provider 或 DSH_LLM_BRIDGE_PROVIDER）
 *   tokenFile      必填（config.tokenFile 或 DSH_LLM_BRIDGE_TOKEN_FILE）
 *   port           默认 17801（DSH_LLM_BRIDGE_PORT）
 *   maxConcurrent  默认 4（DSH_LLM_BRIDGE_MAX_CONCURRENT）
 */

import { createServer } from 'node:http';
import { existsSync, readFileSync } from 'node:fs';

export const name = 'dsh-llm-bridge';
export const inject = ['llm'];

const DEFAULT_PORT = 17801;
const DEFAULT_MAX_CONCURRENT = 4;
const MAX_BODY_BYTES = 1_000_000;
const LOOPBACK = new Set(['127.0.0.1', '::1', '::ffff:127.0.0.1']);

const frame = (event, data) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;

function positiveInt(value, fallback) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : fallback;
}

function stringOr(...candidates) {
  for (const candidate of candidates) {
    if (typeof candidate === 'string' && candidate.trim() !== '') return candidate.trim();
  }
  return '';
}

/** Anthropic error body → SSE error event; never includes the token. */
function upstreamFailureText(reason) {
  const failure = reason && typeof reason === 'object' ? reason.failure : undefined;
  const code = failure && typeof failure.code === 'string' ? failure.code : 'UPSTREAM_ERROR';
  const message = failure && typeof failure.message === 'string' ? failure.message : 'upstream stream failed';
  return `${code}: ${message}`;
}

function stopReason(reason) {
  const kind = reason && typeof reason === 'object' ? reason.kind : undefined;
  if (kind === 'max-tokens') return 'max_tokens';
  if (kind === 'tool-calls') return 'tool_use';
  if (kind === 'stop') return 'end_turn';
  return 'end_turn';
}

/** Translate one Anthropic Messages request into GenerateOptions; fail closed on unsupported shapes. */
export function buildGenerateOptions(body, { provider, signal }) {
  if (!body || typeof body !== 'object') throw new Error('request body must be a JSON object');
  const model = body.model;
  if (typeof model !== 'string' || model.trim() === '') throw new Error('model is required');
  const messages = body.messages;
  if (!Array.isArray(messages) || messages.length === 0) throw new Error('messages must be a non-empty array');
  const mapped = [];
  for (const [index, message] of messages.entries()) {
    if (!message || typeof message !== 'object') throw new Error(`messages[${index}] must be an object`);
    const role = message.role;
    if (role !== 'user' && role !== 'assistant') {
      throw new Error(`messages[${index}].role must be "user" or "assistant"`);
    }
    const content = Array.isArray(message.content) ? message.content : null;
    if (!content) throw new Error(`messages[${index}].content must be an array of blocks`);
    const blocks = [];
    for (const block of content) {
      if (!block || typeof block !== 'object' || block.type !== 'text') {
        // 只支持纯文本：图片与工具调用需要额外映射语义，静默降级会改变请求含义。
        throw new Error(`messages[${index}] contains an unsupported content block; text only`);
      }
      if (typeof block.text !== 'string') throw new Error(`messages[${index}] text block requires a string`);
      blocks.push({ type: 'text', text: block.text });
    }
    mapped.push({ role, content: blocks });
  }
  const options = { provider, model, messages: mapped, signal };
  if (typeof body.system === 'string' && body.system !== '') options.system = body.system;
  const maxTokens = positiveInt(body.max_tokens, 0);
  if (maxTokens > 0) options.maxTokens = maxTokens;
  if (typeof body.temperature === 'number' && Number.isFinite(body.temperature)) {
    options.temperature = body.temperature;
  }
  return options;
}

/** Encode an async iterable of StreamChunks as Anthropic Messages SSE frames. */
export async function* streamToAnthropicSse(chunks, { model }) {
  yield frame('message_start', {
    type: 'message_start',
    message: {
      id: 'dsh-llm-bridge',
      type: 'message',
      role: 'assistant',
      model,
      content: [],
      usage: { input_tokens: 0, output_tokens: 0 },
    },
  });
  let index = 0;
  let open = null;
  let openType = null;
  let usage = null;
  let finish = null;
  const closeOpen = function* () {
    if (open !== null) {
      yield frame('content_block_stop', { type: 'content_block_stop', index: open });
      open = null;
      openType = null;
    }
  };
  for await (const chunk of chunks) {
    if (!chunk || typeof chunk !== 'object') continue;
    if (chunk.type === 'block-start') continue; // 块在首个 delta 时才真正打开，保持索引连续
    if (chunk.type === 'text-delta' || chunk.type === 'reasoning-delta') {
      const text = typeof chunk.text === 'string' ? chunk.text : '';
      if (text === '') continue;
      const wanted = chunk.type === 'text-delta' ? 'text' : 'thinking';
      if (open !== null && openType !== wanted) {
        yield* closeOpen();
      }
      if (open === null) {
        open = index++;
        openType = wanted;
        yield frame('content_block_start', {
          type: 'content_block_start',
          index: open,
          content_block: wanted === 'text' ? { type: 'text', text: '' } : { type: 'thinking', thinking: '' },
        });
      }
      yield frame('content_block_delta', {
        type: 'content_block_delta',
        index: open,
        delta:
          wanted === 'text'
            ? { type: 'text_delta', text }
            : { type: 'thinking_delta', thinking: text },
      });
      continue;
    }
    if (chunk.type === 'tool-call-delta') {
      if (open === null) {
        open = index++;
        yield frame('content_block_start', {
          type: 'content_block_start',
          index: open,
          content_block: { type: 'tool_use', id: String(chunk.id ?? 'tool'), name: String(chunk.name ?? ''), input: {} },
        });
      }
      yield frame('content_block_delta', {
        type: 'content_block_delta',
        index: open,
        delta: { type: 'input_json_delta', partial_json: String(chunk.argumentsDelta ?? '') },
      });
      continue;
    }
    if (chunk.type === 'usage') {
      const input = chunk.usage && Number(chunk.usage.inputTokens);
      const output = chunk.usage && Number(chunk.usage.outputTokens);
      if (Number.isFinite(input) && Number.isFinite(output)) {
        usage = { input_tokens: input, output_tokens: output };
      }
      continue;
    }
    if (chunk.type === 'finish') {
      finish = chunk.reason ?? null;
    }
  }
  // I4：错误/中止不发 message_stop，客户端会把"响应不完整"当成失败。
  const kind = finish && typeof finish === 'object' ? finish.kind : undefined;
  if (kind === 'error' || kind === 'aborted') {
    yield frame('error', { type: 'error', error: { type: kind, message: upstreamFailureText(finish) } });
    return;
  }
  yield* closeOpen();
  const delta = { stop_reason: stopReason(finish) };
  const payload = { type: 'message_delta', delta };
  // I3：没有上游 usage 就不写 usage 字段。
  if (usage) payload.usage = usage;
  yield frame('message_delta', payload);
  yield frame('message_stop', { type: 'message_stop' });
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const parts = [];
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(new Error(`request body exceeds ${MAX_BODY_BYTES} bytes`));
        req.destroy();
        return;
      }
      parts.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(parts).toString('utf8')));
    req.on('error', reject);
  });
}

function sendJson(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) });
  res.end(body);
}

export function apply(ctx, config = {}) {
  const provider = stringOr(config.provider, process.env.DSH_LLM_BRIDGE_PROVIDER);
  const tokenFile = stringOr(config.tokenFile, process.env.DSH_LLM_BRIDGE_TOKEN_FILE);
  const port = positiveInt(config.port ?? process.env.DSH_LLM_BRIDGE_PORT, DEFAULT_PORT);
  const maxConcurrent = positiveInt(
    config.maxConcurrent ?? process.env.DSH_LLM_BRIDGE_MAX_CONCURRENT,
    DEFAULT_MAX_CONCURRENT,
  );
  if (!provider) {
    throw new Error('dsh-llm-bridge: provider is required (config.provider or DSH_LLM_BRIDGE_PROVIDER)');
  }
  if (!tokenFile) {
    throw new Error('dsh-llm-bridge: tokenFile is required (config.tokenFile or DSH_LLM_BRIDGE_TOKEN_FILE)');
  }
  if (!existsSync(tokenFile)) {
    throw new Error(`dsh-llm-bridge: token file does not exist: ${tokenFile}`);
  }
  const token = readFileSync(tokenFile, 'utf8').trim();
  if (token === '') {
    throw new Error('dsh-llm-bridge: token file is empty');
  }

  let active = 0;
  let closing = false;

  const authorized = (req) => {
    const header = req.headers['x-api-key'];
    if (typeof header === 'string' && header === token) return true;
    const auth = req.headers.authorization;
    return typeof auth === 'string' && auth.startsWith('Bearer ') && auth.slice(7).trim() === token;
  };

  const handle = async (req, res) => {
    // I1：只接受回环来源。
    if (!LOOPBACK.has(req.socket.remoteAddress ?? '')) {
      sendJson(res, 403, { error: 'loopback clients only' });
      return;
    }
    if (req.method !== 'POST' || req.url !== '/v1/messages') {
      sendJson(res, 404, { error: 'POST /v1/messages only' });
      return;
    }
    // I2
    if (!authorized(req)) {
      sendJson(res, 401, { error: 'invalid api key' });
      return;
    }
    if (closing) {
      sendJson(res, 503, { error: 'bridge is shutting down' });
      return;
    }
    // I5
    if (active >= maxConcurrent) {
      sendJson(res, 429, { error: `bridge concurrency limit reached (${maxConcurrent})` });
      return;
    }
    let bodyText;
    try {
      bodyText = await readBody(req);
    } catch (error) {
      sendJson(res, 413, { error: String(error && error.message) });
      return;
    }
    let body;
    try {
      body = JSON.parse(bodyText);
    } catch {
      sendJson(res, 400, { error: 'request body must be JSON' });
      return;
    }
    const controller = new AbortController();
    let options;
    try {
      options = buildGenerateOptions(body, { provider, signal: controller.signal });
    } catch (error) {
      sendJson(res, 400, { error: String(error && error.message) });
      return;
    }

    active += 1;
    const release = () => {
      active -= 1;
    };
    let aborted = false;
    // 以响应流的 close 判断客户端断开：请求流的 close 在请求体读完后就会触发，
    // 用它会把每个正常请求都误判为取消。
    const onClose = () => {
      if (!res.writableEnded) {
        aborted = true;
        controller.abort();
      }
    };
    res.on('close', onClose);

    res.writeHead(200, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-store',
      connection: 'keep-alive',
    });
    try {
      for await (const chunk of streamToAnthropicSse(ctx.llm.stream(options), { model: options.model })) {
        if (aborted || res.writableEnded) break;
        res.write(chunk);
      }
    } catch (error) {
      if (!res.writableEnded) {
        res.write(frame('error', { type: 'error', error: { type: 'bridge_error', message: String(error && error.message) } }));
      }
    } finally {
      res.off('close', onClose);
      release();
      if (!res.writableEnded) res.end();
    }
  };

  const server = createServer((req, res) => {
    handle(req, res).catch((error) => {
      if (!res.writableEnded) sendJson(res, 500, { error: String(error && error.message) });
    });
  });
  server.on('clientError', (_error, socket) => socket.destroy());
  server.listen(port, '127.0.0.1');

  ctx.effect(
    () => async () => {
      closing = true;
      await new Promise((resolve) => server.close(resolve));
    },
    'dsh-llm-bridge: stop loopback server',
  );

  return { server, port, provider, maxConcurrent };
}
