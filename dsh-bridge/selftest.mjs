#!/usr/bin/env node
/**
 * Self-test for the DSH `llm.stream` → Anthropic SSE bridge.
 *
 * 用 **stub ctx** 直接调用插件的 `apply`，因此不需要真实 DSH Host、不需要重启、
 * 也不消耗任何模型额度。它验证的是协议映射、鉴权、并发上限、取消传播与
 * fail-closed 行为；唯一无法在此验证的是 `ctx.llm` 由真实 Host 注入这一步。
 *
 * 同时把正常路径的 SSE 响应写成 dsh-bridge/golden-sse.txt，供 Python 侧
 * tests/test_bridge_contract.py 回放，从而把两端钉在同一个线格式上。
 *
 * 用法：node dsh-bridge/selftest.mjs [--write-golden]
 */

import { createServer } from 'node:http';
import { mkdtempSync, writeFileSync, readFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { apply, buildGenerateOptions, streamToAnthropicSse } from './dsh-llm-bridge.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const GOLDEN = join(HERE, 'golden-sse.txt');
const WRITE_GOLDEN = process.argv.includes('--write-golden');

let failures = 0;
let checks = 0;

function check(label, condition, detail = '') {
  checks += 1;
  if (condition) {
    console.log(`  ok   ${label}`);
  } else {
    failures += 1;
    console.log(`  FAIL ${label}${detail ? ` — ${detail}` : ''}`);
  }
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function freePort() {
  return new Promise((resolve) => {
    const probe = createServer();
    probe.listen(0, '127.0.0.1', () => {
      const { port } = probe.address();
      probe.close(() => resolve(port));
    });
  });
}

/** Minimal Cordis-like context: only `llm` and `effect` are used by the plugin. */
function makeStubContext(streamFactory) {
  const disposers = [];
  return {
    disposers,
    ctx: {
      llm: { stream: (options) => streamFactory(options) },
      effect: (factory) => {
        disposers.push(factory());
      },
    },
  };
}

function textStream({ usage = { inputTokens: 12, outputTokens: 7 }, finish = { kind: 'stop' } } = {}) {
  return async function* () {
    yield { type: 'block-start', index: 0, blockType: 'text' };
    yield { type: 'text-delta', index: 0, text: 'Hel' };
    yield { type: 'text-delta', index: 0, text: 'lo' };
    yield { type: 'block-end', index: 0, block: { type: 'text', text: 'Hello' } };
    if (usage) yield { type: 'usage', usage };
    yield { type: 'finish', reason: finish };
  };
}

async function startBridge({ streamFactory, maxConcurrent = 4, port, token, provider = 'stub-provider' }) {
  const { ctx, disposers } = makeStubContext(streamFactory);
  const handle = apply(ctx, { provider, tokenFile: token.file, port, maxConcurrent });
  await sleep(60);
  return handle;
}

async function post(url, token, body, { signal } = {}) {
  return fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-api-key': token },
    body: JSON.stringify(body),
    signal,
  });
}

const REQUEST = {
  model: 'stub-model',
  max_tokens: 2000,
  system: 'be terse',
  messages: [{ role: 'user', content: [{ type: 'text', text: 'say hello' }] }],
};

function eventNames(text) {
  return [...text.matchAll(/^event: (.+)$/gm)].map((match) => match[1]);
}

function dataFrames(text) {
  return [...text.matchAll(/^data: (.+)$/gm)].map((match) => JSON.parse(match[1]));
}

async function main() {
  const scratch = mkdtempSync(join(tmpdir(), 'dsh-bridge-selftest-'));
  const tokenPath = join(scratch, 'token.txt');
  const token = 'selftest-token-value';
  writeFileSync(tokenPath, `${token}\n`, 'utf8');
  const tokenFile = { file: tokenPath };

  // ---- configuration is fail-closed -------------------------------------
  console.log('config');
  const { ctx: bareCtx } = makeStubContext(() => textStream()());
  try {
    apply(bareCtx, {});
    check('missing provider is rejected', false);
  } catch (error) {
    check('missing provider is rejected', /provider is required/.test(String(error.message)));
  }
  try {
    apply(bareCtx, { provider: 'p' });
    check('missing tokenFile is rejected', false);
  } catch (error) {
    check('missing tokenFile is rejected', /tokenFile is required/.test(String(error.message)));
  }
  try {
    apply(bareCtx, { provider: 'p', tokenFile: join(scratch, 'absent.txt') });
    check('absent token file is rejected', false);
  } catch (error) {
    check('absent token file is rejected', /does not exist/.test(String(error.message)));
  }

  // ---- happy path + golden ---------------------------------------------
  console.log('happy path');
  const port = await freePort();
  const url = `http://127.0.0.1:${port}/v1/messages`;
  const upstreamCalls = [];
  const bridge = await startBridge({
    streamFactory: (options) => {
      upstreamCalls.push(options);
      return textStream()();
    },
    port,
    token: tokenFile,
  });

  const ok = await post(url, token, REQUEST);
  const okText = await ok.text();
  check('status 200', ok.status === 200, `got ${ok.status}`);
  check(
    'content-type is SSE',
    String(ok.headers.get('content-type')).includes('text/event-stream'),
    String(ok.headers.get('content-type')),
  );
  const names = eventNames(okText);
  const required = ['message_start', 'content_block_delta', 'message_delta', 'message_stop'];
  const positions = required.map((item) => names.indexOf(item));
  check('all required events present', positions.every((index) => index >= 0), names.join(','));
  check('required events are in order', positions.every((value, index) => index === 0 || value > positions[index - 1]));
  const deltas = dataFrames(okText).filter((item) => item.type === 'content_block_delta');
  check('text deltas are forwarded', deltas.map((item) => item.delta.text).join('') === 'Hello');
  const messageDelta = dataFrames(okText).find((item) => item.type === 'message_delta');
  check('usage is mapped to snake_case', JSON.stringify(messageDelta.usage) === '{"input_tokens":12,"output_tokens":7}');
  check('stop_reason is end_turn', messageDelta.delta.stop_reason === 'end_turn');
  check('upstream received the provider', upstreamCalls[0]?.provider === 'stub-provider');
  check('upstream received system and maxTokens', upstreamCalls[0]?.system === 'be terse' && upstreamCalls[0]?.maxTokens === 2000);
  check('upstream messages are request-only shape', JSON.stringify(upstreamCalls[0]?.messages) === JSON.stringify(REQUEST.messages));

  if (WRITE_GOLDEN) {
    writeFileSync(GOLDEN, okText, 'utf8');
    console.log(`  --   golden written: ${GOLDEN}`);
  } else if (existsSync(GOLDEN)) {
    const committed = readFileSync(GOLDEN, 'utf8');
    check('golden-sse.txt matches live output', committed === okText, 'run with --write-golden to refresh intentionally');
  } else {
    check('golden-sse.txt exists', false, 'run with --write-golden');
  }

  // ---- authentication ---------------------------------------------------
  console.log('auth');
  const noToken = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(REQUEST) });
  check('missing token → 401', noToken.status === 401, `got ${noToken.status}`);
  const wrongToken = await post(url, 'wrong', REQUEST);
  check('wrong token → 401', wrongToken.status === 401, `got ${wrongToken.status}`);
  const bearer = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify(REQUEST),
  });
  check('bearer token is accepted', bearer.status === 200, `got ${bearer.status}`);
  await bearer.text();
  check('unauthorized calls never reached the model', upstreamCalls.length === 2, `calls=${upstreamCalls.length}`);

  // ---- request validation ----------------------------------------------
  console.log('request validation');
  const wrongPath = await fetch(`http://127.0.0.1:${port}/v1/other`, { method: 'POST', headers: { 'x-api-key': token } });
  check('unknown path → 404', wrongPath.status === 404, `got ${wrongPath.status}`);
  const imageBlock = await post(url, token, {
    model: 'stub-model',
    messages: [{ role: 'user', content: [{ type: 'image', source: {} }] }],
  });
  check('non-text content → 400', imageBlock.status === 400, `got ${imageBlock.status}`);
  check('rejection explains the limitation', /text only/.test(await imageBlock.text()));
  const noModel = await post(url, token, { messages: REQUEST.messages });
  check('missing model → 400', noModel.status === 400, `got ${noModel.status}`);

  // ---- reasoning, max-tokens, error, missing usage ----------------------
  console.log('chunk mapping');
  const chunks = [
    { type: 'block-start', index: 0, blockType: 'thinking' },
    { type: 'reasoning-delta', index: 0, text: 'considering' },
    { type: 'block-end', index: 0, block: { type: 'thinking', thinking: 'considering' } },
    { type: 'block-start', index: 1, blockType: 'text' },
    { type: 'text-delta', index: 1, text: 'hi' },
    { type: 'usage', usage: { inputTokens: 3, outputTokens: 2 } },
    { type: 'finish', reason: { kind: 'stop' } },
  ];
  const reasoningText = await (
    await post(url, token, REQUEST)
  ).text();
  // 上面的 happy-path stub 仍在使用；这里用独立 bridge 覆盖其他映射。
  await dispose(bridge);
  const mappingPort = await freePort();
  const mappingUrl = `http://127.0.0.1:${mappingPort}/v1/messages`;
  const mappingBridge = await startBridge({
    streamFactory: (options) => {
      if (options.model === 'reasoning-model') {
        return (async function* () {
          for (const chunk of chunks) yield chunk;
        })();
      }
      if (options.model === 'no-usage-model') {
        return textStream({ usage: null })();
      }
      if (options.model === 'max-tokens-model') {
        return textStream({ finish: { kind: 'max-tokens' } })();
      }
      if (options.model === 'error-model') {
        return (async function* () {
          yield { type: 'text-delta', index: 0, text: 'partial' };
          yield { type: 'finish', reason: { kind: 'error', failure: { code: 'RATE_LIMIT', message: 'slow down' } } };
        })();
      }
      return textStream()();
    },
    port: mappingPort,
    token: tokenFile,
  });

  const reasoning = await (await post(mappingUrl, token, { ...REQUEST, model: 'reasoning-model' })).text();
  const reasoningFrames = dataFrames(reasoning);
  const thinkingDelta = reasoningFrames.find((item) => item.delta?.type === 'thinking_delta');
  check('reasoning delta becomes a thinking block', thinkingDelta?.delta.thinking === 'considering');
  check('text block index advances after thinking', reasoningFrames.find((item) => item.delta?.type === 'text_delta')?.index === 1);
  check(
    'reasoning stream still ends with message_stop',
    eventNames(reasoning).includes('message_stop'),
    eventNames(reasoning).join(','),
  );

  const maxTokens = await (await post(mappingUrl, token, { ...REQUEST, model: 'max-tokens-model' })).text();
  check(
    'max-tokens finish maps to stop_reason max_tokens',
    dataFrames(maxTokens).find((item) => item.type === 'message_delta')?.delta.stop_reason === 'max_tokens',
  );

  const errored = await (await post(mappingUrl, token, { ...REQUEST, model: 'error-model' })).text();
  const errorNames = eventNames(errored);
  check('error finish emits an error event', errorNames.includes('error'), errorNames.join(','));
  check('error finish never emits message_stop', !errorNames.includes('message_stop'));
  check('error event carries the upstream code', /RATE_LIMIT/.test(errored));

  const noUsage = await (await post(mappingUrl, token, { ...REQUEST, model: 'no-usage-model' })).text();
  const noUsageDelta = dataFrames(noUsage).find((item) => item.type === 'message_delta');
  check('missing upstream usage is not fabricated', noUsageDelta.usage === undefined, JSON.stringify(noUsageDelta));
  check('missing usage still ends with message_stop', eventNames(noUsage).includes('message_stop'));

  // ---- cancellation -----------------------------------------------------
  console.log('cancellation');
  const abortProbe = { aborted: false };
  const abortPort = await freePort();
  const abortBridge = await startBridge({
    streamFactory: (options) =>
      (async function* () {
        yield { type: 'text-delta', index: 0, text: 'partial' };
        for (let index = 0; index < 200; index += 1) {
          if (options.signal?.aborted) {
            abortProbe.aborted = true;
            return;
          }
          await sleep(10);
        }
        yield { type: 'usage', usage: { inputTokens: 1, outputTokens: 1 } };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })(),
    port: abortPort,
    token: tokenFile,
  });
  const controller = new AbortController();
  const abortResponse = await post(`http://127.0.0.1:${abortPort}/v1/messages`, token, REQUEST, {
    signal: controller.signal,
  });
  const reader = abortResponse.body.getReader();
  await reader.read();
  controller.abort();
  await sleep(400);
  check('client abort propagates to the upstream signal', abortProbe.aborted);
  await dispose(abortBridge);

  // ---- concurrency bound ------------------------------------------------
  console.log('concurrency');
  const slowPort = await freePort();
  const slowBridge = await startBridge({
    streamFactory: () =>
      (async function* () {
        yield { type: 'text-delta', index: 0, text: 'slow' };
        await sleep(400);
        yield { type: 'usage', usage: { inputTokens: 1, outputTokens: 1 } };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })(),
    port: slowPort,
    maxConcurrent: 1,
    token: tokenFile,
  });
  const slowUrl = `http://127.0.0.1:${slowPort}/v1/messages`;
  const first = await post(slowUrl, token, REQUEST);
  const second = await post(slowUrl, token, REQUEST);
  check('second concurrent request → 429', second.status === 429, `got ${second.status}`);
  check('429 explains the limit', /concurrency limit/.test(await second.text()));
  await first.text();
  await dispose(slowBridge);

  // ---- decision helper is exported for reuse ---------------------------
  console.log('options builder');
  let threw = false;
  try {
    buildGenerateOptions({ model: 'm', messages: [{ role: 'system', content: [] }] }, { provider: 'p', signal: undefined });
  } catch {
    threw = true;
  }
  check('unsupported role is rejected', threw);

  await dispose(mappingBridge);
  rmSync(scratch, { recursive: true, force: true });
  void reasoningText;

  console.log(`\n${checks - failures}/${checks} checks passed`);
  if (failures > 0) process.exitCode = 1;
}

async function dispose(handle) {
  if (!handle) return;
  await new Promise((resolve) => handle.server.close(resolve));
}

main().catch((error) => {
  console.error(`selftest crashed: ${error && error.stack ? error.stack : error}`);
  process.exitCode = 1;
});
