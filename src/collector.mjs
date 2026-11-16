import WebSocket from 'ws';
import { EventEmitter } from 'node:events';
import { digest } from './store.mjs';

let runCounter = 0;

// 载荷指纹使用键序无关的稳定序列化，同一交付重投时内容可比。
function stable(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stable).join(',')}]`;
  return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${stable(value[key])}`).join(',')}}`;
}

// 分路收件器：在一条 WebSocket 上按 subscriptionToken 归属交付。
// 通道编号只描述当次绑定、可以随时被复用，不作为订阅身份；
// 交付先落入 Inbox（证据 + deliveries 记录）再发送 ack；
// 连接关闭即收尾，不留下后台重连，重连必须重新 subscribe。
export class Collector extends EventEmitter {
  constructor(source, store, options = {}) {
    super();
    this.source = source;
    this.store = store;
    this.deadline = options.deadline ?? 30000;
    this.subscriptions = new Map(); // token -> 订阅边界（bound/unbound 确认的生命周期）
    this.channels = new Map();      // channel -> 当前 token，仅随 bound/unbound 变化
    this.deliveries = new Map();    // token#deliveryId -> 记录，用于重投与冲突检测
    this.requested = new Map();     // requestId -> topics，本连接上发出过的订阅请求
    this.records = [];
    this.pending = [];
    this.diagnostics = [];
    this.seq = 0;
    this.ended = false;
    this.failed = false;
    this.socket = null;
    this._timer = null;
    this._finished = false;
  }

  async run({ subscriptions, deadline } = {}) {
    const requests = subscriptions ?? [{ requestId: this._requestId(), topics: this.source.topics }];
    const done = new Promise(resolve => { this._resolveDone = resolve; });
    const socket = new WebSocket(this.source.url);
    this.socket = socket;
    socket.on('message', raw => this._onMessage(raw));
    socket.on('close', () => this._finish());
    socket.on('error', error => this._diagnostic({ code: 'connection-error', detail: String(error && error.message || error) }));
    const limit = deadline ?? this.deadline;
    if (limit > 0) this._timer = setTimeout(() => this._abort('timeout'), limit);
    try {
      await new Promise((resolve, reject) => {
        const onOpen = () => { socket.off('error', onError); resolve(); };
        const onError = error => { socket.off('open', onOpen); reject(error); };
        socket.once('open', onOpen);
        socket.once('error', onError);
      });
    } catch (error) {
      this._clearTimer();
      throw error;
    }
    for (const request of requests) this.subscribe(request.requestId, request.topics);
    return done;
  }

  // 订阅、暂停、恢复都是消息层控制语义，按 token 生效，不影响连接本身。
  subscribe(requestId, topics = this.source.topics) {
    this.requested.set(requestId, topics ?? null);
    return this._send({ type: 'subscribe', requestId, topics });
  }

  pause(token) {
    const sub = this.subscriptions.get(token);
    if (!sub || sub.state !== 'active') return false;
    return this._send({ type: 'pause', subscriptionToken: token });
  }

  resume(token) {
    const sub = this.subscriptions.get(token);
    if (!sub || sub.state !== 'active') return false;
    return this._send({ type: 'resume', subscriptionToken: token });
  }

  _requestId() {
    return `collect-${Date.now()}-${++runCounter}`;
  }

  _send(value) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return false;
    try { this.socket.send(JSON.stringify(value)); return true; }
    catch { return false; }
  }

  _clearTimer() {
    if (this._timer) { clearTimeout(this._timer); this._timer = null; }
  }

  _abort(code) {
    this.failed = true;
    this._diagnostic({ code });
    this._finish();
  }

  _onMessage(raw) {
    let message;
    try { message = JSON.parse(raw.toString()); }
    catch { this._diagnostic({ code: 'malformed-message' }); return; }
    if (!message || typeof message !== 'object') { this._diagnostic({ code: 'malformed-message' }); return; }
    try { this._handle(message, Buffer.from(raw)); }
    catch (error) { this._diagnostic({ code: 'internal-error', detail: String(error && error.message || error) }); }
    if (message.type === 'end') this._finish();
  }

  _handle(message, bytes) {
    switch (message.type) {
      case 'bound': return this._onBound(message, bytes);
      case 'offer': return this._onOffer(message, bytes);
      case 'unbound': return this._onUnbound(message, bytes);
      case 'paused': {
        const sub = this.subscriptions.get(message.subscriptionToken);
        if (sub) { sub.paused = true; this._persistSubscription(sub); }
        return;
      }
      case 'resumed': {
        const sub = this.subscriptions.get(message.subscriptionToken);
        if (sub) { sub.paused = false; this._persistSubscription(sub); }
        return;
      }
      case 'end': this.ended = true; return;
      default: return; // 心跳是连接层 ping/pong，其余控制消息不是交付数据
    }
  }

  _onBound(message, bytes) {
    const { requestId, channel, subscriptionToken: token } = message;
    const evidence = this._tryEvidence('bound', { requestId, channel, subscriptionToken: token }, bytes);
    if (!this.requested.has(requestId)) {
      this._diagnostic({ code: 'unexpected-bound', requestId, subscriptionToken: token, channel });
    }
    const sub = {
      requestId, token, channel,
      topics: this.requested.get(requestId) ?? null,
      state: 'active', paused: false, deliveries: 0, evidence,
    };
    this.subscriptions.set(token, sub);
    this.channels.set(channel, token); // 通道编号从此刻起归属该 token，可被后续 bound 复用
    this._persistSubscription(sub);
    this.emit('bound', { ...sub });
  }

  _onUnbound(message, bytes) {
    const token = message.subscriptionToken;
    const evidence = this._tryEvidence('unbound', { subscriptionToken: token }, bytes);
    const sub = this.subscriptions.get(token);
    if (!sub) { this._diagnostic({ code: 'unbound-unknown-token', subscriptionToken: token, evidence }); return; }
    sub.state = 'closed';
    if (this.channels.get(sub.channel) === token) this.channels.delete(sub.channel);
    this._persistSubscription(sub);
    this.emit('unbound', { ...sub });
  }

  _onOffer(message, bytes) {
    const { channel, subscriptionToken: token, deliveryId, payload } = message;
    const sub = this.subscriptions.get(token);
    // 订阅确认（bound）之前、退订回执（unbound）之后到达的交付都不算进对应范围。
    if (!sub || sub.state !== 'active') {
      const code = sub ? 'offer-after-unbound' : 'unattributed-offer';
      const evidence = this._tryEvidence('offer-rejected', { subscriptionToken: token ?? null, deliveryId: deliveryId ?? null, channel: channel ?? null, reason: code }, bytes);
      this._diagnostic({ code, deliveryId: deliveryId ?? null, subscriptionToken: token ?? null, channel: channel ?? null, evidence });
      return;
    }
    if (channel !== sub.channel) {
      const evidence = this._tryEvidence('offer-rejected', { subscriptionToken: token, deliveryId, channel, reason: 'channel-mismatch' }, bytes);
      this._diagnostic({ code: 'channel-mismatch', deliveryId, subscriptionToken: token, channel, expected: sub.channel, evidence });
      return;
    }
    const key = `${token}#${deliveryId}`;
    const payloadDigest = digest(Buffer.from(stable(payload)));
    const existing = this.deliveries.get(key);
    if (existing) {
      if (existing.payloadDigest === payloadDigest) {
        existing.redeliveries += 1; // 同内容重投：已收可以重 ack
        if (this._send({ type: 'ack', subscriptionToken: token, deliveryId })) this._tryUpdateDelivery(key, existing);
      } else {
        // 同 deliveryId 不同 payload：隔离，不覆盖已收记录，也不确认领取。
        const evidence = this._tryEvidence('offer-rejected', { subscriptionToken: token, deliveryId, channel, reason: 'conflict' }, bytes);
        this._diagnostic({ code: 'conflict', deliveryId, subscriptionToken: token, kept: existing.evidence, rejected: evidence });
      }
      return;
    }
    const record = {
      deliveryId, subscriptionToken: token, channel, requestId: sub.requestId,
      payload, payloadDigest, redeliveries: 0, acked: false, status: 'persisted',
      seq: ++this.seq, evidence: null,
    };
    try {
      record.evidence = this.store.saveEvidence('offer', { subscriptionToken: token, deliveryId, channel }, bytes);
      this.store.put('deliveries', key, record);
    } catch (error) {
      this._persistFailure('offer', error, { deliveryId, subscriptionToken: token });
      this.pending.push({ deliveryId, subscriptionToken: token, channel, reason: 'persistence-failure' });
      return; // 本地未真正收妥，不发送领取确认
    }
    this.deliveries.set(key, record);
    sub.deliveries += 1;
    if (this._send({ type: 'ack', subscriptionToken: token, deliveryId })) {
      record.acked = true;
      record.status = 'complete';
      this.records.push(record);
      this._tryUpdateDelivery(key, record);
      this.emit('record', record);
    } else {
      this._tryUpdateDelivery(key, record);
      this.pending.push({ deliveryId, subscriptionToken: token, channel, reason: 'ack-not-sent' });
    }
  }

  _finish() {
    if (this._finished) return;
    this._finished = true;
    this._clearTimer();
    if (!this.ended) this._diagnostic({ code: 'connection-closed', detail: 'socket closed before the source signalled end' });
    const finalize = () => { if (this._resolveDone) this._resolveDone(this._result()); };
    const socket = this.socket;
    if (socket && socket.readyState !== WebSocket.CLOSED) {
      const guard = setTimeout(() => socket.terminate(), 1000);
      socket.once('close', () => { clearTimeout(guard); finalize(); });
      if (socket.readyState === WebSocket.OPEN) { try { socket.close(); } catch { /* 等待 close 事件收尾 */ } }
    } else {
      finalize();
    }
  }

  _result() {
    const complete = this.ended && !this.failed;
    return {
      mode: 'collect',
      source: this.source.url,
      status: complete ? 'complete' : 'incomplete',
      records: this.records,
      pending: this.pending,
      diagnostics: this.diagnostics,
      subscriptions: [...this.subscriptions.values()].map(sub => ({
        requestId: sub.requestId,
        subscriptionToken: sub.token,
        channel: sub.channel,
        topics: sub.topics,
        state: sub.state,
        paused: sub.paused,
        deliveries: sub.deliveries,
        evidence: sub.evidence,
      })),
    };
  }

  _diagnostic(entry) {
    const diagnostic = { seq: ++this.seq, ...entry };
    this.diagnostics.push(diagnostic);
    this.emit('diagnostic', diagnostic);
  }

  _persistFailure(stage, error, extra = {}) {
    this.failed = true;
    this._diagnostic({ code: 'persistence-failure', stage, detail: String(error && error.message || error), ...extra });
  }

  _tryEvidence(kind, metadata, bytes) {
    try { return this.store.saveEvidence(kind, metadata, bytes); }
    catch (error) { this._persistFailure(kind, error); return null; }
  }

  _persistSubscription(sub) {
    try {
      this.store.put('subscriptions', sub.token, {
        requestId: sub.requestId, channel: sub.channel, topics: sub.topics,
        state: sub.state, paused: sub.paused, deliveries: sub.deliveries, evidence: sub.evidence,
      });
    } catch (error) { this._persistFailure('subscription', error, { subscriptionToken: sub.token }); }
  }

  _tryUpdateDelivery(key, record) {
    try { this.store.put('deliveries', key, record); }
    catch (error) { this._persistFailure('delivery', error, { deliveryId: record.deliveryId }); }
  }
}

// 收件器入口：调用方负责创建和关闭来源及 Inbox。
export async function collect(source, store, options = {}) {
  const collector = new Collector(source, store, options);
  return collector.run(options);
}
