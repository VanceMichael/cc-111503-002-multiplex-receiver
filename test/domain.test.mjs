import test from 'node:test';
import assert from 'node:assert/strict';
import WebSocket from 'ws';
import { fixture } from '../src/hub.mjs';
import { preview } from '../src/probe.mjs';
import { Inbox } from '../src/store.mjs';

test('real websocket handshake can be probed and closed without subscribing', async () => {
  const source = await fixture(); const store = new Inbox();
  try { assert.equal((await preview(source, store)).ready.protocol, 1); assert.equal(source.received.length, 1); }
  finally { store.close(); await source.close(); }
});
test('hub exposes a token boundary independently of the channel number', async () => {
  const source = await fixture('reuse'); const socket = new WebSocket(source.url); const messages = [];
  try {
    await new Promise(resolve => socket.once('open', resolve));
    const done = new Promise(resolve => socket.on('message', bytes => {
      const message = JSON.parse(bytes); messages.push(message); if (message.type === 'end') resolve();
    }));
    socket.send(JSON.stringify({ type: 'subscribe', requestId: 'r1', topics: source.topics }));
    await done;
    const bindings = messages.filter(message => message.type === 'bound');
    assert.equal(bindings.length, 2); assert.notEqual(bindings[0].subscriptionToken, bindings[1].subscriptionToken);
  } finally { socket.terminate(); await source.close(); }
});
