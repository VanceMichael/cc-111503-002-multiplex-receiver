import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture } from '../src/hub.mjs';
import { collect, Collector } from '../src/intake.mjs';
import { Inbox } from '../src/store.mjs';

test('a busy channel can be paused alone without stopping the shared connection', async () => {
  const source = await fixture('fanout'); const store = new Inbox();
  try {
    const collector = new Collector(source, store);
    const landed = [];
    collector.on('record', record => {
      landed.push(record.deliveryId);
      if (record.deliveryId === 'r1-D2') collector.pause('r1:token');
      if (record.deliveryId === 'r2-D4') collector.resume('r1:token');
    });
    const result = await collector.run({ subscriptions: [{ requestId: 'r1' }, { requestId: 'r2' }] });
    assert.equal(result.status, 'complete');
    assert.equal(result.records.length, 8);
    assert.equal(result.records.filter(row => row.subscriptionToken === 'r1:token').length, 4);
    assert.equal(result.records.filter(row => row.subscriptionToken === 'r2:token').length, 4);
    assert.ok(landed.indexOf('r2-D4') < landed.indexOf('r1-D3'),
      'channel one stayed paused while channel two kept delivering on the same connection');
    assert.ok(source.received.some(message => message.type === 'pause' && message.subscriptionToken === 'r1:token'));
    assert.ok(source.received.some(message => message.type === 'resume' && message.subscriptionToken === 'r1:token'));
    assert.deepEqual(result.subscriptions.map(row => row.channel).sort(), [7, 8]);
  } finally { store.close(); await source.close(); }
});

test('offers landing after the unbound receipt stay out of the old scope', async () => {
  const source = await fixture('straggler'); const store = new Inbox();
  try {
    const result = await collect(source, store);
    assert.deepEqual(result.records.map(row => row.deliveryId), ['D1']);
    assert.ok(result.diagnostics.some(row => row.code === 'offer-after-unbound' && row.deliveryId === 'D2'));
    assert.ok(!source.received.some(message => message.type === 'ack' && message.deliveryId === 'D2'));
    assert.equal(store.list('deliveries').length, 1);
  } finally { store.close(); await source.close(); }
});

test('a reconnect subscribes fresh and never restores scope from old channel numbers', async () => {
  const source = await fixture('normal'); const store = new Inbox();
  try {
    const first = await collect(source, store);
    const second = await collect(source, store);
    assert.equal(first.status, 'complete'); assert.equal(second.status, 'complete');
    assert.equal(first.records[0].channel, second.records[0].channel);
    assert.notEqual(first.records[0].subscriptionToken, second.records[0].subscriptionToken);
    assert.equal(store.list('deliveries').length, 2);
    assert.equal(source.received.filter(message => message.type === 'subscribe').length, 2);
  } finally { store.close(); await source.close(); }
});

test('deliveries join back to their subscription boundary and ack state', async () => {
  const source = await fixture('reuse'); const store = new Inbox();
  try {
    const result = await collect(source, store);
    assert.equal(result.subscriptions.length, 2);
    assert.equal(result.subscriptions[0].state, 'closed');
    assert.equal(result.subscriptions[1].state, 'active');
    for (const record of result.records) {
      const sub = result.subscriptions.find(row => row.subscriptionToken === record.subscriptionToken);
      assert.ok(sub, 'every delivery attributes to a confirmed subscription');
      assert.equal(record.channel, sub.channel);
      assert.equal(record.acked, true);
      assert.equal(record.status, 'complete');
      assert.ok(record.evidence);
    }
    const stored = store.list('deliveries');
    assert.equal(stored.length, 2);
    assert.ok(stored.every(row => row.acked === true));
    assert.equal(store.list('subscriptions').length, 2);
  } finally { store.close(); await source.close(); }
});

test('persistence failure holds back the ack and reports pending instead', async () => {
  const source = await fixture('normal'); const store = new Inbox();
  store.close();
  try {
    const result = await collect(source, store);
    assert.equal(result.records.length, 0);
    assert.ok(result.pending.some(row => row.deliveryId === 'D1'));
    assert.ok(result.diagnostics.some(row => row.code === 'persistence-failure'));
    assert.ok(!source.received.some(message => message.type === 'ack'));
    assert.equal(result.status, 'incomplete');
  } finally { await source.close(); }
});
