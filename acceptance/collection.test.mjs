import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture } from '../src/hub.mjs';
import * as intake from '../src/intake.mjs';
import { Inbox } from '../src/store.mjs';

async function run(scenario, check) {
  assert.equal(typeof intake.collect, 'function', 'collection entry is not available yet');
  const source = await fixture(scenario); const store = new Inbox();
  try {
    const result = await intake.collect(source, store);
    await check(result, source, store);
  } finally { store.close(); await source.close(); }
}

test('subscribed deliveries are persisted before acknowledgement', async () => {
  await run('normal', async (result, source, store) => {
    assert.equal(result.records.length, 1); assert.equal(result.records[0].deliveryId, 'D1');
    assert.ok(source.received.some(message => message.type === 'ack' && message.deliveryId === 'D1'));
    assert.ok(store.list('deliveries').length > 0);
  });
});
test('a channel number is not a subscription identity', async () => {
  await run('reuse', async result => {
    assert.equal(result.records.length, 2);
    assert.equal(new Set(result.records.map(row => row.subscriptionToken)).size, 2);
  });
});
test('unbound and contradictory offers are not accepted silently', async () => {
  await run('provisional', async result => {
    assert.equal(result.records.some(row => row.deliveryId === 'premature'), false);
    assert.ok(result.diagnostics.length > 0);
  });
  await run('duplicate', async result => {
    assert.equal(result.records.length, 1); assert.ok(result.diagnostics.some(row => row.code === 'conflict'));
  });
});
