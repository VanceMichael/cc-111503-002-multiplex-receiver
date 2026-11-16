import { WebSocketServer } from 'ws';

export async function fixture(scenario = 'normal') {
  const server = new WebSocketServer({ port: 0, host: '127.0.0.1' });
  await new Promise(resolve => server.once('listening', resolve));
  const received = [];
  server.on('connection', socket => {
    const fanout = { subscriptions: new Map(), ended: false };
    socket.on('close', () => {
      for (const sub of fanout.subscriptions.values()) if (sub.timer) clearTimeout(sub.timer);
    });
    socket.on('message', data => {
      const message = JSON.parse(data); received.push(message);
      const send = value => socket.send(JSON.stringify(value));
      if (message.type === 'hello') send({ type: 'ready', protocol: 1 });
      if (message.type === 'subscribe') {
        if (scenario === 'fanout') {
          // 两个订阅各自一条通道，交错投递；pause/resume 按 token 生效，连接保持在线。
          const token = message.requestId + ':token';
          const channel = 7 + fanout.subscriptions.size;
          const sub = { token, channel, index: 0, paused: false, timer: null, pump: null,
            offers: [1, 2, 3, 4].map(n => ({ deliveryId: `${message.requestId}-D${n}`, payload: { shipment: `${message.requestId}-S${n}` } })) };
          const maybeEnd = () => {
            if (fanout.ended) return;
            if ([...fanout.subscriptions.values()].every(entry => entry.index >= entry.offers.length)) {
              fanout.ended = true; send({ type: 'end' });
            }
          };
          sub.pump = () => {
            sub.timer = null;
            if (!sub.paused && sub.index < sub.offers.length) {
              const offer = sub.offers[sub.index++];
              send({ type: 'offer', channel: sub.channel, subscriptionToken: token, deliveryId: offer.deliveryId, payload: offer.payload });
            }
            if (!sub.paused && sub.index < sub.offers.length) sub.timer = setTimeout(sub.pump, 20);
            maybeEnd();
          };
          fanout.subscriptions.set(token, sub);
          send({ type: 'bound', requestId: message.requestId, channel, subscriptionToken: token });
          sub.timer = setTimeout(sub.pump, 20);
        } else {
          const token = message.requestId + ':token';
          if (scenario === 'provisional') send({ type: 'offer', channel: 7, subscriptionToken: 'unknown', deliveryId: 'premature', payload: {} });
          send({ type: 'bound', requestId: message.requestId, channel: 7, subscriptionToken: token });
          send({ type: 'offer', channel: 7, subscriptionToken: token, deliveryId: 'D1', payload: { shipment: 'S1' } });
          if (scenario === 'duplicate') {
            send({ type: 'offer', channel: 7, subscriptionToken: token, deliveryId: 'D1', payload: { shipment: 'S1' } });
            send({ type: 'offer', channel: 7, subscriptionToken: token, deliveryId: 'D1', payload: { shipment: 'different' } });
          }
          if (scenario === 'reuse') {
            send({ type: 'unbound', subscriptionToken: token });
            send({ type: 'bound', requestId: message.requestId, channel: 7, subscriptionToken: token + ':new' });
            send({ type: 'offer', channel: 7, subscriptionToken: token + ':new', deliveryId: 'D2', payload: { shipment: 'S2' } });
          }
          if (scenario === 'straggler') {
            // 退订回执之后才到达的旧通道交付：不得计入已关闭的范围。
            send({ type: 'unbound', subscriptionToken: token });
            send({ type: 'offer', channel: 7, subscriptionToken: token, deliveryId: 'D2', payload: { shipment: 'S2' } });
          }
          socket.ping();
          send({ type: 'end', requestId: message.requestId });
        }
      }
      if (message.type === 'pause') {
        send({ type: 'paused', subscriptionToken: message.subscriptionToken });
        const sub = fanout.subscriptions.get(message.subscriptionToken);
        if (sub) { sub.paused = true; if (sub.timer) { clearTimeout(sub.timer); sub.timer = null; } }
      }
      if (message.type === 'resume') {
        send({ type: 'resumed', subscriptionToken: message.subscriptionToken });
        const sub = fanout.subscriptions.get(message.subscriptionToken);
        if (sub) { sub.paused = false; if (!sub.timer && sub.index < sub.offers.length) sub.timer = setTimeout(sub.pump, 5); }
      }
    });
  });
  return { url: `ws://127.0.0.1:${server.address().port}`, topics: ['warehouse', 'carrier'], received,
    close: async () => { for (const socket of server.clients) socket.terminate(); await new Promise(resolve => server.close(resolve)); } };
}
