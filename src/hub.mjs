import { WebSocketServer } from 'ws';

export async function fixture(scenario = 'normal') {
  const server = new WebSocketServer({ port: 0, host: '127.0.0.1' });
  await new Promise(resolve => server.once('listening', resolve));
  const received = [];
  server.on('connection', socket => {
    socket.on('message', data => {
      const message = JSON.parse(data); received.push(message);
      const send = value => socket.send(JSON.stringify(value));
      if (message.type === 'hello') send({ type: 'ready', protocol: 1 });
      if (message.type === 'subscribe') {
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
        socket.ping();
        send({ type: 'end', requestId: message.requestId });
      }
      if (message.type === 'pause') send({ type: 'paused', subscriptionToken: message.subscriptionToken });
      if (message.type === 'resume') send({ type: 'resumed', subscriptionToken: message.subscriptionToken });
    });
  });
  return { url: `ws://127.0.0.1:${server.address().port}`, topics: ['warehouse', 'carrier'], received,
    close: async () => { for (const socket of server.clients) socket.terminate(); await new Promise(resolve => server.close(resolve)); } };
}
