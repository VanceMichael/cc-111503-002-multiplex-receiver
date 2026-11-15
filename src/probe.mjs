import WebSocket from 'ws';

export async function preview(source, store) {
  const socket = new WebSocket(source.url);
  await new Promise((resolve, reject) => { socket.once('open', resolve); socket.once('error', reject); });
  try {
    const reply = new Promise((resolve, reject) => { socket.once('message', resolve); socket.once('error', reject); });
    socket.send(JSON.stringify({ type: 'hello' }));
    const bytes = Buffer.from(await reply);
    const evidence = store.saveEvidence('handshake', { source: source.url }, bytes);
    return { mode: 'probe', ready: JSON.parse(bytes), evidence };
  } finally {
    await new Promise(resolve => { socket.once('close', resolve); socket.close(); });
  }
}
