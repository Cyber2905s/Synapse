import type { WebSocket } from '@fastify/websocket';
import { z } from 'zod';
import { KEYS } from '@synapse/shared';
import type { Route } from '../app.ts';
import { wsConnections } from '../metrics.ts';

const clientMessage = z.object({ type: z.literal('ack'), id: z.uuid() });
const PREFIX = KEYS.userChannel('');

/**
 * Live in-app feed. Workers PUBLISH to `synapse:user:<id>`; every API instance subscribes only to
 * the channels of users it currently holds sockets for, so any instance can serve any user and
 * adding instances doesn't multiply traffic.
 */
export const wsRoutes: Route = (app, { db, sub }) => {
  const sockets = new Map<string, Set<WebSocket>>();

  sub.on('message', (channel: string, message: string) => {
    const frame = JSON.stringify({ type: 'notification', notification: JSON.parse(message) });
    for (const socket of sockets.get(channel.slice(PREFIX.length)) ?? []) socket.send(frame);
  });

  // Drop peers that stop answering pings so their subscriptions don't leak.
  const alive = new WeakSet<WebSocket>();
  const heartbeat = setInterval(() => {
    for (const set of sockets.values())
      for (const socket of set) {
        if (!alive.has(socket)) socket.terminate();
        alive.delete(socket);
        socket.ping();
      }
  }, 30_000);
  app.addHook('onClose', async () => clearInterval(heartbeat));

  app.get('/v1/ws', { websocket: true }, async (socket, req) => {
    const parsed = z.object({ userId: z.string().min(1).max(128) }).safeParse(req.query);
    if (!parsed.success) return socket.close(1008, 'userId query parameter required');
    const { userId } = parsed.data;

    alive.add(socket);
    socket.on('pong', () => alive.add(socket));

    let set = sockets.get(userId);
    if (!set) {
      set = new Set();
      sockets.set(userId, set);
      await sub.subscribe(KEYS.userChannel(userId));
    }
    set.add(socket);
    wsConnections.inc();

    // Client acknowledges receipt → delivery moves from 'sent' to 'delivered'.
    socket.on('message', async (raw) => {
      let msg;
      try {
        msg = clientMessage.safeParse(JSON.parse(raw.toString()));
      } catch {
        return; // not JSON
      }
      if (!msg.success) return;
      await db.query(
        `UPDATE deliveries SET status = 'delivered', delivered_at = now(), updated_at = now()
         WHERE id = $1 AND user_id = $2 AND status = 'sent'`,
        [msg.data.id, userId],
      );
    });

    socket.on('close', async () => {
      wsConnections.dec();
      set.delete(socket);
      if (set.size === 0 && sockets.get(userId) === set) {
        sockets.delete(userId);
        await sub.unsubscribe(KEYS.userChannel(userId));
      }
    });

    socket.send(JSON.stringify({ type: 'ready', userId }));
  });
};
