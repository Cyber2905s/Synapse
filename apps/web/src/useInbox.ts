import { useEffect, useRef, useState } from 'react';
import { get, type Notification } from './api.ts';

export type Connection = 'connecting' | 'live' | 'offline';

/**
 * Live inbox: backfills over HTTP on every (re)connect, then merges WebSocket pushes, de-duplicating
 * by notification id (a push can race the backfill). Each pushed notification is acked, which the
 * API records as `delivered`.
 */
export function useInbox(userId: string, onArrive: () => void) {
  const [items, setItems] = useState<Notification[]>([]);
  const [fresh, setFresh] = useState<Set<string>>(new Set());
  const [connection, setConnection] = useState<Connection>('connecting');
  const arrive = useRef(onArrive);
  arrive.current = onArrive;

  useEffect(() => {
    let ws: WebSocket | null = null;
    let retry = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let closed = false;
    setItems([]);

    const merge = (incoming: Notification[]) =>
      setItems((prev) => {
        const byId = new Map(prev.map((n) => [n.id, n]));
        for (const n of incoming) byId.set(n.id, { ...byId.get(n.id), ...n });
        return [...byId.values()].sort((a, b) => b.createdAt.localeCompare(a.createdAt));
      });

    const connect = () => {
      setConnection('connecting');
      const proto = location.protocol === 'https:' ? 'wss' : 'ws';
      ws = new WebSocket(`${proto}://${location.host}/v1/ws?userId=${encodeURIComponent(userId)}`);
      ws.onmessage = (e) => {
        const msg = JSON.parse(e.data as string);
        if (msg.type === 'ready') {
          retry = 0;
          setConnection('live');
          get<Notification[]>(`/v1/users/${encodeURIComponent(userId)}/notifications?limit=100`)
            .then(merge)
            .catch(() => undefined);
        } else if (msg.type === 'notification') {
          const n = msg.notification as Notification;
          ws?.send(JSON.stringify({ type: 'ack', id: n.id }));
          merge([{ ...n, status: 'delivered' }]);
          setFresh((s) => new Set(s).add(n.id));
          arrive.current();
        }
      };
      ws.onclose = () => {
        if (closed) return;
        setConnection('offline');
        timer = setTimeout(connect, Math.min(10_000, 500 * 2 ** retry++));
      };
    };
    connect();

    return () => {
      closed = true;
      clearTimeout(timer);
      ws?.close();
    };
  }, [userId]);

  return { items, fresh, connection };
}
