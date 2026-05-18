import { useCallback, useEffect, useState } from 'react';
import {
  MAILPIT_URL,
  get,
  send,
  type Channel,
  type DlqEntry,
  type EventType,
  type Notification,
  type Preferences,
  type Sink,
} from './api.ts';
import { useInbox } from './useInbox.ts';

const CHANNEL_NAMES: Record<Channel, string> = {
  in_app: 'In-app',
  email: 'Email',
  webhook: 'Webhook',
};

const time = (iso: string | number) =>
  new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });

/** Re-runs `fn` every `ms` while mounted and whenever `deps` change. */
function usePoll<T>(fn: () => Promise<T>, ms: number, deps: unknown[]) {
  const [value, setValue] = useState<T | null>(null);
  const [tick, setTick] = useState(0);
  useEffect(() => {
    let live = true;
    const run = () =>
      fn()
        .then((v) => live && setValue(v))
        .catch(() => undefined);
    run();
    const id = setInterval(run, ms);
    return () => {
      live = false;
      clearInterval(id);
    };
  }, [...deps, tick]);
  return [value, () => setTick((t) => t + 1)] as const;
}

export function App() {
  const [userId, setUserId] = useState('alice');
  const [draftUser, setDraftUser] = useState('alice');
  const [pulse, setPulse] = useState(0);
  const [arrivals, setArrivals] = useState(0);
  const onArrive = useCallback(() => setArrivals((a) => a + 1), []);
  const inbox = useInbox(userId, onArrive);

  return (
    <div className="shell">
      <header className="masthead">
        <h1 className="wordmark">Synapse</h1>
        <form
          className="viewer"
          onSubmit={(e) => {
            e.preventDefault();
            if (draftUser.trim()) setUserId(draftUser.trim());
          }}
        >
          <label htmlFor="user">Viewing inbox of</label>
          <input id="user" value={draftUser} onChange={(e) => setDraftUser(e.target.value)} />
          <span className={`conn conn-${inbox.connection}`} role="status">
            {inbox.connection === 'live'
              ? 'Live'
              : inbox.connection === 'connecting'
                ? 'Connecting'
                : 'Reconnecting'}
          </span>
        </form>
        <nav className="links">
          <a href={MAILPIT_URL} target="_blank" rel="noreferrer">
            Open Mailpit
          </a>
          <a href="/metrics" target="_blank" rel="noreferrer">
            Metrics
          </a>
        </nav>
      </header>

      <main className="stage">
        <Producer userId={userId} onSent={() => setPulse((p) => p + 1)} />
        <Spine pulse={pulse} arrivals={arrivals} />
        <Inbox items={inbox.items} fresh={inbox.fresh} userId={userId} />
      </main>

      <section className="lower">
        <Deliveries userId={userId} />
        <DeadLetters />
        <PreferencesForm userId={userId} />
      </section>
    </div>
  );
}

function Producer({ userId, onSent }: { userId: string; onSent: () => void }) {
  const [types, setTypes] = useState<EventType[]>([]);
  const [type, setType] = useState('');
  const [payload, setPayload] = useState('');
  const [reuseId, setReuseId] = useState(false);
  const [lastId, setLastId] = useState<string | null>(null);
  const [log, setLog] = useState<
    { key: number; type: string; id: string; status: number; ms: number }[]
  >([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    get<EventType[]>('/v1/event-types').then((t) => {
      setTypes(t);
      setType((cur) => cur || t[0]?.type || '');
    });
  }, []);

  // Point the sample payload at whoever's inbox is on screen.
  useEffect(() => {
    const t = types.find((x) => x.type === type);
    if (!t) return;
    const data = { ...t.sample };
    if ('userId' in data) data.userId = userId;
    if (Array.isArray(data.userIds)) data.userIds = [userId, ...data.userIds.slice(1)];
    setPayload(JSON.stringify(data, null, 2));
  }, [type, types, userId]);

  const fire = async (count: number) => {
    let data: unknown;
    try {
      data = JSON.parse(payload);
    } catch {
      setError('The payload is not valid JSON. Fix it and send again.');
      return;
    }
    setError(null);
    for (let i = 0; i < count; i++) {
      const id = reuseId && lastId ? lastId : crypto.randomUUID();
      const started = performance.now();
      const res = await send<{ error?: string; issues?: { path: string[]; message: string }[] }>(
        '/v1/events',
        {
          id,
          type,
          data,
        },
      );
      const ms = Math.round(performance.now() - started);
      setLastId(id);
      setLog((l) => [{ key: Date.now() + i, type, id, status: res.status, ms }, ...l].slice(0, 8));
      if (res.status === 202) onSent();
      else {
        const issue = res.body?.issues?.[0];
        setError(
          issue
            ? `${issue.path.join('.')}: ${issue.message}`
            : (res.body?.error ?? `Rejected with ${res.status}`),
        );
        break;
      }
    }
  };

  const current = types.find((t) => t.type === type);

  return (
    <section className="panel producer" aria-labelledby="producer-h">
      <h2 id="producer-h">Send an event</h2>
      <p className="lede">Publish a domain event the way a backend service would.</p>

      <label className="field">
        <span>Event type</span>
        <select value={type} onChange={(e) => setType(e.target.value)}>
          {types.map((t) => (
            <option key={t.type} value={t.type}>
              {t.type}
            </option>
          ))}
        </select>
      </label>
      {current && (
        <p className="hint">
          {current.description}. Goes out by{' '}
          {current.channels.map((c) => CHANNEL_NAMES[c].toLowerCase()).join(', ')}.
        </p>
      )}

      <label className="field">
        <span>Payload</span>
        <textarea
          className="code"
          spellCheck={false}
          rows={8}
          value={payload}
          onChange={(e) => setPayload(e.target.value)}
        />
      </label>

      <label className="check">
        <input type="checkbox" checked={reuseId} onChange={(e) => setReuseId(e.target.checked)} />
        Reuse the last event ID, to see duplicates dropped
      </label>

      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}

      <div className="actions">
        <button className="primary" onClick={() => fire(1)} disabled={!type}>
          Send event
        </button>
        <button onClick={() => fire(25)} disabled={!type}>
          Send 25
        </button>
      </div>

      {log.length > 0 && (
        <ol className="sendlog" aria-label="Recently sent">
          {log.map((l) => (
            <li key={l.key}>
              <span className={l.status === 202 ? 'ok' : 'bad'}>{l.status}</span>
              <span>{l.type}</span>
              <code title={l.id}>{l.id.slice(0, 8)}</code>
              <span className="muted">{l.ms} ms</span>
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}

/** The one piece of motion: a signal travelling from the producer to the inbox. */
function Spine({ pulse, arrivals }: { pulse: number; arrivals: number }) {
  return (
    <div className="spine" aria-hidden="true">
      <div className="track" />
      {pulse > 0 && <div key={`p${pulse}`} className="signal" />}
      <div key={`a${pulse}`} className={`node ${pulse ? 'lit' : ''}`} style={{ top: '18%' }}>
        <span>Accepted</span>
      </div>
      <div key={`r${pulse}`} className={`node ${pulse ? 'lit delay' : ''}`} style={{ top: '50%' }}>
        <span>Routed</span>
      </div>
      <div key={`d${arrivals}`} className={`node ${arrivals ? 'lit' : ''}`} style={{ top: '82%' }}>
        <span>Pushed</span>
      </div>
    </div>
  );
}

function Inbox({
  items,
  fresh,
  userId,
}: {
  items: Notification[];
  fresh: Set<string>;
  userId: string;
}) {
  return (
    <section className="panel inbox" aria-labelledby="inbox-h" aria-live="polite">
      <h2 id="inbox-h">
        Inbox <span className="count">{items.length}</span>
      </h2>
      <p className="lede">What {userId} sees in the app, pushed over WebSocket.</p>
      {items.length === 0 ? (
        <p className="empty">
          Nothing here yet. Send an event addressed to {userId} and it will land here.
        </p>
      ) : (
        <ul className="notes">
          {items.map((n) => (
            <li key={n.id} className={fresh.has(n.id) ? 'note fresh' : 'note'}>
              <div className="note-head">
                <strong>{n.title}</strong>
                <time dateTime={n.createdAt}>{time(n.createdAt)}</time>
              </div>
              <p>{n.body}</p>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

function Deliveries({ userId }: { userId: string }) {
  const [rows] = usePoll(
    () => get<Notification[]>(`/v1/deliveries?userId=${encodeURIComponent(userId)}&limit=15`),
    1500,
    [userId],
  );
  return (
    <section className="panel" aria-labelledby="deliv-h">
      <h2 id="deliv-h">Delivery status</h2>
      <p className="lede">Every channel attempt for {userId}, as stored in Postgres.</p>
      {!rows?.length ? (
        <p className="empty">No deliveries for {userId} yet.</p>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th scope="col">Notification</th>
              <th scope="col">Channel</th>
              <th scope="col">Status</th>
              <th scope="col">Tries</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((d) => (
              <tr key={d.id} title={d.lastError ?? undefined}>
                <td className="clip">{d.title}</td>
                <td>{CHANNEL_NAMES[d.channel]}</td>
                <td>
                  <span className={`status s-${d.status}`}>{d.status}</span>
                </td>
                <td>{d.attempts}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

function DeadLetters() {
  const [dlq, refreshDlq] = usePoll(
    () => get<{ size: number; entries: DlqEntry[] }>('/v1/dlq'),
    2000,
    [],
  );
  const [sink, refreshSink] = usePoll(() => get<Sink>('/demo/webhook-sink'), 2000, []);
  const [notice, setNotice] = useState<string | null>(null);

  const replay = async (entryIds?: string[]) => {
    const res = await send<{ replayed: number }>('/v1/dlq/replay', entryIds ? { entryIds } : {});
    setNotice(
      `Replayed ${res.body.replayed} ${res.body.replayed === 1 ? 'delivery' : 'deliveries'}.`,
    );
    refreshDlq();
  };

  const toggleSink = async () => {
    await send('/demo/webhook-sink/mode', { failing: !sink?.failing }, 'PUT');
    refreshSink();
  };

  return (
    <section className="panel" aria-labelledby="dlq-h">
      <h2 id="dlq-h">
        Dead letters <span className="count">{dlq?.size ?? 0}</span>
      </h2>
      <p className="lede">
        Deliveries that failed every retry. Replay them once the cause is fixed.
      </p>

      <div className="sink">
        <label className="check">
          <input type="checkbox" checked={!!sink?.failing} onChange={toggleSink} />
          Make the demo webhook endpoint fail
        </label>
        <span className="muted">{sink?.received.length ?? 0} signed calls received</span>
      </div>

      {notice && (
        <p className="notice" role="status">
          {notice}
        </p>
      )}

      {!dlq?.entries.length ? (
        <p className="empty">
          The queue is empty. Turn on the failing endpoint and send a webhook event to fill it.
        </p>
      ) : (
        <>
          <ul className="dead">
            {dlq.entries.map((e) => (
              <li key={e.entryId}>
                <div>
                  <strong className="clip">{e.delivery?.title ?? e.deliveryId}</strong>
                  <span className="muted">
                    {e.delivery
                      ? `${CHANNEL_NAMES[e.delivery.channel]} to ${e.delivery.userId}, `
                      : ''}
                    {e.error} at {time(e.failedAt)}
                  </span>
                </div>
                <button onClick={() => replay([e.entryId])}>Replay</button>
              </li>
            ))}
          </ul>
          <button onClick={() => replay()}>Replay all</button>
        </>
      )}
    </section>
  );
}

function PreferencesForm({ userId }: { userId: string }) {
  const [prefs, setPrefs] = useState<Preferences | null>(null);
  const [quiet, setQuiet] = useState({ on: false, start: '22:00', end: '07:00' });
  const [saved, setSaved] = useState<string | null>(null);

  useEffect(() => {
    setSaved(null);
    get<Preferences>(`/v1/users/${encodeURIComponent(userId)}/preferences`).then((p) => {
      setPrefs(p);
      setQuiet(
        p.quietHours ? { on: true, ...p.quietHours } : { on: false, start: '22:00', end: '07:00' },
      );
    });
  }, [userId]);

  if (!prefs) return <section className="panel" aria-busy="true" />;

  const save = async () => {
    const sink = await get<Sink>('/demo/webhook-sink');
    const res = await send(
      `/v1/users/${encodeURIComponent(userId)}/preferences`,
      {
        channels: prefs.channels,
        quietHours: quiet.on
          ? {
              start: quiet.start,
              end: quiet.end,
              timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
            }
          : null,
        // The demo routes webhooks to the built-in receiver so signatures can be checked end to end.
        webhookUrl: prefs.channels.webhook ? sink.url : prefs.webhookUrl,
      },
      'PUT',
    );
    setSaved(
      res.status === 200 ? 'Preferences saved.' : 'Could not save. Check the quiet hours times.',
    );
  };

  return (
    <section className="panel" aria-labelledby="prefs-h">
      <h2 id="prefs-h">Preferences</h2>
      <p className="lede">How {userId} wants to be reached.</p>
      <fieldset>
        <legend>Channels</legend>
        {(Object.keys(CHANNEL_NAMES) as Channel[]).map((c) => (
          <label className="check" key={c}>
            <input
              type="checkbox"
              checked={prefs.channels[c]}
              onChange={(e) =>
                setPrefs({ ...prefs, channels: { ...prefs.channels, [c]: e.target.checked } })
              }
            />
            {CHANNEL_NAMES[c]}
            {c === 'email' && prefs.email && <span className="muted"> to {prefs.email}</span>}
          </label>
        ))}
      </fieldset>
      <fieldset>
        <legend>Quiet hours</legend>
        <label className="check">
          <input
            type="checkbox"
            checked={quiet.on}
            onChange={(e) => setQuiet({ ...quiet, on: e.target.checked })}
          />
          Hold email and webhooks overnight
        </label>
        <div className="times">
          <label>
            From{' '}
            <input
              type="time"
              value={quiet.start}
              disabled={!quiet.on}
              onChange={(e) => setQuiet({ ...quiet, start: e.target.value })}
            />
          </label>
          <label>
            until{' '}
            <input
              type="time"
              value={quiet.end}
              disabled={!quiet.on}
              onChange={(e) => setQuiet({ ...quiet, end: e.target.value })}
            />
          </label>
        </div>
      </fieldset>
      <div className="actions">
        <button className="primary" onClick={save}>
          Save preferences
        </button>
        {saved && (
          <span className="notice" role="status">
            {saved}
          </span>
        )}
      </div>
    </section>
  );
}
