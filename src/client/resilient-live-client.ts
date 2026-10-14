import { z } from "zod";
import { PinnedFrameValidator } from "./pinned-frame-validator.js";
import { RenewalCoordinator } from "../canonical/renewal-coordinator.js";

export type LiveFrame = Readonly<Record<string, unknown>>;
export interface LiveTicket {
  endpoint: string;
  version: "2.0";
  ticket: string;
  expires_at: string;
  renew_after_seconds: 30;
}
export interface LiveSnapshot {
  cursor: string;
  matches: LiveFrame[];
  replacement: boolean;
}
export interface LiveDiagnostic {
  code: string;
  lane?: number;
  closeCode?: number;
}
export interface LiveStatus {
  state:
    | "idle"
    | "connecting"
    | "live"
    | "recovering"
    | "resyncing"
    | "failed"
    | "stopped";
  readyLanes: number;
  cursor: string | null;
}
export interface LiveClientOptions {
  endpoint: string;
  includePredictions: boolean;
  ticket: (signal: AbortSignal) => Promise<LiveTicket>;
  onSnapshot: (
    snapshot: LiveSnapshot,
    signal: AbortSignal,
  ) => void | Promise<void>;
  onEvent: (event: LiveFrame, signal: AbortSignal) => void | Promise<void>;
  onHeartbeat?: (frame: LiveFrame, signal: AbortSignal) => void | Promise<void>;
  onCheckpoint?: (cursor: string, signal: AbortSignal) => void | Promise<void>;
  onStatus?: (status: LiveStatus) => void;
  onDiagnostic?: (diagnostic: LiveDiagnostic) => void;
  connect?: (endpoint: string) => WebSocket;
  initialCursor?: string;
  subscription?: {
    match_ids?: string[];
    tours?: ("atp" | "wta" | "itf")[];
    genders?: ("men" | "women")[];
    categories?: string[];
    include_point_events?: boolean;
  };
  reconnectBaseMs?: number;
}
const cursor = z.string().regex(/^(0|[1-9]\d{0,17})$/);
const subscription = z
  .object({
    match_ids: z
      .array(
        z
          .string()
          .regex(
            /^mat_[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/,
          ),
      )
      .max(32)
      .optional(),
    tours: z
      .array(z.enum(["atp", "wta", "itf"]))
      .max(3)
      .optional(),
    genders: z
      .array(z.enum(["men", "women"]))
      .max(2)
      .optional(),
    categories: z.array(z.string().min(1).max(40)).max(32).optional(),
    include_point_events: z.boolean().optional(),
  })
  .strict();
const ticketSchema = z
  .object({
    endpoint: z.string(),
    version: z.literal("2.0"),
    ticket: z.string().min(1).max(8192),
    expires_at: z.string(),
    renew_after_seconds: z.literal(30),
  })
  .strict();
const bytes = (text: string) => new TextEncoder().encode(text).byteLength;
function fingerprint(value: unknown): string {
  if (Array.isArray(value)) return "[" + value.map(fingerprint).join(",") + "]";
  if (value !== null && typeof value === "object")
    return (
      "{" +
      Object.entries(value)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => JSON.stringify(k) + ":" + fingerprint(v))
        .join(",") +
      "}"
    );
  return JSON.stringify(value);
}
interface Lane {
  index: number;
  socket: WebSocket | null;
  abort: AbortController;
  ready: boolean;
  attempt: number;
  resume: string | null;
  received: string | null;
  expires: number;
  lastFrame: number;
  timers: Set<ReturnType<typeof setTimeout>>;
  snapshot: {
    cursor: string;
    count: number;
    index: number;
    matches: LiveFrame[];
    ids: Set<string>;
    bytes: number;
  } | null;
  deadline: ReturnType<typeof setTimeout> | null;
  renewTimer: ReturnType<typeof setTimeout> | null;
  renewalCount: number;
  awaitingAuth: boolean;
  replay: string | null;
  opening: boolean;
}
interface Seen {
  cursor: string;
  wire: string;
  bytes: number;
  pending: boolean;
}

/** Two identical subscriptions, one acknowledged delivery stream. No transport error is hidden. */
export class ResilientLiveClient {
  private readonly validator: PinnedFrameValidator;
  private readonly filter: Record<string, unknown>;
  private readonly lanes: Lane[];
  private readonly renewals = new RenewalCoordinator();
  private readonly lifetime = new AbortController();
  private running = false;
  private state: LiveStatus["state"] = "idle";
  private committed: string | null;
  private chain = Promise.resolve();
  private queued = 0;
  private queueBytes = 0;
  private readonly seen = new Map<string, Seen>();
  private readonly seenCursors = new Map<string, string>();
  private seenBytes = 0;
  private deliveryEpoch = 0;
  private snapshotOwner: number | null = null;
  private replacement = false;
  private started = false;
  private resolveStart: (() => void) | null = null;
  private rejectStart: ((error: Error) => void) | null = null;
  constructor(private readonly options: LiveClientOptions) {
    const url = new URL(options.endpoint);
    if (
      url.search ||
      url.hash ||
      url.username ||
      url.password ||
      url.pathname !== "/v2/live" ||
      !(
        url.protocol === "wss:" ||
        (url.protocol === "ws:" &&
          ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname))
      )
    )
      throw Error("LIVE_ENDPOINT_INVALID");
    if (
      options.reconnectBaseMs !== undefined &&
      (!Number.isInteger(options.reconnectBaseMs) ||
        options.reconnectBaseMs < 10 ||
        options.reconnectBaseMs > 1000)
    )
      throw Error("RECONNECT_BOUND_INVALID");
    this.filter = subscription.parse(options.subscription ?? {});
    this.committed =
      options.initialCursor === undefined
        ? null
        : cursor.parse(options.initialCursor);
    this.validator = new PinnedFrameValidator();
    this.lanes = [0, 1].map((index) => ({
      index,
      socket: null,
      abort: new AbortController(),
      ready: false,
      attempt: 0,
      resume: null,
      received: null,
      expires: Infinity,
      lastFrame: 0,
      timers: new Set(),
      snapshot: null,
      deadline: null,
      renewTimer: null,
      renewalCount: 0,
      awaitingAuth: false,
      replay: null,
      opening: false,
    }));
  }
  status(): LiveStatus {
    return {
      state: this.state,
      readyLanes: this.lanes.filter((l) => l.ready).length,
      cursor: this.committed,
    };
  }
  start(): Promise<void> {
    if (this.state === "stopped") throw Error("CLIENT_STOPPED");
    if (this.started) throw Error("CLIENT_ALREADY_STARTED");
    this.started = true;
    this.running = true;
    this.state = "connecting";
    this.notify();
    const ready = new Promise<void>((resolve, reject) => {
      this.resolveStart = resolve;
      this.rejectStart = reject;
    });
    void this.open(this.lanes[0]!);
    if (this.committed !== null) void this.open(this.lanes[1]!);
    return ready;
  }
  async stop(): Promise<void> {
    if (this.state === "stopped") return;
    this.running = false;
    this.state = "stopped";
    this.lifetime.abort();
    this.rejectStart?.(Error("CLIENT_STOPPED"));
    this.rejectStart = null;
    this.resolveStart = null;
    for (const lane of this.lanes) this.dispose(lane);
    this.seen.clear();
    this.seenCursors.clear();
    this.seenBytes = 0;
    this.notify();
  }
  private diagnostic(code: string, lane?: Lane, closeCode?: number) {
    try {
      this.options.onDiagnostic?.({
        code,
        ...(lane ? { lane: lane.index } : {}),
        ...(closeCode !== undefined ? { closeCode } : {}),
      });
    } catch {
      /* Observers cannot take down delivery. */
    }
  }
  private notify() {
    try {
      this.options.onStatus?.(this.status());
    } catch {
      this.diagnostic("STATUS_OBSERVER_FAILED");
    }
  }
  private fail(code: string) {
    if (!this.running) return;
    this.diagnostic(code);
    this.running = false;
    this.state = "failed";
    this.lifetime.abort();
    for (const lane of this.lanes) this.dispose(lane);
    this.rejectStart?.(Error(code));
    this.rejectStart = null;
    this.resolveStart = null;
    this.notify();
  }
  private timer(lane: Lane, ms: number, action: () => void) {
    if (!this.running) return null;
    const t = setTimeout(() => {
      lane.timers.delete(t);
      if (this.running) action();
    }, ms);
    lane.timers.add(t);
    return t;
  }
  private dispose(lane: Lane) {
    lane.opening = false;
    lane.abort.abort();
    for (const t of lane.timers) clearTimeout(t);
    lane.timers.clear();
    lane.ready = false;
    lane.snapshot = null;
    lane.deadline = null;
    lane.renewTimer = null;
    const ws = lane.socket;
    lane.socket = null;
    this.renewals.closed(lane.index);
    if (ws && ws.readyState < 2) ws.close(1000, "CLIENT_STOP");
  }
  private reconnect(lane: Lane, code: string, closeCode?: number) {
    if (!this.running) return;
    this.diagnostic(code, lane, closeCode);
    this.dispose(lane);
    if (this.snapshotOwner === lane.index) {
      this.snapshotOwner = null;
      this.replacement = false;
    }
    this.state = this.lanes.some((l) => l.ready) ? "live" : "recovering";
    this.notify();
    const wait =
      Math.min(
        30000,
        (this.options.reconnectBaseMs ?? 1000) *
          2 ** Math.min(lane.attempt++, 5),
      ) + Math.floor(Math.random() * 250);
    this.timer(lane, wait, () => {
      void this.open(lane);
    });
  }
  private async getTicket(lane: Lane): Promise<LiveTicket> {
    for (let attempt = 0; attempt < 3; attempt++) {
      lane.abort.signal.throwIfAborted();
      const request = new AbortController(),
        abort = () => request.abort();
      lane.abort.signal.addEventListener("abort", abort, { once: true });
      let timeout: ReturnType<typeof setTimeout> | undefined;
      try {
        const raw = await Promise.race([
          this.options.ticket(request.signal),
          new Promise<never>((_, reject) => {
            timeout = setTimeout(() => {
              request.abort();
              reject(Error("TICKET_TIMEOUT"));
            }, 5000);
            request.signal.addEventListener(
              "abort",
              () => reject(Error("TICKET_ABORTED")),
              { once: true },
            );
          }),
        ]);
        const result = ticketSchema.parse(raw),
          expiry = Date.parse(result.expires_at);
        if (
          result.endpoint !== this.options.endpoint ||
          !Number.isFinite(expiry) ||
          expiry <= Date.now() + 5000
        )
          throw Error("TICKET_INVALID");
        return result;
      } catch (error) {
        if (lane.abort.signal.aborted) throw Error("TICKET_ABORTED");
        const code =
          error instanceof Error && /^HTTP_\d{3}$/.test(error.message)
            ? error.message
            : error instanceof TypeError
              ? "NETWORK"
              : error instanceof Error &&
                  /^TICKET_(TIMEOUT|ABORTED)$/.test(error.message)
                ? "TIMEOUT"
                : "TICKET_INVALID";
        if (
          !/^(HTTP_5\d\d|HTTP_408|NETWORK|TIMEOUT)$/.test(code) ||
          attempt === 2
        )
          throw Error(code);
        if (Date.now() + 6000 >= lane.expires)
          throw Error("TICKET_RENEWAL_DEADLINE");
        this.diagnostic("TICKET_RETRY", lane);
        await new Promise<void>((resolve, reject) => {
          const t = setTimeout(resolve, 500 * (attempt + 1)),
            cancel = () => {
              clearTimeout(t);
              reject(Error("TICKET_ABORTED"));
            };
          lane.abort.signal.addEventListener("abort", cancel, { once: true });
          setTimeout(
            () => lane.abort.signal.removeEventListener("abort", cancel),
            500 * (attempt + 1),
          );
        });
      } finally {
        if (timeout !== undefined) clearTimeout(timeout);
        lane.abort.signal.removeEventListener("abort", abort);
      }
    }
    throw Error("TICKET_INVALID");
  }
  private async open(lane: Lane) {
    if (!this.running || lane.opening || lane.socket) return;
    lane.opening = true;
    lane.abort = new AbortController();
    lane.received = null;
    lane.expires = Infinity;
    lane.renewalCount = 0;
    lane.awaitingAuth = false;
    lane.replay = null;
    const signal = lane.abort.signal;
    try {
      const ticket = await this.getTicket(lane);
      if (!this.running || signal.aborted) return;
      const ws = (this.options.connect ?? ((url) => new WebSocket(url)))(
        ticket.endpoint,
      );
      lane.socket = ws;
      lane.lastFrame = Date.now();
      const current = () =>
        this.running && lane.socket === ws && !signal.aborted;
      lane.deadline = this.timer(lane, 5000, () => {
        if (!lane.ready) this.reconnect(lane, "STARTUP_TIMEOUT");
      });
      ws.addEventListener("open", () => {
        if (current()) this.authenticate(lane, ticket);
      });
      ws.addEventListener("error", () => {
        if (current()) this.diagnostic("TRANSPORT_ERROR", lane);
      });
      ws.addEventListener("close", (event) => {
        if (current()) {
          if (event.code === 1008 && event.reason !== "TICKET_EXPIRED") {
            this.diagnostic("PHYSICAL_CLOSE", lane, event.code);
            this.fail("AUTHORIZATION_DENIED");
          } else this.reconnect(lane, "PHYSICAL_CLOSE", event.code);
        }
      });
      ws.addEventListener("message", (event) => {
        if (!current()) return;
        try {
          if (typeof event.data !== "string" || bytes(event.data) > 1048576)
            throw Error("FRAME_BOUND_INVALID");
          const frame = this.validator.check(
            JSON.parse(event.data),
            this.options.includePredictions,
          );
          lane.lastFrame = Date.now();
          if (frame.type === "authenticated") {
            if (!lane.awaitingAuth) throw Error("UNEXPECTED_AUTHENTICATED");
            lane.awaitingAuth = false;
            if (
              frame.include_predictions !== this.options.includePredictions ||
              typeof frame.resume_token !== "string"
            )
              throw Error("CAPABILITY_CHANGED");
            const expiry = Date.parse(String(frame.expires_at));
            if (!Number.isFinite(expiry) || expiry <= Date.now())
              throw Error("AUTHENTICATION_EXPIRED");
            lane.resume = frame.resume_token;
            lane.expires = expiry;
            lane.received = this.committed;
            this.send(lane, {
              type: "subscribe",
              ...this.filter,
              ...(this.committed !== null ? { after: this.committed } : {}),
            });
          }
          if (frame.type === "replay.started") {
            if (
              lane.replay !== null ||
              lane.received === null ||
              frame.after !== lane.received ||
              BigInt(String(frame.cursor)) < BigInt(String(frame.after))
            )
              throw Error("REPLAY_ORDER");
            lane.replay = String(frame.cursor);
          }
          if (frame.type === "replay.completed") {
            if (
              lane.replay !== frame.cursor ||
              lane.received === null ||
              BigInt(String(frame.cursor)) < BigInt(lane.received)
            )
              throw Error("REPLAY_ORDER");
            lane.received = String(frame.cursor);
            lane.replay = null;
          }
          if (frame.type === "snapshot" || frame.type === "snapshot.completed")
            lane.received = String(frame.cursor);
          if (
            frame.type === "heartbeat" &&
            lane.received !== null &&
            BigInt(String(frame.cursor)) < BigInt(lane.received)
          )
            throw Error("CURSOR_REGRESSION");
          if (typeof frame.event_id === "string") {
            const c = String(frame.cursor);
            if (lane.received !== null && BigInt(c) <= BigInt(lane.received))
              throw Error("LANE_EVENT_ORDER");
            lane.received = c;
          }
          if (frame.type === "resync_required") this.beginResync(lane);
          this.enqueue(lane, ws, frame, bytes(event.data));
        } catch (error) {
          this.fail(
            error instanceof Error && /^[A-Z_]+$/.test(error.message)
              ? error.message
              : "FRAME_INVALID",
          );
        }
      });
      const idle = () => {
        if (!current()) return;
        if (Date.now() - lane.lastFrame >= 15000) {
          this.reconnect(lane, "IDLE_TIMEOUT");
          return;
        }
        this.timer(lane, 5000, idle);
      };
      this.timer(lane, 5000, idle);
    } catch (error) {
      if (!this.running || signal.aborted) return;
      const code = error instanceof Error ? error.message : "CONNECTION_FAILED";
      if (
        (/^HTTP_4\d\d$/.test(code) && code !== "HTTP_408") ||
        code === "TICKET_INVALID"
      )
        this.fail("TICKET_REJECTED");
      else this.reconnect(lane, "CONNECT_RETRY");
    } finally {
      if (lane.abort.signal === signal) lane.opening = false;
    }
  }
  private authenticate(lane: Lane, ticket: LiveTicket) {
    lane.awaitingAuth = true;
    lane.replay = null;
    this.send(lane, {
      type: "authenticate",
      ticket: ticket.ticket,
      resumable: true,
      ...(lane.resume ? { resume_token: lane.resume } : {}),
    });
  }
  private send(lane: Lane, value: Record<string, unknown>) {
    if (lane.socket?.readyState !== 1) throw Error("TRANSPORT_NOT_OPEN");
    lane.socket.send(JSON.stringify(value));
  }
  private beginResync(lane: Lane) {
    this.deliveryEpoch++;
    this.diagnostic("RESYNC_REQUIRED", lane);
    this.state = "resyncing";
    this.snapshotOwner = lane.index;
    this.replacement = true;
    lane.ready = false;
    this.renewals.closed(lane.index);
    for (const peer of this.lanes) if (peer !== lane) this.dispose(peer);
    this.seen.clear();
    this.seenCursors.clear();
    this.seenBytes = 0;
    this.notify();
  }
  private enqueue(
    lane: Lane,
    ws: WebSocket,
    frame: Record<string, unknown>,
    size: number,
  ) {
    const epoch = this.deliveryEpoch;
    if (typeof frame.event_id === "string") {
      const key = frame.event_id,
        c = String(frame.cursor),
        wire = fingerprint(frame),
        old = this.seen.get(key),
        oldId = this.seenCursors.get(c);
      if (
        (old && (old.cursor !== c || old.wire !== wire)) ||
        (oldId && oldId !== key)
      )
        throw Error("EVENT_IDENTITY_CONFLICT");
      if (
        old ||
        (this.committed !== null && BigInt(c) <= BigInt(this.committed))
      )
        return;
      this.seen.set(key, {
        cursor: c,
        wire,
        bytes: bytes(wire),
        pending: true,
      });
      this.seenCursors.set(c, key);
      this.seenBytes += bytes(wire);
    }
    if (++this.queued > 256 || (this.queueBytes += size) > 16777216)
      throw Error("CONSUMER_BACKPRESSURE");
    this.chain = this.chain
      .then(async () => {
        if (!this.running || epoch !== this.deliveryEpoch) return;
        // Event payloads already admitted by either lane remain valid after that transport closes.
        if (typeof frame.event_id !== "string" && lane.socket !== ws) return;
        try {
          await this.consume(lane, frame, epoch);
          this.trimSeen();
        } catch (error) {
          this.fail(
            error instanceof Error && /^[A-Z_]+$/.test(error.message)
              ? error.message
              : "CONSUMER_FAILED",
          );
        }
      })
      .finally(() => {
        this.queued--;
        this.queueBytes -= size;
      });
  }
  private async checkpoint(c: string) {
    const epoch = this.deliveryEpoch;
    if (this.committed !== null && BigInt(c) <= BigInt(this.committed)) return;
    await this.consumer(() =>
      this.options.onCheckpoint?.(c, this.lifetime.signal),
    );
    if (this.running && epoch === this.deliveryEpoch) this.committed = c;
  }
  private async consumer(callback: () => void | Promise<void>) {
    try {
      await callback();
    } catch {
      throw Error("CONSUMER_FAILED");
    }
  }
  private async consume(
    lane: Lane,
    frame: Record<string, unknown>,
    epoch: number,
  ) {
    const type = String(frame.type),
      c = String(frame.cursor);
    if (typeof frame.event_id === "string") {
      if (this.committed === null) throw Error("EVENT_BEFORE_BASELINE");
      if (BigInt(c) > BigInt(this.committed)) {
        await this.consumer(() =>
          this.options.onEvent(frame, this.lifetime.signal),
        );
        if (this.running && epoch === this.deliveryEpoch) this.committed = c;
      }
      const entry = this.seen.get(frame.event_id);
      if (entry) entry.pending = false;
      return;
    }
    if (type === "resync_required") return;
    if (type === "snapshot.started") {
      if (this.snapshotOwner !== null && this.snapshotOwner !== lane.index)
        throw Error("CONCURRENT_SNAPSHOT");
      this.snapshotOwner = lane.index;
      lane.snapshot = {
        cursor: c,
        count: Number(frame.match_count),
        index: 0,
        matches: [],
        ids: new Set(),
        bytes: 0,
      };
      return;
    }
    if (type === "snapshot.chunk") {
      const s = lane.snapshot;
      if (!s || s.cursor !== c || s.index++ !== frame.index)
        throw Error("SNAPSHOT_ORDER");
      for (const item of frame.matches as LiveFrame[]) {
        const id = String(item.match_id);
        if (s.ids.has(id)) throw Error("SNAPSHOT_DUPLICATE");
        s.ids.add(id);
        s.matches.push(item);
        s.bytes += bytes(JSON.stringify(item));
      }
      if (s.matches.length > s.count || s.bytes > 16777216)
        throw Error("SNAPSHOT_BOUND_INVALID");
      return;
    }
    if (type === "snapshot.completed" || type === "snapshot") {
      const s = lane.snapshot;
      if (
        type === "snapshot.completed" &&
        (!s || s.cursor !== c || s.count !== s.matches.length)
      )
        throw Error("SNAPSHOT_INCOMPLETE");
      if (this.committed !== null && !this.replacement)
        throw Error("UNEXPECTED_SNAPSHOT");
      if (this.committed !== null && BigInt(c) < BigInt(this.committed))
        throw Error("SNAPSHOT_REGRESSION");
      const matches =
        type === "snapshot" ? (frame.matches as LiveFrame[]) : s!.matches;
      if (new Set(matches.map((v) => v.match_id)).size !== matches.length)
        throw Error("SNAPSHOT_DUPLICATE");
      await this.consumer(() =>
        this.options.onSnapshot(
          { cursor: c, matches, replacement: this.replacement },
          this.lifetime.signal,
        ),
      );
      if (!this.running) return;
      this.committed = c;
      this.snapshotOwner = null;
      this.replacement = false;
      lane.snapshot = null;
      this.seen.clear();
      this.seenCursors.clear();
      this.seenBytes = 0;
      this.ready(lane);
      return;
    }
    if (type === "replay.completed") {
      await this.checkpoint(c);
      this.ready(lane);
      return;
    }
    if (type === "heartbeat") {
      if (this.lanes.find((l) => l.ready)?.index === lane.index)
        await this.consumer(() =>
          this.options.onHeartbeat?.(frame, this.lifetime.signal),
        );
      return;
    }
  }
  private ready(lane: Lane) {
    if (!this.running || !lane.socket) return;
    if (lane.deadline !== null) {
      clearTimeout(lane.deadline);
      lane.timers.delete(lane.deadline);
      lane.deadline = null;
    }
    if (lane.renewTimer !== null) {
      clearTimeout(lane.renewTimer);
      lane.timers.delete(lane.renewTimer);
    }
    lane.ready = true;
    lane.attempt = 0;
    this.renewals.ready(lane.index);
    this.state = "live";
    this.resolveStart?.();
    this.resolveStart = null;
    this.rejectStart = null;
    this.notify();
    const peer = this.lanes[1 - lane.index]!;
    if (!peer.socket && !peer.opening && peer.timers.size === 0)
      void this.open(peer);
    lane.renewTimer = this.timer(
      lane,
      lane.index === 1 && lane.renewalCount === 0 ? 15000 : 30000,
      () => {
        lane.renewTimer = null;
        lane.renewalCount++;
        void this.renew(lane);
      },
    );
  }
  private async renew(lane: Lane) {
    if (!this.running || !lane.ready) return;
    try {
      await this.renewals.acquire(lane.index, lane.expires, lane.abort.signal);
      const ticket = await this.getTicket(lane);
      if (!this.running || lane.abort.signal.aborted) return;
      lane.ready = false;
      this.notify();
      this.authenticate(lane, ticket);
      lane.deadline = this.timer(lane, 5000, () => {
        if (!lane.ready) this.reconnect(lane, "RENEWAL_TIMEOUT");
      });
    } catch {
      if (this.running && !lane.abort.signal.aborted)
        this.reconnect(lane, "RENEWAL_RETRY");
    }
  }
  private trimSeen() {
    for (const [id, s] of this.seen) {
      if (this.seen.size <= 16384 && this.seenBytes <= 16777216) break;
      if (s.pending) continue;
      this.seen.delete(id);
      this.seenCursors.delete(s.cursor);
      this.seenBytes -= s.bytes;
    }
    if (this.seenBytes > 16777216) throw Error("CONSUMER_BACKPRESSURE");
  }
}
