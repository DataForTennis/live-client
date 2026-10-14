import { expect, it } from "vitest";
import { readFile } from "node:fs/promises";
import { generateKeyPairSync, sign } from "node:crypto";
import { WebSocket, WebSocketServer } from "ws";
import { createServer } from "node:http";
import { startProtocolFixture } from "./protocol-fixture.js";
type CanonicalEvent = Record<string, unknown> & { cursor: string };
import {
  ResilientLiveClient,
  type LiveClientOptions,
} from "../src/client/resilient-live-client.js";

const contract = JSON.parse(
  await readFile(
    new URL("./fixtures/canonical-live-v2.asyncapi.json", import.meta.url),
    "utf8",
  ),
);
const id = "00000000-0000-4000-8000-000000000001",
  m = "mat_" + id,
  clock = "2026-10-14T00:00:00.000Z";
const match = {
  id: m,
  match_id: m,
  player_ids: ["ply_" + id, "ply_00000000-0000-4000-8000-000000000002"],
  player_names: ["First", "Second"],
  tournament_id: "trn_" + id,
  edition_id: "edt_" + id,
  draw_id: "drw_" + id,
  tournament_name: "Fixture",
  tour: "atp",
  gender: "men",
  discipline: "singles",
  category: "500",
  prediction_eligible: true,
  best_of: 3,
  round: null,
  scheduled_start: null,
  status: "live",
  interruption_reason: null,
  score_revision: "1",
  prediction_input_revision: "1",
  score: {
    sets: [{ set: 1, side1_games: 0, side2_games: 0 }],
    points: { side1: "0", side2: "0" },
    server_side: 1,
    points_kind: "normal",
  },
  score_observed_at: clock,
  source_last_observed_at: clock,
  score_received_at: clock,
  enrichment_deadline_at: clock,
  point_id: null,
  point_sequence: null,
  point_revision: null,
  statistics: [],
  availability: "observed",
};
async function until(predicate: () => boolean) {
  const end = Date.now() + 2000;
  while (!predicate() && Date.now() < end)
    await new Promise((r) => setTimeout(r, 10));
  expect(predicate()).toBe(true);
}
async function fixture(
  onEvent: (frame: Record<string, unknown>) => Promise<void> | void,
  initialCursor?: string,
  useNative = false,
) {
  const keys = generateKeyPairSync("ed25519"),
    events: CanonicalEvent[] = [],
    sockets: WebSocket[] = [],
    snapshots: string[] = [],
    diagnostics: string[] = [];
  const gateway = await startProtocolFixture(
    {
      bootstrap: async () => ({
        cursor: String(events.length),
        matches: [match],
        predictions: [],
      }),
      events: async (after) => ({
        cursor: String(events.length),
        events: events.filter((e) => BigInt(e.cursor) > BigInt(after)),
      }),
    },
    keys.publicKey.export({ type: "spki", format: "pem" }).toString(),
  );
  const endpoint = `ws://127.0.0.1:${(gateway.server.address() as { port: number }).port}/v2/live`;
  const client = new ResilientLiveClient({
    endpoint,
    includePredictions: false,
    ...(initialCursor !== undefined ? { initialCursor } : {}),
    reconnectBaseMs: 10,
    ...(useNative
      ? {}
      : {
          connect: (url: string) => {
            const socket = new WebSocket(url, {
              maxPayload: 1048576,
              perMessageDeflate: false,
            });
            sockets.push(socket);
            return socket as unknown as globalThis.WebSocket;
          },
        }),
    ticket: async () => {
      const now = Math.floor(Date.now() / 1000),
        h = Buffer.from(
          JSON.stringify({ alg: "EdDSA", typ: "JWT", kid: "test" }),
        ).toString("base64url"),
        p = Buffer.from(
          JSON.stringify({
            iss: "datafortennis",
            aud: "dft-live/v2",
            sub: "resilient-client",
            jti: String(sockets.length),
            iat: now,
            exp: now + 60,
            channel: "private",
            credential_id: "key",
            match_ids: [],
            include_predictions: false,
            origin: null,
            max_connections: 2,
            max_matches: 32,
          }),
        ).toString("base64url");
      return {
        endpoint,
        version: "2.0" as const,
        ticket:
          h +
          "." +
          p +
          "." +
          sign(null, Buffer.from(h + "." + p), keys.privateKey).toString(
            "base64url",
          ),
        expires_at: new Date((now + 60) * 1000).toISOString(),
        renew_after_seconds: 30 as const,
      };
    },
    onSnapshot: (s) => {
      snapshots.push(s.cursor);
    },
    onEvent,
    onDiagnostic: (d) => diagnostics.push(d.code),
  });
  const publish = () => {
    const cursor = String(events.length + 1);
    events.push({
      schema_version: "2.0",
      cursor,
      event_id: "evt_00000000-0000-4000-8000-" + cursor.padStart(12, "0"),
      type: "match.metadata.updated",
      match_id: m,
      source_observed_at: clock,
      published_at: clock,
      data: { ...match, tournament_name: "Fixture " + cursor },
    });
    return cursor;
  };
  return {
    client,
    gateway,
    sockets,
    snapshots,
    diagnostics,
    publish,
    close: async () => {
      await client.stop();
      for (const ws of sockets) ws.terminate();
      await gateway.close();
    },
  };
}
it("delivers once from two real authenticated lanes and survives either physical close", async () => {
  const received: string[] = [],
    f = await fixture((e) => {
      received.push(String(e.cursor));
    });
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    expect(f.gateway.metrics().authenticated_connections).toBe(2);
    f.publish();
    await until(() => f.client.status().cursor === "1");
    expect(received).toEqual(["1"]);
    f.sockets[0]!.terminate();
    f.publish();
    await until(() => f.client.status().cursor === "2");
    await until(() => f.client.status().readyLanes === 2);
    f.sockets[1]!.terminate();
    f.publish();
    await until(() => f.client.status().cursor === "3");
    await until(() => f.client.status().readyLanes === 2);
    expect(received).toEqual(["1", "2", "3"]);
    expect(f.snapshots).toEqual(["0"]);
    expect(f.diagnostics.filter((c) => c === "PHYSICAL_CLOSE")).toHaveLength(2);
  } finally {
    await f.close();
  }
});
it("replays after both lanes close without resetting an acknowledged snapshot", async () => {
  const received: string[] = [],
    f = await fixture((e) => {
      received.push(String(e.cursor));
    });
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    f.publish();
    await until(() => f.client.status().cursor === "1");
    for (const ws of f.sockets) ws.terminate();
    f.publish();
    f.publish();
    await until(() => f.client.status().cursor === "3");
    await until(() => f.client.status().readyLanes === 2);
    expect(received).toEqual(["1", "2", "3"]);
    expect(f.snapshots).toEqual(["0"]);
  } finally {
    await f.close();
  }
});
it("acknowledges only after an asynchronous consumer succeeds and serializes delivery", async () => {
  let release!: () => void;
  const blocked = new Promise<void>((r) => {
      release = r;
    }),
    received: string[] = [];
  const f = await fixture(async (e) => {
    received.push(String(e.cursor));
    if (e.cursor === "1") await blocked;
  });
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    f.publish();
    await until(() => received.length === 1);
    f.publish();
    await new Promise((r) => setTimeout(r, 80));
    expect(f.client.status().cursor).toBe("0");
    expect(received).toEqual(["1"]);
    release();
    await until(() => f.client.status().cursor === "2");
    expect(received).toEqual(["1", "2"]);
  } finally {
    release();
    await f.close();
  }
});
it("fails closed on a consumer error and preserves the last committed cursor", async () => {
  const f = await fixture(() => {
    throw Error("application private details");
  });
  try {
    await f.client.start();
    f.publish();
    await until(() => f.client.status().state === "failed");
    expect(f.client.status().cursor).toBe("0");
    expect(f.diagnostics).toContain("CONSUMER_FAILED");
    expect(JSON.stringify(f.client.status())).not.toContain("private");
  } finally {
    await f.close();
  }
});
it("renews short tickets while the peer keeps delivery available", async () => {
  const received: string[] = [],
    f = await fixture((e) => {
      received.push(String(e.cursor));
    });
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    await new Promise((r) => setTimeout(r, 16500));
    f.publish();
    await until(() => f.client.status().cursor === "1");
    expect(f.client.status().state).toBe("live");
    expect(f.client.status().readyLanes).toBe(2);
    expect(received).toEqual(["1"]);
    expect(f.sockets).toHaveLength(2);
    expect(f.diagnostics).not.toContain("RENEWAL_RETRY");
    expect(f.diagnostics).not.toContain("AUTHORIZATION_DENIED");
  } finally {
    await f.close();
  }
}, 20000);

const frame = (cursor: string, eventId = cursor) => ({
  schema_version: "2.0",
  type: "match.metadata.updated",
  cursor,
  event_id: "evt_00000000-0000-4000-8000-" + eventId.padStart(12, "0"),
  match_id: m,
  published_at: clock,
  source_observed_at: clock,
  data: match,
  state: { match_id: m, score_revision: "1", match, last_point: null },
});
async function scripted(options: Partial<LiveClientOptions> = {}) {
  const server = createServer(),
    wss = new WebSocketServer({ server }),
    sockets: WebSocket[] = [],
    commands: Array<Record<string, unknown>> = [],
    received: string[] = [],
    snapshots: string[] = [],
    diagnostics: string[] = [];
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const endpoint = `ws://127.0.0.1:${(server.address() as { port: number }).port}/v2/live`;
  wss.on("connection", (ws) => {
    sockets.push(ws);
    ws.on("message", (raw) => {
      const v = JSON.parse(raw.toString());
      commands.push(v);
      if (v.type === "authenticate")
        ws.send(
          JSON.stringify({
            schema_version: "2.0",
            type: "authenticated",
            include_predictions: false,
            expires_at: new Date(Date.now() + 60000).toISOString(),
            resume_token: String(sockets.indexOf(ws)).repeat(64),
          }),
        );
      if (v.type === "subscribe") {
        if (v.after !== undefined) {
          ws.send(
            JSON.stringify({
              schema_version: "2.0",
              type: "replay.started",
              after: v.after,
              cursor: v.after,
            }),
          );
          ws.send(
            JSON.stringify({
              schema_version: "2.0",
              type: "replay.completed",
              cursor: v.after,
            }),
          );
        } else
          ws.send(
            JSON.stringify({
              schema_version: "2.0",
              type: "snapshot",
              cursor: "0",
              matches: [],
            }),
          );
      }
    });
  });
  const client = new ResilientLiveClient({
    endpoint,
    includePredictions: false,
    reconnectBaseMs: 10,
    connect: (url) => new WebSocket(url) as unknown as globalThis.WebSocket,
    ticket: async () => ({
      endpoint,
      version: "2.0",
      ticket: "fixture-only",
      expires_at: new Date(Date.now() + 60000).toISOString(),
      renew_after_seconds: 30,
    }),
    onEvent: (e) => {
      received.push(String(e.cursor));
    },
    onSnapshot: (s) => {
      snapshots.push(s.cursor);
    },
    onDiagnostic: (d) => diagnostics.push(d.code),
    ...options,
  });
  const send = (lane: number, v: Record<string, unknown>) =>
    sockets[lane]!.send(JSON.stringify(v));
  return {
    client,
    endpoint,
    sockets,
    commands,
    received,
    snapshots,
    diagnostics,
    send,
    close: async () => {
      await client.stop();
      for (const ws of sockets) ws.terminate();
      await new Promise<void>((r) => wss.close(() => r()));
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}
it("rejects conflicting copies of an event instead of silently deduplicating them", async () => {
  const f = await scripted();
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    f.send(0, frame("1"));
    await until(() => f.client.status().cursor === "1");
    f.send(1, {
      ...frame("1"),
      data: { ...match, tournament_name: "conflicting" },
    });
    await until(() => f.client.status().state === "failed");
    expect(f.diagnostics).toContain("EVENT_IDENTITY_CONFLICT");
    expect(f.received).toEqual(["1"]);
  } finally {
    await f.close();
  }
});
it("rejects a reused cursor with a different event ID", async () => {
  const f = await scripted();
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    f.send(0, frame("1"));
    await until(() => f.client.status().cursor === "1");
    f.send(1, frame("1", "2"));
    await until(() => f.client.status().state === "failed");
    expect(f.diagnostics).toContain("EVENT_IDENTITY_CONFLICT");
  } finally {
    await f.close();
  }
});
it("fails closed on authoritative schema drift without delivering the invalid frame", async () => {
  const f = await scripted();
  try {
    await f.client.start();
    f.send(0, { ...frame("1"), unknown_property: "unsafe" });
    await until(() => f.client.status().state === "failed");
    expect(f.received).toEqual([]);
    expect(f.diagnostics).toContain("SCHEMA_DRIFT");
  } finally {
    await f.close();
  }
});
it("does not assume consecutive cursors for filtered subscriptions or acknowledge heartbeat cursors", async () => {
  const f = await scripted({
    initialCursor: "5",
    subscription: { categories: ["500"] },
  });
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    expect(
      f.commands.filter((c) => c.type === "subscribe").map((c) => c.after),
    ).toEqual(["5", "5"]);
    f.send(0, frame("8"));
    await until(() => f.client.status().cursor === "8");
    f.send(0, {
      schema_version: "2.0",
      type: "heartbeat",
      cursor: "11",
      server_time: clock,
      matches: [],
    });
    await new Promise((r) => setTimeout(r, 30));
    expect(f.client.status().cursor).toBe("8");
    f.send(0, frame("12"));
    await until(() => f.client.status().cursor === "12");
    expect(f.received).toEqual(["8", "12"]);
    expect(f.snapshots).toEqual([]);
  } finally {
    await f.close();
  }
});
it("resynchronizes atomically after replay expiry while a consumer is still pending", async () => {
  let release!: () => void;
  const pending = new Promise<void>((r) => {
      release = r;
    }),
    applied: string[] = [],
    replacement: boolean[] = [];
  const f = await scripted({
    onEvent: async (e) => {
      applied.push(String(e.cursor));
      if (e.cursor === "1") await pending;
    },
    onSnapshot: (s) => {
      replacement.push(s.replacement);
    },
  });
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    f.send(0, frame("1"));
    await until(() => applied.length === 1);
    f.send(1, {
      schema_version: "2.0",
      type: "resync_required",
      reason: "DURABLE_CURSOR_EXPIRED",
      cursor: "10",
    });
    f.send(1, {
      schema_version: "2.0",
      type: "snapshot.started",
      cursor: "10",
      match_count: 1,
    });
    f.send(1, {
      schema_version: "2.0",
      type: "snapshot.chunk",
      cursor: "10",
      index: 0,
      matches: [{ match_id: m, score_revision: "1", match, last_point: null }],
    });
    f.send(1, {
      schema_version: "2.0",
      type: "snapshot.completed",
      cursor: "10",
    });
    f.send(0, frame("11"));
    await new Promise((r) => setTimeout(r, 80));
    expect(f.client.status().state).toBe("resyncing");
    expect(replacement).toEqual([false]);
    release();
    await until(() => f.client.status().cursor === "10");
    await until(() => f.client.status().readyLanes === 2);
    expect(replacement).toEqual([false, true]);
    expect(applied).toEqual(["1"]);
    f.send(1, frame("12"));
    await until(() => f.client.status().cursor === "12");
    expect(applied).toEqual(["1", "12"]);
  } finally {
    release();
    await f.close();
  }
});
it("never applies a partial replacement snapshot", async () => {
  const f = await scripted();
  try {
    await f.client.start();
    f.send(0, {
      schema_version: "2.0",
      type: "resync_required",
      reason: "CURSOR_NOT_IN_HOT_WINDOW",
      cursor: "10",
    });
    f.send(0, {
      schema_version: "2.0",
      type: "snapshot.started",
      cursor: "10",
      match_count: 2,
    });
    f.send(0, {
      schema_version: "2.0",
      type: "snapshot.chunk",
      cursor: "10",
      index: 0,
      matches: [{ match_id: m, score_revision: "1", match, last_point: null }],
    });
    f.send(0, {
      schema_version: "2.0",
      type: "snapshot.completed",
      cursor: "10",
    });
    await until(() => f.client.status().state === "failed");
    expect(f.diagnostics).toContain("SNAPSHOT_INCOMPLETE");
    expect(f.snapshots).toEqual(["0"]);
    expect(f.client.status().cursor).toBe("0");
  } finally {
    await f.close();
  }
});
it("bounds pending delivery and preserves the unacknowledged cursor under a slow consumer", async () => {
  let release!: () => void;
  const pending = new Promise<void>((r) => {
    release = r;
  });
  const f = await scripted({ onEvent: () => pending });
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    f.send(0, frame("1"));
    await new Promise((r) => setTimeout(r, 20));
    for (let n = 2; n <= 300; n++) f.send(0, frame(String(n)));
    await until(() => f.client.status().state === "failed");
    expect(f.diagnostics).toContain("CONSUMER_BACKPRESSURE");
    expect(f.client.status().cursor).toBe("0");
  } finally {
    release();
    await f.close();
  }
});
it("stops both connections and cancels retries and further callbacks", async () => {
  const f = await scripted();
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    const count = f.sockets.length;
    for (const ws of f.sockets) ws.terminate();
    await f.client.stop();
    await new Promise((r) => setTimeout(r, 400));
    expect(f.sockets).toHaveLength(count);
    expect(f.received).toEqual([]);
    expect(f.client.status().state).toBe("stopped");
  } finally {
    await f.close();
  }
});
it("commits a filtered replay checkpoint only after the application stores it", async () => {
  let release!: () => void;
  const pending = new Promise<void>((r) => {
      release = r;
    }),
    checkpoints: string[] = [];
  const f = await scripted({
    initialCursor: "5",
    onCheckpoint: async (c) => {
      checkpoints.push(c);
      await pending;
    },
  });
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    f.send(0, {
      schema_version: "2.0",
      type: "replay.started",
      after: "5",
      cursor: "10",
    });
    f.send(0, {
      schema_version: "2.0",
      type: "replay.completed",
      cursor: "10",
    });
    await until(() => checkpoints.length === 1);
    expect(f.client.status().cursor).toBe("5");
    release();
    await until(() => f.client.status().cursor === "10");
    expect(checkpoints).toEqual(["10"]);
  } finally {
    release();
    await f.close();
  }
});
it("rejects a replay completion that was not bracketed by a matching replay start", async () => {
  const f = await scripted();
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    f.send(0, {
      schema_version: "2.0",
      type: "replay.completed",
      cursor: "100",
    });
    await until(() => f.client.status().state === "failed");
    expect(f.client.status().cursor).toBe("0");
    expect(f.diagnostics).toContain("REPLAY_ORDER");
  } finally {
    await f.close();
  }
});
it("opens at most one replacement attempt per lane while a peer ticket is pending", async () => {
  let endpoint = "";
  let calls = 0;
  const f = await scripted({
    ticket: async () => {
      if (++calls > 1) await new Promise((r) => setTimeout(r, 100));
      return {
        endpoint,
        version: "2.0",
        ticket: "short-fixture",
        expires_at: new Date(Date.now() + 60000).toISOString(),
        renew_after_seconds: 30,
      };
    },
  });
  endpoint = f.endpoint;
  try {
    await f.client.start();
    f.send(0, {
      schema_version: "2.0",
      type: "replay.started",
      after: "0",
      cursor: "0",
    });
    f.send(0, { schema_version: "2.0", type: "replay.completed", cursor: "0" });
    await new Promise((r) => setTimeout(r, 250));
    expect(f.sockets).toHaveLength(2);
    expect(f.client.status().readyLanes).toBe(2);
  } finally {
    await f.close();
  }
});
it("uses the Node.js native WebSocket with actual signed tickets and the same merged delivery", async () => {
  const received: string[] = [],
    f = await fixture(
      (e) => {
        received.push(String(e.cursor));
      },
      undefined,
      true,
    );
  try {
    await f.client.start();
    await until(() => f.client.status().readyLanes === 2);
    f.publish();
    await until(() => f.client.status().cursor === "1");
    expect(received).toEqual(["1"]);
    expect(f.gateway.metrics().authenticated_connections).toBe(2);
  } finally {
    await f.close();
  }
});
it("does not start a client whose lifetime has already been stopped", async () => {
  const f = await scripted();
  try {
    await f.client.stop();
    expect(() => f.client.start()).toThrow("CLIENT_STOPPED");
    expect(f.sockets).toEqual([]);
  } finally {
    await f.close();
  }
});
