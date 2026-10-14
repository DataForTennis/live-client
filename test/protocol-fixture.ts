import { createServer } from "node:http";
import { createPublicKey, verify } from "node:crypto";
import { WebSocketServer } from "ws";

/** Synthetic public-protocol fixture, not a gateway implementation. No provider or account access. */
export async function startProtocolFixture(
  source: {
    bootstrap: () => Promise<{
      cursor: string;
      matches: Record<string, unknown>[];
    }>;
    events: (
      after: string,
    ) => Promise<{ cursor: string; events: Record<string, unknown>[] }>;
  },
  publicKeyPem: string,
) {
  const server = createServer();
  const wss = new WebSocketServer({
    server,
    maxPayload: 1048576,
    perMessageDeflate: false,
  });
  const states = new Map<
    import("ws").WebSocket,
    {
      authenticated: boolean;
      subscribed: boolean;
      cursor: string;
      expires: number;
      heartbeatAt: number;
    }
  >();
  const send = (ws: import("ws").WebSocket, value: unknown) => {
    if (ws.readyState === 1) ws.send(JSON.stringify(value));
  };
  const key = createPublicKey(publicKeyPem);
  let polling = false;
  wss.on("connection", (ws) => {
    const state = {
      authenticated: false,
      subscribed: false,
      cursor: "0",
      expires: 0,
      heartbeatAt: Date.now(),
    };
    states.set(ws, state);
    ws.on("close", () => states.delete(ws));
    ws.on("message", (raw) => {
      void (async () => {
        const command = JSON.parse(raw.toString());
        if (command.type === "authenticate") {
          const parts = String(command.ticket).split(".");
          const claims = JSON.parse(
            Buffer.from(parts[1] ?? "", "base64url").toString(),
          );
          if (
            parts.length !== 3 ||
            !verify(
              null,
              Buffer.from(parts[0] + "." + parts[1]),
              key,
              Buffer.from(parts[2]!, "base64url"),
            ) ||
            claims.exp * 1000 <= Date.now()
          ) {
            ws.close(1008, "TICKET_INVALID");
            return;
          }
          state.authenticated = true;
          state.subscribed = false;
          state.expires = claims.exp * 1000;
          send(ws, {
            schema_version: "2.0",
            type: "authenticated",
            include_predictions: false,
            expires_at: new Date(state.expires).toISOString(),
            resume_token: "a".repeat(64),
          });
        } else if (command.type === "subscribe" && state.authenticated) {
          if (command.after === undefined) {
            const baseline = await source.bootstrap();
            state.cursor = baseline.cursor;
            send(ws, {
              schema_version: "2.0",
              type: "snapshot",
              cursor: state.cursor,
              matches: baseline.matches.map((match) => ({
                match_id: match.match_id,
                score_revision: match.score_revision,
                match,
                last_point: null,
              })),
            });
          } else {
            const result = await source.events(command.after);
            send(ws, {
              schema_version: "2.0",
              type: "replay.started",
              after: command.after,
              cursor: result.cursor,
            });
            for (const event of result.events)
              send(ws, {
                ...event,
                state: {
                  match_id: event.match_id,
                  score_revision: "1",
                  match: event.data,
                  last_point: null,
                },
              });
            state.cursor = result.cursor;
            send(ws, {
              schema_version: "2.0",
              type: "replay.completed",
              cursor: state.cursor,
            });
          }
          state.subscribed = true;
        }
      })().catch(() => ws.close(1008, "FIXTURE_COMMAND_INVALID"));
    });
  });
  const timer = setInterval(() => {
    void (async () => {
      if (polling) return;
      polling = true;
      try {
        for (const [ws, state] of states) {
          if (state.authenticated && Date.now() >= state.expires) {
            state.subscribed = false;
            ws.close(1008, "TICKET_EXPIRED");
          }
          if (!state.subscribed || ws.readyState !== 1) continue;
          const result = await source.events(state.cursor);
          for (const event of result.events)
            send(ws, {
              ...event,
              state: {
                match_id: event.match_id,
                score_revision: "1",
                match: event.data,
                last_point: null,
              },
            });
          state.cursor = result.cursor;
          if (Date.now() - state.heartbeatAt >= 10000) {
            send(ws, {
              schema_version: "2.0",
              type: "heartbeat",
              cursor: state.cursor,
              server_time: new Date().toISOString(),
              matches: [],
            });
            state.heartbeatAt = Date.now();
          }
        }
      } finally {
        polling = false;
      }
    })();
  }, 10);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    server,
    metrics: () => ({
      authenticated_connections: [...states.values()].filter(
        (state) => state.authenticated,
      ).length,
    }),
    close: async () => {
      clearInterval(timer);
      for (const ws of wss.clients) ws.terminate();
      await new Promise<void>((resolve) => wss.close(() => resolve()));
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
