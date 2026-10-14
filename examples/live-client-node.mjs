// Node.js 24. The private API key stays in this server process.
import { ResilientLiveClient } from "../dist/dft-live-client.mjs";
const key = process.env.DFT_API_KEY;
if (!key) throw Error("DFT_API_KEY_REQUIRED");
const client = new ResilientLiveClient({
  endpoint: "wss://api.datafortennis.com/v2/live",
  includePredictions: false,
  subscription: { include_point_events: true },
  ticket: async (signal) => {
    const response = await fetch(
      "https://api.datafortennis.com/v2/live/stream-tickets",
      {
        method: "POST",
        signal,
        headers: {
          authorization: "Bearer " + key,
          "content-type": "application/json",
          "user-agent": "DataForTennisReferenceClient/0.1",
        },
        body: JSON.stringify({ include_predictions: false }),
      },
    );
    if (!response.ok) {
      await response.body?.cancel();
      throw Error("HTTP_" + response.status);
    }
    const text = await response.text();
    if (new TextEncoder().encode(text).byteLength > 16384)
      throw Error("TICKET_RESPONSE_BOUND");
    return JSON.parse(text).data;
  },
  onSnapshot: (snapshot) =>
    console.log(
      JSON.stringify({
        type: "snapshot.applied",
        cursor: snapshot.cursor,
        matches: snapshot.matches.length,
        replacement: snapshot.replacement,
      }),
    ),
  onEvent: (event) =>
    console.log(
      JSON.stringify({
        type: "event",
        event_id: event.event_id,
        cursor: event.cursor,
      }),
    ),
  onStatus: (status) =>
    console.log(JSON.stringify({ type: "client.status", ...status })),
  onDiagnostic: (diagnostic) =>
    console.error(JSON.stringify({ type: "client.diagnostic", ...diagnostic })),
});
// These callbacks are an in-memory example, not a durable application transaction.
for (const signal of ["SIGINT", "SIGTERM"])
  process.once(signal, () => {
    void client.stop();
  });
await client.start();
