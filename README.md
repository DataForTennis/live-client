# DataForTennis Live Client

[Documentation](https://docs.datafortennis.com/start-here/live-web-socket-quickstart#automatic-failover-reference-client) | [Releases](https://github.com/DataForTennis/live-client/releases) | [CI](https://github.com/DataForTennis/live-client/actions/workflows/ci.yml)

This TypeScript client maintains two identical subscriptions to the existing Live V2 gateway. One ordered application callback stream merges them. A physical close is still reported; the ready peer continues delivery. Both connections share the last successfully processed cursor. Reconnection uses a fresh 60-second ticket and lane-specific resumable session token, then requests replay from that cursor. Retries use exponential backoff with jitter, capped at 30 seconds plus up to 249 ms of jitter.

This is a standalone reference client, distributed as browser and ESM bundles. It is not an npm package or an SLA certification. It adds no server, tunnel, DNS entry, permission or subscription tier. One instance owns both existing connection slots for its principal; do not start another instance for the same principal in another tab/process.

## Build and use

Use the pinned Node.js 24 toolchain and dependencies. Run `npm ci --ignore-scripts` and `npm run build`. `dist/dft-live-client.mjs` exports `ResilientLiveClient`; `dist/dft-live-client.js` exposes `DFT.ResilientLiveClient` in browsers. The manifest lists each file's SHA256. The validators are precompiled from the reviewed authoritative contract copy; `npm run client:validators` regenerates them and the build checks for drift. Schema compilation is absent at runtime and does not require `unsafe-eval`.

For Node.js 24, `DFT_API_KEY` must already exist in the process environment. Run `node examples/live-client-node.mjs`. The example obtains tickets with a server-held API key and prints only cursors, event IDs and bounded diagnostic codes. Its callbacks demonstrate in-memory delivery. Implement real storage inside your callbacks before relying on crash recovery.

For a browser, self-host the release bundle within your app and obtain tickets from your authenticated backend. That backend calls `POST /v2/live/stream-tickets` with its server-held key and the browser's exact HTTPS origin. Protect that route with your application session, authorization and CSRF policy. Never put the private API key into JavaScript, localStorage, a URL or a diagnostic. Both tickets must authorize the same subscription and prediction capability.

```js
import {ResilientLiveClient} from './dft-live-client.mjs';
const client = new ResilientLiveClient({
  endpoint: 'wss://api.datafortennis.com/v2/live',
  includePredictions: false,
  subscription: {include_point_events: true},
  ticket: async signal => {
    const response = await fetch('/your-authenticated-live-ticket-route', {
      method: 'POST', credentials: 'same-origin', signal,
      // Add the CSRF header required by your own backend.
    });
    if (!response.ok) throw new Error('HTTP_' + response.status);
    return response.json(); // Your backend returns the ticket object, without an API key.
  },
  onSnapshot: async snapshot => {
    await replaceApplicationStateAndCheckpoint(snapshot.matches, snapshot.cursor);
  },
  onEvent: async event => {
    await applyEventAndCheckpointAtomically(event, event.cursor);
  },
  onCheckpoint: async cursor => {
    await saveCheckpointWithoutChangingEntities(cursor);
  },
  onHeartbeat: async heartbeat => {
    await refreshFreshnessAndClearExpiredProbabilities(heartbeat);
  },
  onStatus: status => showDeliveryState(status),
  onDiagnostic: diagnostic => recordTransportDiagnostic(diagnostic),
});
await client.start();
```

Application functions in this example are integration points, not functions supplied by the client. Resolve an event callback only after its application transaction commits. Persist event IDs, entity revisions, state and cursor together. When starting after a process restart, supply `initialCursor` only alongside the corresponding restored application state. This gives replay with durable application deduplication; the library alone cannot promise exactly-once external side effects across a crash.

## Delivery and recovery semantics

- `start()` resolves when the first lane finishes snapshot or replay. `status().readyLanes === 2` confirms the peer is also ready. Cold startup applies one snapshot, then starts the peer from its acknowledged cursor.
- Callbacks are serialized. An event is acknowledged in memory only after `onEvent` succeeds; a callback failure closes both lanes, reports `CONSUMER_FAILED` and preserves the last acknowledged cursor. `onCheckpoint` handles filtered replay completion. Heartbeat cursors never substitute for event delivery.
- Events received on both lanes or after replay are delivered once during the running client. Different event IDs at one cursor or different payloads at one event ID fail closed. The bounded conflict cache retains at most 16,384 IDs or 16 MiB; older copies at/below the committed cursor are ignored. Global cursor gaps are valid for filtered subscriptions.
- A `resync_required` interrupts delivery, reports `RESYNC_REQUIRED`, closes the other lane, and assembles a complete replacement snapshot. `onSnapshot` receives `replacement: true`. No partial snapshot is applied. This recovers current state; events outside retained replay cannot be recovered by this client. Applications needing a complete historical event log must handle that loss explicitly.
- Tickets renew on the existing socket with one lane reserved through authentication and replay, while the peer remains subscribed. The first peer renewal is staggered by 15 seconds; subsequent renewals use 30 seconds. Failed renewal does not extend ticket expiry.
- Authentication/startup and ticket requests retain 5-second deadlines. A 15-second absence of application frames reconnects the affected lane; neither server heartbeat checks nor native physical acceptance rules are relaxed.
- Frames are limited to 1 MiB. Snapshots are limited to 3,072 matches, 64 matches per chunk and 16 MiB. Pending processing is limited to 256 frames or 16 MiB. Overflow fails visibly instead of dropping events or advancing the cursor.
- `stop()` aborts ticket requests, timers and both sockets. No new callback starts afterward. A callback already executing receives an aborted signal and must cooperate; the client cannot undo its external side effects.
- Raw clients reject prediction objects and events. Every frame is validated against the same public schema, plus result/input/probability coherence rules. Replay data is historical. Track source freshness and prediction expiry separately from connection availability and clear expired probabilities.

Both lanes use the same gateway and infrastructure. They reduce the effect of individual connection failures but cannot survive every shared gateway, source, storage or host failure. Transport failures remain visible in diagnostics and still require root investigation.

## Verify and reproduce a release

Use Node.js 24.21.0. All dependencies are pinned in `package-lock.json`.

```sh
npm ci --ignore-scripts
npx playwright install chromium
npm run check
npm run release:package
```

The release contains browser and ESM bundles, TypeScript declarations, the pinned public contract, third-party notices and a SHA256 manifest. GitHub Actions runs the same checks and builds the release artifacts. Compare `release/SHA256SUMS` with the file attached to the tagged GitHub release. The build contains no API key, telemetry or downloaded runtime code. The package is private in npm metadata to prevent accidental npm publication; there is no published npm install name.

`PROVENANCE.json` records the reviewed implementation and contract version. Tests use ephemeral signing keys, synthetic matches and a local protocol fixture. They require no account or live credential. They cover two lanes, replay, async acknowledgement, identity conflicts, schema drift, resync, backpressure, renewal, shutdown and a real browser under a CSP without unsafe-eval. These checks do not establish a monthly availability SLA.
