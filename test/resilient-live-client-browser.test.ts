import { it, expect } from "vitest";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { chromium } from "playwright";
import { build } from "esbuild";
import { WebSocketServer } from "ws";

it("runs two browser connections under a CSP that forbids unsafe-eval", async () => {
  const contract = JSON.parse(
    await readFile(
      new URL("./fixtures/canonical-live-v2.asyncapi.json", import.meta.url),
      "utf8",
    ),
  );
  const result = await build({
    entryPoints: ["src/client/resilient-live-client.ts"],
    bundle: true,
    write: false,
    platform: "browser",
    format: "iife",
    globalName: "DFT",
  });
  const code = result.outputFiles![0]!.text;
  const server = createServer((req, res) => {
    res.setHeader(
      "Content-Security-Policy",
      "default-src 'self'; script-src 'self'; connect-src 'self' ws://127.0.0.1:*",
    );
    if (req.url === "/client.js") {
      res.setHeader("content-type", "application/javascript");
      res.end(code);
    } else if (req.url === "/run.js") {
      res.setHeader("content-type", "application/javascript");
      res.end(
        `window.failures=[];window.applied=[];window.client=new DFT.ResilientLiveClient({endpoint:'ws://'+location.host+'/v2/live',includePredictions:false,ticket:async()=>({endpoint:'ws://'+location.host+'/v2/live',version:'2.0',ticket:'short-fixture-only',expires_at:new Date(Date.now()+60000).toISOString(),renew_after_seconds:30}),onSnapshot:s=>window.applied.push(s.cursor),onEvent:()=>{},onDiagnostic:d=>window.failures.push(d.code)});window.client.start().catch(e=>window.failures.push(e.message));`,
      );
    } else {
      res.setHeader("content-type", "text/html");
      res.end(
        '<!doctype html><script src="/client.js"></script><script src="/run.js"></script>',
      );
    }
  });
  const wss = new WebSocketServer({ server });
  wss.on("connection", (ws) =>
    ws.on("message", (raw) => {
      const v = JSON.parse(raw.toString());
      if (v.type === "authenticate")
        ws.send(
          JSON.stringify({
            schema_version: "2.0",
            type: "authenticated",
            include_predictions: false,
            expires_at: new Date(Date.now() + 60000).toISOString(),
            resume_token: "a".repeat(64),
          }),
        );
      if (v.type === "subscribe") {
        if (v.after !== undefined)
          ws.send(
            JSON.stringify({
              schema_version: "2.0",
              type: "replay.started",
              after: v.after,
              cursor: v.after,
            }),
          );
        ws.send(
          JSON.stringify(
            v.after === undefined
              ? {
                  schema_version: "2.0",
                  type: "snapshot",
                  cursor: "0",
                  matches: [],
                }
              : {
                  schema_version: "2.0",
                  type: "replay.completed",
                  cursor: v.after,
                },
          ),
        );
      }
    }),
  );
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage(),
      errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.name));
    await page.goto(
      `http://127.0.0.1:${(server.address() as { port: number }).port}`,
    );
    expect(errors).toEqual([]);
    await page.waitForFunction(
      () =>
        (
          window as unknown as {
            client?: { status: () => { readyLanes: number } };
          }
        ).client?.status().readyLanes === 2,
      {},
      { timeout: 2000 },
    );
    expect(
      await page.evaluate(
        () => (window as unknown as { applied: string[] }).applied,
      ),
    ).toEqual(["0"]);
    expect(
      await page.evaluate(
        () => (window as unknown as { failures: string[] }).failures,
      ),
    ).toEqual([]);
    expect(errors).toEqual([]);
    expect(wss.clients.size).toBe(2);
    await page.evaluate(() =>
      (
        window as unknown as { client: { stop: () => Promise<void> } }
      ).client.stop(),
    );
  } finally {
    await browser.close();
    for (const ws of wss.clients) ws.terminate();
    await new Promise<void>((r) => wss.close(() => r()));
    await new Promise<void>((r) => server.close(() => r()));
  }
}, 10000);
