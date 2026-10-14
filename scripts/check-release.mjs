import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { transform } from "esbuild";
import { ResilientLiveClient } from "../dist/dft-live-client.mjs";

const manifest = JSON.parse(
  await readFile("dist/live-client-manifest.json", "utf8"),
);
const provenance = JSON.parse(await readFile("PROVENANCE.json", "utf8"));
const contract = await readFile(
  "test/fixtures/canonical-live-v2.asyncapi.json",
);
assert.equal(
  createHash("sha256").update(contract).digest("hex"),
  provenance.contract_sha256,
);
assert.equal(manifest.contract_sha256, provenance.contract_sha256);
assert.equal(typeof ResilientLiveClient, "function");
for (const [name, expected] of Object.entries(provenance.runtime_module_sha256)) {
  const code = (await transform(await readFile(name, "utf8"), {
    loader: "ts", format: "esm", target: "es2022", minify: true,
  })).code;
  assert.equal(createHash("sha256").update(code).digest("hex"), expected, name);
}
for (const [name, expected] of Object.entries(manifest.files)) {
  assert(
    [
      "dft-live-client.js",
      "dft-live-client.mjs",
      "dft-live-client.d.mts",
    ].includes(name),
  );
  const data = await readFile("dist/" + name);
  assert.equal(data.length, expected.bytes);
  assert.equal(
    createHash("sha256").update(data).digest("hex"),
    expected.sha256,
  );
}
console.log("Release imports, pinned contract and checksums verified.");
