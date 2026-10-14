import { build } from "esbuild";
import { readFile, writeFile, mkdir, copyFile } from "node:fs/promises";
import { createHash } from "node:crypto";

const pkg = JSON.parse(await readFile("package.json", "utf8"));
const provenance = JSON.parse(await readFile("PROVENANCE.json", "utf8"));
await mkdir("dist", { recursive: true });
for (const [format, name] of [
  ["iife", "dft-live-client.js"],
  ["esm", "dft-live-client.mjs"],
]) {
  await build({
    entryPoints: ["src/client/resilient-live-client.ts"],
    bundle: true,
    platform: "browser",
    format,
    globalName: format === "iife" ? "DFT" : undefined,
    minify: true,
    target: "es2022",
    outfile: "dist/" + name,
  });
}
await copyFile(
  "dist/types/src/client/resilient-live-client.d.ts",
  "dist/dft-live-client.d.mts",
);
const files = [
  "dft-live-client.js",
  "dft-live-client.mjs",
  "dft-live-client.d.mts",
];
const manifest = {
  kind: "dft_live_client_reference",
  version: pkg.version,
  schema_version: provenance.schema_version,
  contract_sha256: provenance.contract_sha256,
  files: Object.fromEntries(
    await Promise.all(
      files.map(async (name) => {
        const data = await readFile("dist/" + name);
        return [
          name,
          {
            bytes: data.byteLength,
            sha256: createHash("sha256").update(data).digest("hex"),
          },
        ];
      }),
    ),
  ),
};
await writeFile(
  "dist/live-client-manifest.json",
  JSON.stringify(manifest, null, 2) + "\n",
);
console.log(JSON.stringify(manifest));
