import {
  readFile,
  writeFile,
  mkdir,
  copyFile,
  readdir,
  rm,
} from "node:fs/promises";
import { createHash } from "node:crypto";

const manifest = JSON.parse(
  await readFile("dist/live-client-manifest.json", "utf8"),
);
// Only generated release files are replaced; source and user configuration stay outside this directory.
await rm("release", { recursive: true, force: true });
await mkdir("release");
for (const name of Object.keys(manifest.files))
  await copyFile("dist/" + name, "release/" + name);
await copyFile(
  "dist/live-client-manifest.json",
  "release/live-client-manifest.json",
);
await copyFile("README.md", "release/README.md");
await copyFile("PROVENANCE.json", "release/PROVENANCE.json");
await copyFile(
  "test/fixtures/canonical-live-v2.asyncapi.json",
  "release/live.asyncapi.json",
);
await writeFile(
  "release/node-example.mjs",
  (await readFile("examples/live-client-node.mjs", "utf8")).replace(
    "../dist/dft-live-client.mjs",
    "./dft-live-client.mjs",
  ),
);
const notices = [];
for (const name of [
  "zod",
  "ajv",
  "fast-deep-equal",
  "fast-uri",
  "json-schema-traverse",
]) {
  const root = "node_modules/" + name;
  const pkg = JSON.parse(await readFile(root + "/package.json", "utf8"));
  const filename = (await readdir(root)).find((file) =>
    /^licen[cs]e(?:\.|$)/i.test(file),
  );
  if (!filename) throw Error("DEPENDENCY_NOTICE_MISSING_" + name);
  notices.push(
    `${name} ${pkg.version}\n${await readFile(root + "/" + filename, "utf8")}`,
  );
}
await writeFile("release/THIRD_PARTY_NOTICES.txt", notices.join("\n\n---\n\n"));
const sums = [];
for (const name of (await readdir("release")).sort()) {
  const data = await readFile("release/" + name);
  sums.push(createHash("sha256").update(data).digest("hex") + "  " + name);
}
await writeFile("release/SHA256SUMS", sums.join("\n") + "\n");
console.log(
  JSON.stringify({
    version: manifest.version,
    files: sums.length,
    checksum_file: "release/SHA256SUMS",
  }),
);
