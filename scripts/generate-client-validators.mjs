import { readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { Ajv } from "ajv";
import standalone from "ajv/dist/standalone/index.js";
const source = new URL(
  "../test/fixtures/canonical-live-v2.asyncapi.json",
  import.meta.url,
);
const output = new URL(
  "../src/client/live-validators.generated.ts",
  import.meta.url,
);
const text = await readFile(source, "utf8"),
  sha = createHash("sha256").update(text).digest("hex");
if (sha !== "48c10d1668c641286e88a197efd9a74b0aa125296e21731263c34702070d4bfb")
  throw Error("CLIENT_CONTRACT_PIN_CHANGED_REVIEW_REQUIRED");
const contract = JSON.parse(text),
  refs = (value) =>
    Array.isArray(value)
      ? value.map(refs)
      : value && typeof value === "object"
        ? Object.fromEntries(
            Object.entries(value).map(([k, v]) => [
              k,
              k === "$ref" && typeof v === "string"
                ? v.replace(
                    "#/components/schemas/",
                    "dft-live-components#/$defs/",
                  )
                : refs(v),
            ]),
          )
        : value;
const ajv = new Ajv({
    strict: true,
    allowUnionTypes: true,
    inlineRefs: false,
    code: { source: true, esm: true },
  }),
  exports = {},
  types = {};
let index = 0;
ajv.addSchema({
  $id: "dft-live-components",
  $defs: refs(contract.components.schemas),
});
for (const [name, raw] of Object.entries(contract.components.messages)) {
  if (["Authenticate", "Subscribe", "Ping"].includes(name)) continue;
  const payload = raw.payload,
    id = "frame" + index++;
  ajv.addSchema({ ...refs(payload), $id: id }, id);
  exports[id] = id;
  for (const type of payload.properties.type.enum ?? [
    payload.properties.type.const,
  ])
    if (typeof type === "string") types[type] = id;
}
let code = standalone.default(ajv, exports),
  imports = "";
for (const path of new Set(
  [...code.matchAll(/require\("([^"]+)"\)/g)].map((m) => m[1]),
)) {
  const name = "runtime" + imports.split("\n").length;
  imports += `import ${name}Import from '${path}.js';\nconst ${name}=typeof ${name}Import==='function'?{default:${name}Import}:${name}Import;\n`;
  code = code.replaceAll(`require("${path}")`, name);
}
code =
  "// @ts-nocheck\n// Generated from the reviewed public V2 contract. Run npm run client:validators.\n" +
  imports +
  code +
  `\nexport const contractSha256='${sha}';\nexport const validators={${Object.entries(
    types,
  )
    .map(([type, id]) => JSON.stringify(type) + ":" + id)
    .join(",")}};\n`;
if (process.argv.includes("--check")) {
  if ((await readFile(output, "utf8")) !== code)
    throw Error("CLIENT_VALIDATORS_NOT_REGENERATED");
} else await writeFile(output, code);
console.log(
  JSON.stringify({
    contract_sha256: sha,
    messages: index,
    types: Object.keys(types).length,
    bytes: Buffer.byteLength(code),
    checked: process.argv.includes("--check"),
  }),
);
