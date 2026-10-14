import { validators, contractSha256 } from "./live-validators.generated.js";
import { checkFrameCoherence } from "./frame-coherence.js";

/** Build-time compilation keeps browser CSP intact and pins the authoritative schema. */
export class PinnedFrameValidator {
  readonly contractSha256: string = contractSha256;
  check(value: unknown, predictions: boolean): Record<string, unknown> {
    if (!value || typeof value !== "object" || Array.isArray(value))
      throw Error("SCHEMA_DRIFT");
    const frame = value as Record<string, unknown>,
      type = String(frame.type);
    if (!Object.hasOwn(validators, type)) throw Error("SCHEMA_DRIFT");
    const validate = (validators as Record<string, (v: unknown) => boolean>)[
      type
    ]!;
    if (!validate(value)) throw Error("SCHEMA_DRIFT");
    return checkFrameCoherence(frame, predictions);
  }
}
