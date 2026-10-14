const own = (v: unknown): Record<string, unknown> =>
  v && typeof v === "object" && !Array.isArray(v)
    ? (v as Record<string, unknown>)
    : {};
export function checkFrameCoherence(
  v: Record<string, unknown>,
  predictions: boolean,
): Record<string, unknown> {
  const states = Array.isArray(v.matches)
    ? v.matches
    : [v.state].filter(Boolean);
  if (
    !predictions &&
    (String(v.type).startsWith("prediction.") ||
      states.some((s) => Object.hasOwn(own(s), "prediction")) ||
      states.some((s) => Object.hasOwn(own(s), "prediction_status")))
  )
    throw Error("RAW_CAPABILITY_LEAK");
  for (const raw of states) {
    const s = own(raw),
      m = own(s.match),
      p = own(s.prediction);
    if (typeof m.match_id === "string") {
      const winner = m.winner_player_id,
        basis = m.winner_basis,
        reason = m.result_reason,
        players = m.player_ids;
      if (
        (winner != null &&
          (!Array.isArray(players) ||
            !players.includes(winner) ||
            !["finished", "retired", "walkover"].includes(String(m.status)) ||
            !["provider", "completed_score"].includes(String(basis)))) ||
        (winner == null && basis != null) ||
        (reason != null &&
          reason !==
            (
              {
                finished: "normal",
                retired: "retired",
                walkover: "walkover",
                cancelled: "cancelled",
              } as Record<string, string>
            )[String(m.status)])
      )
        throw Error("MATCH_RESULT_COHERENCE");
    }
    if (!Object.keys(p).length) continue;
    if (
      p.score_revision !== s.score_revision ||
      p.state_revision !== m.prediction_input_revision ||
      p.match_id !== s.match_id
    )
      throw Error("PREDICTION_ALIGNMENT");
  }
  if (v.type === "prediction.updated") {
    const p = own(v.data),
      prob = p.probabilities as number[];
    if (
      p.match_id !== v.match_id ||
      !Array.isArray(prob) ||
      prob.length !== 2 ||
      prob.some((n) => !Number.isFinite(n) || n < 0 || n > 1) ||
      Math.abs(prob[0]! + prob[1]! - 1) > 1e-9
    )
      throw Error("PREDICTION_PROBABILITY_INVALID");
  }
  return v;
}
