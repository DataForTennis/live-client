/** Authentication resets subscriptions. A permit stays reserved through replay,
 * not merely through ticket fetch or the authenticated response. */
export class RenewalCoordinator {
  private readonly lanes = [false, false];
  private owner: number | null = null;
  private readonly waiting = new Map<
    () => void,
    { index: number; cancel: () => void }
  >();
  ready(index: number) {
    this.lanes[index] = true;
    if (this.owner === index) this.owner = null;
    this.wake();
  }
  closed(index: number) {
    this.lanes[index] = false;
    if (this.owner === index) this.owner = null;
    for (const entry of [...this.waiting.values()])
      if (entry.index === index) entry.cancel();
    this.wake();
  }
  private wake() {
    for (const check of [...this.waiting.keys()]) check();
  }
  private reserve(index: number) {
    if (this.owner !== null || !this.lanes[1 - index]) return false;
    this.owner = index;
    this.lanes[index] = false;
    return true;
  }
  async acquire(
    index: number,
    expiresAt: number,
    signal: AbortSignal,
  ): Promise<void> {
    if (![0, 1].includes(index) || !Number.isFinite(expiresAt))
      throw Error("RENEWAL_INPUT_INVALID");
    signal.throwIfAborted();
    const remaining = expiresAt - Date.now() - 5000;
    if (remaining <= 0) throw Error("TICKET_RENEWAL_DEADLINE");
    if (this.reserve(index)) return;
    if (this.waiting.size >= 2) throw Error("RENEWAL_WAIT_CAPACITY");
    return new Promise((resolve, reject) => {
      const cleanup = () => {
        clearTimeout(timer);
        this.waiting.delete(check);
        signal.removeEventListener("abort", abort);
      };
      const check = () => {
        if (this.reserve(index)) {
          cleanup();
          resolve();
        }
      };
      const abort = () => {
        cleanup();
        reject(signal.reason ?? Error("RENEWAL_ABORTED"));
      };
      const timer = setTimeout(() => {
        cleanup();
        reject(Error("TICKET_RENEWAL_PEER_NOT_READY"));
      }, remaining);
      this.waiting.set(check, {
        index,
        cancel: () => {
          cleanup();
          reject(Error("RENEWAL_CONNECTION_CLOSED"));
        },
      });
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) abort();
      else check();
    });
  }
}
