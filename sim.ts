// Card authorization lifecycles under three processing designs, run against
// thousands of random interleavings. See README.md.
//
//   node sim.ts            (Node 23.6+ runs TypeScript directly)
//   node sim.ts --seeds 20000

type CardEvent =
  | { kind: "auth"; amount: number }
  | { kind: "incremental"; amount: number }
  | { kind: "capture"; amount: number }
  | { kind: "reversal"; amount: number };

// What the "database" holds for one lifecycle: the hold, and the two transfers
// the post describes (loan disbursement and collateral -> repayment book transfer).
interface Row {
  hold: number;
  disbursement: number; // amount held or settled on the loan disbursement
  bookTransfer: number; // amount held or settled on the book transfer
  captured: number;
  reversed: number;
  version: number;
}

type Step = Generator<"yield", void, void>;

// --- a deterministic random scheduler ------------------------------------------------------
function rng(seed: number) {
  let s = seed >>> 0 || 1;
  return () => {
    s ^= s << 13;
    s ^= s >>> 17;
    s ^= s << 5;
    return (s >>> 0) / 2 ** 32;
  };
}

// --- one lifecycle's events, the way a merchant might send them -----------------------------
function makeLifecycle(rand: () => number): CardEvent[] {
  const auth = 100 + Math.floor(rand() * 400);
  const events: CardEvent[] = [{ kind: "auth", amount: auth }];
  let open = auth;
  if (rand() < 0.4) {
    const tip = 10 + Math.floor(rand() * 60);
    events.push({ kind: "incremental", amount: tip });
    open += tip;
  }
  // Split what's open into 2-4 partial captures and maybe a partial reversal.
  const parts = 2 + Math.floor(rand() * 3);
  for (let i = 0; i < parts && open > 0; i++) {
    const last = i === parts - 1;
    const amt = last ? open : Math.max(1, Math.floor(open * (0.2 + rand() * 0.4)));
    events.push(rand() < 0.2 && !last ? { kind: "reversal", amount: amt } : { kind: "capture", amount: amt });
    open -= amt;
  }
  return events;
}

// --- applying one event: read the row, do the work, write it back ----------------------------
// Each `yield` is an await on the database or a bank API, where another handler can run.
const stats = { outOfOrder: 0 };

function* apply(db: Map<string, Row>, id: string, e: CardEvent, lock?: Lock): Step {
  if (lock) yield* lock.acquire(id);
  const row = { ...db.get(id)! }; // read
  // A capture or reversal for more than is currently held means it ran before the
  // auth or incremental auth it belongs to. A real handler would decline or error here.
  if ((e.kind === "capture" || e.kind === "reversal") && row.hold < e.amount) stats.outOfOrder += 1;
  yield "yield";
  switch (e.kind) {
    case "auth":
    case "incremental":
      row.hold += e.amount;
      row.disbursement += e.amount; // update the held transfers
      yield "yield";
      row.bookTransfer += e.amount;
      break;
    case "capture":
      row.hold -= e.amount;
      row.captured += e.amount; // settle part of both transfers
      yield "yield";
      break;
    case "reversal":
      row.hold -= e.amount;
      row.reversed += e.amount;
      row.disbursement -= e.amount;
      yield "yield";
      row.bookTransfer -= e.amount;
      break;
  }
  row.version += 1;
  db.set(id, row); // write
  if (lock) lock.release(id);
}

class Lock {
  held = new Set<string>();
  waits = 0;
  *acquire(id: string): Step {
    while (this.held.has(id)) {
      this.waits += 1;
      yield "yield";
    }
    this.held.add(id);
  }
  release(id: string) {
    this.held.delete(id);
  }
}

// --- the three designs ------------------------------------------------------------------------
// 1. "unordered": every webhook is its own job, running concurrently, no locks.
// 2. "locked": same, but each job takes a row lock for the lifecycle (what the post says was
//    needed for correctness, and what caused contention).
// 3. "per-lifecycle": one runner per lifecycle; new events are signals it drains in order
//    (the TPS design: one Temporal workflow per lifecycle).
type Design = "unordered" | "locked" | "per-lifecycle";

function* perLifecycleRunner(db: Map<string, Row>, id: string, inbox: CardEvent[], done: () => boolean): Step {
  // Mirrors the TPS loop: process what's pending, and if a signal arrived meanwhile, go again.
  while (true) {
    const next = inbox.shift();
    if (next) {
      yield* apply(db, id, next);
      continue;
    }
    if (done()) return;
    yield "yield";
  }
}

interface Result {
  violations: number;
  lockWaits: number;
  outOfOrder: number;
}

function runOnce(design: Design, seed: number, lifecycles: number): Result {
  const rand = rng(seed);
  stats.outOfOrder = 0;
  const db = new Map<string, Row>();
  const all: { id: string; events: CardEvent[] }[] = [];
  for (let i = 0; i < lifecycles; i++) {
    const id = `ls_${i}`;
    db.set(id, { hold: 0, disbursement: 0, bookTransfer: 0, captured: 0, reversed: 0, version: 0 });
    all.push({ id, events: makeLifecycle(rand) });
  }

  const lock = new Lock();
  const jobs: Step[] = [];
  // Webhooks from different lifecycles interleave at random; within one lifecycle they
  // arrive in order. Several can be in flight at once, which is where races come from.
  const arrivals: { id: string; e: CardEvent }[] = [];
  const cursors = all.map((l) => ({ id: l.id, events: [...l.events] }));
  while (cursors.some((c) => c.events.length)) {
    const live = cursors.filter((c) => c.events.length);
    const c = live[Math.floor(rand() * live.length)];
    arrivals.push({ id: c.id, e: c.events.shift()! });
  }

  const inboxes = new Map<string, CardEvent[]>();
  let delivered = 0;
  if (design === "per-lifecycle") {
    for (const l of all) {
      inboxes.set(l.id, []);
      jobs.push(perLifecycleRunner(db, l.id, inboxes.get(l.id)!, () => delivered === arrivals.length));
    }
  }

  const pending = [...arrivals];
  while (pending.length || jobs.length) {
    // With some probability a new webhook arrives, otherwise a running job advances.
    if (pending.length && (jobs.length === 0 || rand() < 0.5)) {
      const { id, e } = pending.shift()!;
      delivered++;
      if (design === "per-lifecycle") inboxes.get(id)!.push(e); // a signal to the running workflow
      else jobs.push(apply(db, id, e, design === "locked" ? lock : undefined));
      continue;
    }
    const i = Math.floor(rand() * jobs.length);
    if (jobs[i].next().done) jobs.splice(i, 1);
  }

  let violations = 0;
  for (const l of all) {
    const row = db.get(l.id)!;
    const authed = l.events.filter((e) => e.kind === "auth" || e.kind === "incremental").reduce((s, e) => s + e.amount, 0);
    const captured = l.events.filter((e) => e.kind === "capture").reduce((s, e) => s + e.amount, 0);
    const reversed = l.events.filter((e) => e.kind === "reversal").reduce((s, e) => s + e.amount, 0);
    const ok =
      row.hold === 0 &&
      row.captured === captured &&
      row.reversed === reversed &&
      row.disbursement === authed - reversed &&
      row.bookTransfer === authed - reversed &&
      row.version === l.events.length;
    if (!ok) violations++;
  }
  return { violations, lockWaits: lock.waits, outOfOrder: stats.outOfOrder };
}

// --- main --------------------------------------------------------------------------------------
const argSeeds = process.argv.indexOf("--seeds");
const SEEDS = argSeeds > 0 ? Number(process.argv[argSeeds + 1]) : 5000;
const LIFECYCLES = 8; // concurrent lifecycles per run (several cards swiping at once)

console.log(`${SEEDS} random runs x ${LIFECYCLES} lifecycles each\n`);
console.log("design         | runs with a wrong final state | lifecycles wrong | out-of-order applies per run | lock waits per run");
for (const design of ["unordered", "locked", "per-lifecycle"] as Design[]) {
  let badRuns = 0;
  let badLifecycles = 0;
  let waits = 0;
  let ooo = 0;
  for (let seed = 1; seed <= SEEDS; seed++) {
    const r = runOnce(design, seed, LIFECYCLES);
    if (r.violations) badRuns++;
    badLifecycles += r.violations;
    waits += r.lockWaits;
    ooo += r.outOfOrder;
  }
  const pct = (n: number, d: number) => `${((100 * n) / d).toFixed(1)}%`;
  console.log(
    `${design.padEnd(14)} | ${pct(badRuns, SEEDS).padStart(29)} | ${pct(badLifecycles, SEEDS * LIFECYCLES).padStart(16)} | ${(ooo / SEEDS).toFixed(2).padStart(28)} | ${(waits / SEEDS).toFixed(1).padStart(18)}`,
  );
}

// The per-lifecycle design should never end wrong or apply an event out of order.
{
  let bad = 0;
  for (let seed = 1; seed <= SEEDS; seed++) {
    const r = runOnce("per-lifecycle", seed, LIFECYCLES);
    bad += r.violations + r.outOfOrder;
  }
  if (bad) {
    console.error(`per-lifecycle design had ${bad} violations`);
    process.exitCode = 1;
  }
}
