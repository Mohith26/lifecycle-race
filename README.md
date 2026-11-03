# lifecycle-race

Slash's post [Building Card Transaction Processing at Slash](https://www.slash.com/engineering/blog/card-transaction-processing)
explains why they moved card processing to TPS: one Temporal workflow per card
authorization lifecycle, with each new card event arriving as a signal, instead of
separate queues that each handle webhooks and need database locks to stay correct.
I wanted to see the difference on a small model I could run thousands of times, so
this is a toy version of the three designs and a scheduler that tries random
interleavings.

```
node sim.ts              # Node 23.6+ runs the TypeScript directly, no install
node sim.ts --seeds 20000
```

## The model

- A lifecycle is an auth, sometimes an incremental auth (a tip), then 2 to 4 partial
  captures or reversals that use up the hold, like the examples in the post.
- Applying an event reads the lifecycle's row (hold, the two held transfers, captured
  and reversed totals), does some work with `await`-like pauses in between, and writes
  the row back.
- Webhooks for one lifecycle arrive in order, mixed in with 7 other lifecycles. A seeded
  scheduler decides at every pause which job runs next, so each seed is a different
  interleaving.
- Three designs:
  - `unordered`: every webhook is its own job, running concurrently, no locks.
  - `locked`: the same, but each job holds a row lock on its lifecycle while it works.
  - `per-lifecycle`: one runner per lifecycle that drains its signals in order, as TPS does.
- After each run I check every lifecycle: hold back to 0, captured and reversed totals
  right, both transfers equal to authorized minus reversed, one write per event. I also
  count "out-of-order applies": a capture or reversal that runs while the hold is smaller
  than its amount, meaning it got ahead of the auth it belongs to.

## Results (5,000 runs x 8 lifecycles, `node sim.ts`)

| design | runs ending wrong | lifecycles ending wrong | out-of-order applies per run | lock waits per run |
|---|---|---|---|---|
| unordered | 100.0% | 97.6% | 14.97 | 0 |
| locked | 0% | 0% | 4.98 | 106.5 |
| per-lifecycle | 0% | 0% | 0 | 0 |

- Without locks, read-modify-write on the same lifecycle loses updates almost every time.
  The exact rate is an artifact of how aggressive my scheduler is; the point is only that
  it's not rare.
- Locks fix the totals, but the scheduler waited on a lock about 106 times per run, and
  locks give mutual exclusion, not order: about 5 events per run still ran before the auth
  they belonged to. Keeping order with locks needs extra state checks in every handler,
  which is the "guardrail code" the post says TPS let them delete.
- One runner per lifecycle gets both for free: no lock waits, nothing out of order, and
  every final state correct.

## What this is not

It's a toy. There is no Temporal, no real database, no ledger-level contention across
lifecycles (the post's other bottleneck), and the pauses are scheduler steps, not
milliseconds. It shows the ordering and lost-update argument, not throughput.

One small thing I noticed while reading: in the post's `TPSWorkflow` snippet the loop sets
`continueAsNew = true` rather than `shouldContinueAsNew = true`, so as written it would
exit after one pass instead of restarting the task list. I assume that's just the blog
version of the code; the `per-lifecycle` runner here restarts the way the text describes.
