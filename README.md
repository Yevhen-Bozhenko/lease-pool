# test-account-broker

A small, self-contained Java reference implementation of three ways to share a limited pool of test
accounts across parallel tests — and a benchmark that shows what each one costs you.

**If you read one thing, read [Example output](#example-output).** Three strategies, one identical
workload: the naive one collides on all 24 tests, the other two never collide — and the broker does
it in half the static strategy's wall clock, using the whole pool instead of half of it. Everything
below explains that table.

> This is a **generic, illustrative reference implementation of a common pattern**. It is not tied to
> any specific system, product or codebase, and it contains no real credentials or endpoints. All
> numbers below are **illustrative and machine-dependent** (they depend on your CPU, core count, JVM
> and OS timer granularity — on Windows in particular `Thread.sleep` rounds up to ~15 ms, which
> inflates every duration). Run it yourself; the *shape* of the result is the point, not the digits.

## The problem

Tests need accounts. There are never enough of them, so a pool is shared. Once the suite runs in
parallel, the pool — not the hardware — caps how fast it can go, and you face two independent failure
modes:

1. **Collisions** — two tests use the same account at the same time. Symptoms are flaky,
   unreproducible failures: your fixture data changes underneath you, a logout in one test breaks
   another, an assertion sees a state no test created.
2. **Under-utilisation** — tests wait for accounts that are not actually busy. Nothing fails; the
   suite is just slow, and adding accounts to the pool does not help.

A good strategy has to solve *both*. It is easy to solve either one alone, which is exactly why the
bad strategies survive so long.

## The three strategies

All three implement the same interface, `AccountStrategy`, and the benchmark runs the identical
workload against each. `acquire(owner)` hands back a `Lease`; closing the lease returns the account.
`Lease` is `AutoCloseable`, so a try-with-resources block releases it even when the test throws —
which is why that is preferred over calling `release(lease)` by hand.

Three rather than two, because the interesting comparison looks like naive vs. broker and isn't.
Naive is the *fastest* row, so a two-row demo says "correctness costs you 3×" — the wrong lesson, and
wrong because naive is fast only by being broken. `StaticAssignment` is the row that makes the
argument work: it is the only strategy the broker actually beats on wall clock, the only
demonstration of under-utilisation — which is what the capability match exists to fix — and the
realistic baseline, since hard-coded per-test account ids is what teams actually have.

### 1. `NaiveSharedList` — fast and wrong

Scan a shared list, take the first account that looks free. No locking:

```java
if (!account.isInUse()) {   // (1) CHECK — free right now
    ...                     //     ...nothing stops other threads getting here too
    account.forceHold(me);  // (2) ACT   — claim it
}
```

Check and act are two separate steps, so any number of threads can pass the check before any of them
acts, and all of them walk away "owning" the same account. In production code the window is
nanoseconds wide, which is what makes this bug rare and environment-dependent. The demo widens it
with a 2 ms sleep so the failure lands identically on every run — see [Tuning](#tuning) for what
happens when you set that window back to `0`.

It is also the fastest strategy in the table, because nobody ever waits. Speed is not evidence that
a pool is being shared correctly.

### 2. `StaticAssignment` — safe and slow

The legacy fix: hard-code an account id per test at setup ("this suite always logs in as ACC-03").
There are more tests than ids, so several tests share one id. The rules are the whole story:

- a test may use **only** its assigned account;
- if that account is busy the test waits, **even when other accounts sit idle**.

Mutual exclusion is genuine (one monitor per account), so collisions are zero. But the wall clock is
now set by the most contended id rather than by the pool size: with 6 tests pinned to each of 4 ids,
those tests run 6-deep in a queue while the other 4 accounts do nothing.

### 3. `AccountBroker` — safe and fast

Three changes, one per problem:

- **Atomic reserve-if-free.** "Is it free?" and "it's mine now" are a single `compareAndSet` inside
  the broker's lock, so two callers can never take the same account. Fixes strategy 1.
- **`acquire(owner, capability)`.** A caller asks for *any* free account carrying the tag it needs
  instead of naming an id, so every account is usable by every caller. Fixes strategy 2.
- **TTL leases.** Every claim carries an expiry. An owner that crashes, hangs or is killed without
  releasing has its lease reclaimed automatically, so one dead test cannot permanently shrink the
  pool. Reclaim runs on the acquiring thread under the lock — no background thread, nothing to shut
  down.

Waiters block on a `Condition` rather than spinning, and the lock is **fair** so a steady stream of
arrivals cannot starve a waiter. Fairness costs a little throughput and buys predictable queueing,
which is the right trade for a small shared pool.

## When this pattern applies

The broker is a **reserve-if-free pool with expiring leases**, and nothing in it is specific to test
accounts. It fits wherever three things are true at once: the resource is shared, holding it is
mutually exclusive, and the pool is smaller than the number of workers that want one. That third
condition is what turns a correctness problem into a throughput problem — with enough resources to go
round every strategy looks fine, which is why these bugs surface only under parallelism.

Same shape, different domain: seats on a licensed tool, sandbox tenants or database schemas handed to
parallel CI jobs, rate-limited API keys, devices in a hardware lab, staging environments, serial
ports. The two failure modes from the top reappear unchanged in all of them.

The two ideas are separable, and you may need only one. Reserve-if-free is what makes a pool
*correct*, so it is never optional. TTL leases make it *self-healing*, which matters once the pool
outlives a single run — over a long enough window, some holder always dies without releasing.

### Limits

Single-process and in-memory. Correctness rests on one `ReentrantLock` and one `AtomicReference` per
account, which reach exactly as far as one JVM's heap, and leases live in a `HashMap` that dies with
the process.

**This is not a distributed lock.** Two brokers over the same accounts keep separate books and will
double-issue. If your workers span machines, containers or JVMs the pattern still holds but the
mechanism does not — the reserve and the expiry have to live somewhere every worker can see them: a
conditional `UPDATE` on a database row, Redis `SET NX PX`, or ZooKeeper/etcd leases. The API shape
survives that move; the `compareAndSet` does not.

## Using the broker in your own project

The broker knows nothing about tests. An **owner** is any label you want to see in a diagnostic; a
**capability** is an opaque tag your domain defines — `"shopper"`, `"eu-vat"`, `"readonly"`,
whatever describes how your accounts differ. The public surface is three things: acquire, release,
and a lease TTL.

```java
// One broker per suite: the pool is a suite-level resource.
AccountBroker accounts = new AccountBroker(
        List.of(new Account("acct-01", "shopper"),
                new Account("acct-02", "shopper"),
                new Account("acct-03", "merchant")),
        TimeUnit.MINUTES.toMillis(5),    // lease TTL: the safety net for a killed JVM
        TimeUnit.MINUTES.toMillis(1));   // how long a caller waits for a free account

try (Lease lease = accounts.acquire("checkout-test", "shopper")) {
    logIn(lease.account().id());
    ...
}   // released here however the block exits
```

Pick the TTL to sit comfortably above the longest a caller can legitimately hold an account: too low
and live work gets its account reclaimed underneath it, too high and a crashed run locks accounts
until somebody notices. `acquire(owner)` with no capability matches any free account.

### When acquire fails

The two ways it can fail need opposite responses, so they are different types — no message parsing
needed to tell them apart.

**`NoAccountAvailableException`** — the pool was saturated: every matching account stayed held for
the whole acquire timeout. Retryable; back off and try again, or fail the test with a clear "no
capacity" reason. Carries `owner()`, `capability()` and `waited()`.

**`IllegalArgumentException`** — no account in the pool carries that capability. Never retry; waiting
cannot fix it. Usually a typo, sometimes a tag that environment never provisioned. Thrown *before*
any waiting, so it is instant and names both the unknown tag and the tags the pool does offer.

```java
try (Lease lease = accounts.acquire("checkout-test", "shopper")) {
    ...
} catch (NoAccountAvailableException e) {
    log("no '%s' account for %s after %d ms".formatted(e.capability(), e.owner(),
            e.waited().toMillis()));
    throw e;
}
```

`src/test/java/demo/BrokerTestNgExample.java` shows the same thing wired into a real framework, with
TestNG `@BeforeMethod`/`@AfterMethod` leasing an account around each test method (and `alwaysRun` on
the teardown, so a failing test still returns its account). It is compiled so it cannot rot, but
`mvn test` does not run it — there are no real accounts behind those ids.

## Running it

Plain JDK (21+), no build tool (on Windows PowerShell, use `src\main\java\demo\*.java`):

```bash
javac -d out src/main/java/demo/*.java
java -cp out demo.Benchmark
```

With Maven, if you prefer:

```bash
mvn -q compile exec:java   # the benchmark
mvn test                   # the JUnit suite
```

There is nothing to configure: every run executes all three strategies against the same workload and
then the TTL reclaim demo. Each strategy gets a fresh pool every round, so nothing inherits another's
state. To change the shape of the run, edit the constants at the top of `Benchmark.java` (see
[Tuning](#tuning)).

### Tests

Eight JUnit cases pin the claims this README makes — seven on the broker, one on the naive strategy's
spin bound. Each carries a `@DisplayName` stating what it checks, so `AccountBrokerTest.java` reads
as a list of the guarantees.

```bash
mvn test
```

They exist because the benchmark *cannot* catch some of these: it runs a 5 s lease TTL against 40 ms
of work, so the stale-release hazard stays invisible however many times you run it.

The two TTL cases advance a fake clock by hand rather than sleeping past the expiry: a sleep long
enough to be reliable on a loaded CI agent is also long enough to slow the suite, and it still only
makes the expiry *probable*. That clock drives lease expiry only — the acquire deadline stays on the
real clock, so a regression in TTL reclaim *fails* these tests instead of hanging them. The whole
suite runs in well under a second, and both test dependencies are test-scoped, so the demo has zero
runtime dependencies.

## Example output

Illustrative run on an 8-core Windows laptop, Temurin JDK 21:

```
test-account-broker
pool=8, parallel tests=24, tests per static id=6, work=40 ms/test
1 warm-up round + 5 measured rounds, median reported
ideal wall clock for a fully shared pool: 120 ms

Strategy            Collisions   Duration (ms)   Accounts used   Notes
-----------------------------------------------------------------------------------------------------------------
NaiveSharedList             24              45          1 / 8    unsynchronised scan; check-then-act race
StaticAssignment             0             280          4 / 8    one fixed account per test; shared ids serialise
AccountBroker                0             139          8 / 8    atomic claim + acquire(capability) + TTL leases

TTL reclaim demo (a test that never releases)
crashing-test leased ACC-ADM (ttl 150 ms), then died without releasing
next-test leased ACC-ADM after 1 expired lease(s) were reclaimed
crashing-test released its expired lease; ACC-ADM still held by next-test
```

The three rows are the whole argument: naive is fastest *because* it is broken, static is correct but
leaves half the pool idle, and the broker is correct while using all of it.

**Collisions** counts *tests that ever shared an account*, not overlapping moments, so it tops out at
`PARALLEL_TESTS`. Naive's `24` means every test was affected — not 24 separate incidents.

Reading the table:

- **Naive.** All 24 tests piled onto the first account that looked free — the `1 / 8`, the 24
  collisions and the low duration are one fact, not three. If you only measured duration, this is
  the strategy you would ship.
- **Static.** Zero collisions, but only `4 / 8` accounts: the run costs *(tests per id) × (work)*
  instead of *(tests / pool) × (work)*, so buying more accounts would not move it at all.
- **Broker.** Zero collisions on `8 / 8`, close to the 120 ms floor printed at the top. It cannot
  beat naive, because unlike naive it waits when the pool is full — that gap is what correctness
  costs, and it is far smaller than the gap to the merely-correct strategy.

Note that the naive row's 2 ms race window falls *inside* the timed region, so if anything its
duration is pessimistic. It still wins, which is the point.

### Reproducibility

Runs on the same machine agree exactly where it matters and only roughly where it does not. Collision
counts and accounts-used are identical every time — integer facts about who held what, kept that way
by no randomness, fixed strategy order and a discarded warm-up round per strategy. Durations move by
10–15%, and the **median of 5 timed rounds** narrows that without removing it.

The variance is `Thread.sleep(WORK_DURATION_MILLIS)`, not the strategies: on Windows a 40 ms sleep
measures 40–62 ms, because the scheduler tick is ~15.6 ms and 40 rounds up to three of them. Each row
is a chain of those sleeps — 1 deep for naive, 3 for the broker, 6 for static — so the rows land near
1x, 3x and 6x one sleep, and static swings widest because it stacks the most. Read the durations as a
ratio, not a millisecond count; the numbers in the table are one machine's samples.

The rest of the honesty checklist, since this table is the repo's main claim:

- **Collisions are counted by registration, not sampled.** Polling `isHeldBy` would miss a collision
  that opens and closes between polls — an undercount in the one column the whole argument rests on.
  `CollisionDetector` registers an owner when it starts using an account and deregisters it when it
  stops, so an overlap is caught by two owners being registered at once. It can only undercount,
  never invent a collision.
- **Identical work per row.** The timed body is one `Thread.sleep` in all three strategies; nothing
  strategy-specific runs inside it except acquire and release, which is the thing being compared.
- **Scored from outside.** The detector observes what the workload did, never how the strategy did
  it, so all three rows are judged by the same rules.
- **A round that loses a test throws.** It measured a smaller workload than the others, and a median
  would hide it. The check is a completion count, not just a caught-exception list — an `Error` never
  reaches the catch blocks, and would otherwise be folded into the median unnoticed.

One more disclosure: the static row's `4 / 8` accounts is set by `TESTS_PER_STATIC_ID`, a chosen
constant modelling a legacy config, not an emergent measurement.

## Tuning

All knobs are constants at the top of `src/main/java/demo/Benchmark.java`:

| Constant                    | Default | Meaning                                                  |
| --------------------------- | ------- | -------------------------------------------------------- |
| `POOL_SIZE`                 | 8       | accounts in the shared pool                               |
| `PARALLEL_TESTS`            | 24      | tests started simultaneously                              |
| `TESTS_PER_STATIC_ID`       | 6       | tests hard-coded to the same id (static strategy)         |
| `WORK_DURATION_MILLIS`      | 40      | pretend work per test (must stay under the spin limit)    |
| `NAIVE_RACE_WINDOW_MILLIS`  | 2       | width of the naive check-then-act window                  |
| `CAPABILITY`                | STANDARD| the tag every benchmark account carries                   |
| `LEASE_TTL_MILLIS`          | 5000    | broker lease TTL (must exceed the work duration)          |
| `ACQUIRE_TIMEOUT_MILLIS`    | 30000   | how long a test waits for the broker before failing       |
| `WARMUP_ROUNDS`             | 1       | discarded rounds per strategy                             |
| `MEASURED_ROUNDS`           | 5       | timed rounds per strategy; the median is reported         |

One bound sits outside that file: `DEFAULT_SPIN_LIMIT_MILLIS` in `NaiveSharedList` (30 s), a wedge
guard rather than a knob. A naive waiter whose peer holds an account for longer than that aborts with
`IllegalStateException`, and `Benchmark.runRound` turns a lost test into a failed run — taking the
static and broker rows and the TTL demo with it, not just the naive row. So raising
`WORK_DURATION_MILLIS` past 30 s means editing that constant too.

Three experiments worth running, with what they produced on the machine above (baseline: naive
45 ms, static 280 ms, broker 139 ms):

| Change | What happens |
| --- | --- |
| `POOL_SIZE` 8 → 16 | broker **139 → 92 ms**; naive and static unchanged. Only the broker can use a bigger pool. |
| `TESTS_PER_STATIC_ID` 6 → 12 | static **280 → 561 ms** on `2 / 8` accounts; naive and broker unchanged. Only the static row feels its own config. |
| `NAIVE_RACE_WINDOW_MILLIS` 2 → 0 | naive still collides (~18 tests), now scattered over `8 / 8` accounts in ~90 ms. |

The last one matters most: removing the window does **not** remove the bug. With 24 threads released
at once the unsynchronised scan races on its own. The window only makes the failure identical every
run — one account, every test — instead of a number that moves around.

## Layout

`src/main/java/demo/` holds the three strategies — `NaiveSharedList`, `StaticAssignment`,
`AccountBroker` — over a shared `Account` / `Lease` / `AccountStrategy` core, plus
`NoAccountAvailableException`, `Benchmark`, `CollisionDetector` and
`TtlReclaimDemo`. `src/test/java/demo/` holds the JUnit suite and the TestNG
usage example. Every class opens with a comment saying what it is for. `pom.xml` is optional.

## Caveats

- The benchmark measures a workload built out of `Thread.sleep`, not real I/O. It is a demonstration
  of *contention shape*, not a microbenchmark. The sleeps that remain in this repo all *model*
  something (pretend work, and the widened race window in strategy 1); none of them stands in for
  synchronisation — waiters use a `Condition`, retries use `Thread.yield`, and the tests use an
  injected clock.
- Deliberately out of scope: no logging framework, no CLI parsing, no external config, no
  persistence, no CI pipeline, no publishing setup, no fourth strategy. Configuration is the
  constants at the top of `Benchmark.java` and output is `System.out`, so the concurrency is what a
  reader spends their attention on.
- `capability` is a plain `String` rather than an enum or a type parameter. An enum would catch typos
  at compile time, but it would also pin the broker to one vocabulary, and being vocabulary-agnostic
  is the point. The up-front check in `acquire` recovers most of the benefit at run time.
- Two known gaps are accepted rather than fixed: the 20 ms reclaim poll wakes waiters more often than
  lease expiry actually requires (fixing it means real logic in the file whose job is to be obviously
  correct), and `AccountStrategy.release(Lease)` is public, so a caller can bypass try-with-resources
  if they insist. Narrowing it would make the interface less self-explanatory, and the interface is
  half the teaching material.
- The naive strategy's exact collision count is timing-dependent by nature. It should always be
  well above zero with the default race window; that is all the demo claims.

## License

MIT — see [LICENSE](LICENSE).
