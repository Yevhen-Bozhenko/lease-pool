# lease-pool

A small, dependency-free Java library for handing a limited pool of shared resources to parallel
workers: **reserve-if-free**, so two workers never hold the same resource, and **expiring leases**, so
a worker that never releases cannot shrink the pool. JDK 21+, six files, no dependencies.

The repository is also its own evidence. The library is benchmarked against the two strategies teams
reach for first, on one identical workload — see **[Example output](#example-output)**: the naive one
collides on all 24 tests, the other two never collide, and the broker does it in half the static
strategy's wall clock while using the whole pool instead of half of it.

> This is a **generic, illustrative reference implementation of a common pattern**. It is not tied to
> any specific system, product or codebase, and it contains no real credentials or endpoints. All
> numbers below are **illustrative and machine-dependent** (they depend on your CPU, core count, JVM
> and OS timer granularity — on Windows in particular `Thread.sleep` rounds up to ~15 ms, which
> inflates every duration). Run it yourself; the *shape* of the result is the point, not the digits.

## When this pattern applies

`LeaseBroker` is a **reserve-if-free pool with expiring leases**, and nothing in it is specific to
test accounts. It fits wherever three things are true at once: the resource is shared, holding it is
mutually exclusive, and the pool is smaller than the number of workers that want one. That third
condition is what turns a correctness problem into a throughput problem — with enough resources to go
round every strategy looks fine, which is why these bugs surface only under parallelism.

Same shape, different domain: seats on a licensed tool, sandbox tenants or database schemas handed to
parallel CI jobs, rate-limited API keys, devices in a hardware lab, staging environments, serial
ports. The two failure modes in [The problem](#the-problem) reappear unchanged in all of them.

The two ideas are separable, and you may need only one. Reserve-if-free is what makes a pool
*correct*, so it is never optional. TTL leases make it *self-healing*, which matters once the pool
outlives a single run — over a long enough window, some holder always dies without releasing.

### Limits

Single-process and in-memory. Correctness rests on one `ReentrantLock`, which reaches exactly as far
as one JVM's heap, and the leases live in fields that die with the process.

**This is not a distributed lock.** Two brokers over the same accounts keep separate books and will
double-issue. If your workers span machines, containers or JVMs the pattern still holds but the
mechanism does not — the reserve and the expiry have to live somewhere every worker can see them: a
conditional `UPDATE` on a database row, Redis `SET NX PX`, or ZooKeeper/etcd leases. The API shape
survives that move; the lock does not.

## Using the broker in your own project

The broker knows nothing about tests, and nothing about accounts either. Three pieces of vocabulary
are yours to define:

- **The payload `T`** is whatever your callers actually need once they hold one — a login, a JDBC
  url, a device handle. The broker never looks inside it, so `lease.get()` hands your own type back
  and you keep no lookup table on the side.
- **Tags** are free-form strings describing how your resources differ: `"shopper"`, `"eu-vat"`,
  `"readonly"`. A resource carries a set of them, and this library never checks them against a list
  of allowed values. That is what keeps it out of your domain.
- **An owner** is any label you want to see in a diagnostic.

The public surface is acquire, release, and a lease TTL.

```java
record Login(String user, String password) { }   // your type, not the library's

// One broker per suite: the pool is a suite-level resource.
LeaseBroker<Login> accounts = new LeaseBroker<>(
        List.of(new Resource<>("acct-01", Set.of("shopper", "eu"), new Login("u1", "p1")),
                new Resource<>("acct-02", Set.of("shopper", "eu", "vat"), new Login("u2", "p2")),
                new Resource<>("acct-03", Set.of("merchant"), new Login("u3", "p3"))),
        TimeUnit.MINUTES.toMillis(5),    // lease TTL: the safety net for a killed JVM
        TimeUnit.MINUTES.toMillis(1));   // how long a caller waits for a free account

try (Lease<Login> lease = accounts.acquire("checkout-test", Selector.tagged("shopper"))) {
    logIn(lease.get().user(), lease.get().password());
    ...
}   // released here however the block exits
```

`Selector.tagged("shopper", "vat")` matches a resource carrying **all** of those tags. Extra tags
never disqualify, so `acct-02` above answers a request for `"shopper"` and a request for `"vat"`
alike, and adding a tag to a resource cannot break an existing caller. `Selector` is a functional
interface, so a lambda over the tag set works where the built-ins do not fit. `acquire(owner)` with
no selector matches any free resource.

Pick the TTL to sit comfortably above the longest a caller can legitimately hold a resource: too low
and live work gets its resource reclaimed underneath it, too high and a crashed run locks resources
until somebody notices.

Getting that margin wrong is quiet: a reclaim is invisible to the owner it happened to, so a caller
whose work outran the TTL keeps driving a resource somebody else now holds. Leave real headroom.

### When acquire fails

The two ways it can fail need opposite responses, so they are different types — no message parsing
needed to tell them apart.

**`NoResourceAvailableException`** — the pool was saturated: every matching resource stayed held for
the whole acquire timeout. Retryable; back off and try again, or fail the test with a clear "no
capacity" reason. Carries `owner()`, `selector()` and `waited()`.

**`IllegalArgumentException`** — nothing in the pool matches that selector at all. Never retry;
waiting cannot fix it. Usually a typo, sometimes a tag that environment never provisioned, sometimes
a combination of tags no single resource carries. Thrown *before* any waiting, so it is instant, and
it names both what was asked for and the tags the pool does offer.

```java
try (Lease<Login> lease = accounts.acquire("checkout-test", Selector.tagged("shopper"))) {
    ...
} catch (NoResourceAvailableException e) {
    log("nothing matching %s for %s after %d ms".formatted(e.selector(), e.owner(),
            e.waited().toMillis()));
    throw e;
}
```

The check is asked of the pool, not of a tag list, so it works for a hand-written `Selector` too. In
exchange, a selector has to be a pure function of the tag set: one that changes its mind turns that
instant failure into a silent wait, and it needs a `toString()`, since both messages above print the
selector to say what was asked for and a lambda inherits one that names nothing.

Two files show the same thing wired into a real framework, leasing an account around each test
method and returning it however the test ends.

`src/test/java/demo/BrokerJUnitExampleTest.java` holds a JUnit 5 extension: `beforeEach` acquires,
`afterEach` releases, and a `ParameterResolver` hands the payload to the test method as an argument,
so the test itself never mentions the pool. The extension is the nested `LeasedAccount` class, and
it belongs in your project rather than in the library, since shipping it would put JUnit on the
library's compile path. The file is named `...Test`, so surefire runs it and the example cannot rot
unnoticed.

`src/test/java/demo/BrokerTestNgExample.java` is the same shape in TestNG, with
`@BeforeMethod`/`@AfterMethod` and `alwaysRun` on the teardown so a failing test still returns its
account. It is compiled so it cannot rot, but two things stop it running: its name is not `...Test`,
so surefire never scans it, and the JUnit Platform that surefire runs here has no engine for
TestNG's annotations.

## The problem

Test accounts are the running example from here on, because they show both failures at once;
substitute your own resource freely. There are never enough of them, so a pool is shared. Once the
suite runs in parallel, the pool — not the hardware — caps how fast it can go, and you face two
independent failure modes:

1. **Collisions** — two tests use the same account at the same time. Symptoms are flaky,
   unreproducible failures: your fixture data changes underneath you, a logout in one test breaks
   another, an assertion sees a state no test created.
2. **Under-utilisation** — tests wait for accounts that are not actually busy. Nothing fails; the
   suite is just slow, and adding accounts to the pool does not help.

A good strategy has to solve *both*. It is easy to solve either one alone, which is exactly why the
bad strategies survive so long.

## The three strategies

All three implement the same interface, `ResourcePool<T>`, and the benchmark runs the identical
workload against each. `acquire(owner)` hands back a `Lease<T>`; closing the lease returns the
account. `Lease` is `AutoCloseable`, so a try-with-resources block releases it even when the test
throws — which is why that is preferred over calling `release(lease)` by hand.

Three rather than two, because the interesting comparison looks like naive vs. broker and isn't.
Naive is the *fastest* row, so a two-row demo says "correctness costs you 3×" — the wrong lesson, and
wrong because naive is fast only by being broken. `StaticAssignment` is the row that makes the
argument work: it is the only strategy the broker actually beats on wall clock, the only
demonstration of under-utilisation — which is what the tag selector exists to fix — and the
realistic baseline, since hard-coded per-test account ids is what teams actually have.

### 1. `NaiveSharedList` — fast and wrong

Scan a shared list, take the first account that looks free. No locking:

```java
if (!slot.isInUse()) {   // (1) CHECK — free right now
    ...                  //     ...nothing stops other threads getting here too
    slot.forceHold(me);  // (2) ACT   — claim it
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

### 3. `LeaseBroker` — safe and fast

Three changes, one per problem:

- **Indivisible reserve-if-free.** "Is it free?" and "it's mine now" happen in one critical section
  under the broker's lock, and the holder field lives in a private slot nothing outside the broker
  can reach, so two callers can never take the same account. Fixes strategy 1.
- **`acquire(owner, selector)`.** A caller asks for *any* free account carrying the tags it needs
  instead of naming an id, so every account is usable by every caller. Fixes strategy 2.
- **TTL leases.** Every claim carries an expiry. An owner that crashes, hangs or is killed without
  releasing has its lease reclaimed automatically, so one dead test cannot permanently shrink the
  pool. Reclaim runs on the acquiring thread under the lock — no background thread, nothing to shut
  down.

Waiters block on a `Condition` rather than spinning, and the lock is **fair**, so no caller barges
past callers already queued for it. That is a guarantee about the lock and not about the resource:
a waiter whose reclaim poll expires goes back to the tail of the queue, so callers that arrived
later are routinely served first. Waiters keep making progress — the acquire timeout is what bounds
the wait, not the ordering.

## Running it

Plain JDK (21+), no build tool:

```bash
javac -d out src/main/java/io/github/yevhenbozhenko/pool/*.java src/main/java/demo/*.java
java -cp out demo.Benchmark
```

On Windows PowerShell, use backslashes in those two source paths.

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

Thirteen JUnit cases run. Eleven pin the claims this README makes — nine on the broker, one on the
naive strategy's spin bound, one on the static strategy refusing a lease over a resource it never
issued. The other two are the JUnit usage example above; they pin only that the wiring compiles and
runs, while an `@AfterAll` beside them asserts every lease came back, which is the part that catches
a regression. Each of the eleven carries a `@DisplayName` stating
what it checks, so `LeaseBrokerTest.java` reads as a list of the guarantees.
It sits in the library's own package and touches nothing from `demo`, so the library is testable by
itself.

```bash
mvn test
```

They exist because the benchmark *cannot* catch some of these: it runs a 5 s lease TTL against 40 ms
of work, so the stale-release hazard stays invisible however many times you run it.

Three of the TTL cases advance a fake clock by hand rather than sleeping past the expiry: a sleep long
enough to be reliable on a loaded CI agent is also long enough to slow the suite, and it still only
makes the expiry *probable*. That clock drives lease expiry only — the acquire deadline stays on the
real clock, so a regression in TTL reclaim *fails* these tests instead of hanging them.

The fourth sleeps because it has to: a frozen clock cannot drive the timed `await` it exists to pin.
The suite runs in about a second, and both test dependencies are test-scoped, so the demo has zero
runtime dependencies.

## Example output

Illustrative run on an 8-core Windows laptop, Temurin JDK 21:

```
lease-pool
pool=8, parallel tests=24, tests per static id=6, work=40 ms/test
1 warm-up round + 5 measured rounds, median reported
ideal wall clock for a fully shared pool: 120 ms

Strategy            Collisions   Duration (ms)  Resources used   Notes
-----------------------------------------------------------------------------------------------------------------
NaiveSharedList             24              45          1 / 8    unsynchronised scan; check-then-act race
StaticAssignment             0             280          4 / 8    one fixed resource per test; shared ids serialise
LeaseBroker                  0             139          8 / 8    claim under one lock + selector + TTL leases

TTL reclaim demo (a test that never releases)
crashing-test leased ACC-ADM (adm@example.test, ttl 150 ms), then crashed without releasing
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
counts and resources-used are identical every time — integer facts about who held what, kept that way
by no randomness, fixed strategy order and a discarded warm-up round per strategy. Durations move by
10–15%, and the **median of 5 timed rounds** narrows that without removing it.

The variance is `Thread.sleep(WORK_DURATION_MILLIS)`, not the strategies: on Windows a 40 ms sleep
measures 40–62 ms, because the scheduler tick is ~15.6 ms and 40 rounds up to three of them. Each row
is a chain of those sleeps — 1 deep for naive, 3 for the broker, 6 for static — so the rows land near
1x, 3x and 6x one sleep, and static swings widest because it stacks the most. Read the durations as a
ratio, not a millisecond count; the numbers in the table are one machine's samples.

The rest of the honesty checklist, since this table is the repo's main claim:

- **Collisions are counted by registration, not sampled.** Polling the holder would miss a collision
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

| Constant                   | Default  | Meaning                                                |
| -------------------------- | -------- | ------------------------------------------------------ |
| `POOL_SIZE`                | 8        | accounts in the shared pool                            |
| `PARALLEL_TESTS`           | 24       | tests started simultaneously                           |
| `TESTS_PER_STATIC_ID`      | 6        | tests hard-coded to the same id (static strategy)      |
| `WORK_DURATION_MILLIS`     | 40       | pretend work per test (must stay under the spin limit) |
| `NAIVE_RACE_WINDOW_MILLIS` | 2        | width of the naive check-then-act window               |
| `TAG`                      | standard | the tag every benchmark resource carries               |
| `LEASE_TTL_MILLIS`         | 5000     | broker lease TTL (must exceed the work duration)       |
| `ACQUIRE_TIMEOUT_MILLIS`   | 30000    | how long a test waits for the broker before failing    |
| `WARMUP_ROUNDS`            | 1        | discarded rounds per strategy                          |
| `MEASURED_ROUNDS`          | 5        | timed rounds per strategy; the median is reported      |

One bound sits outside that file: `DEFAULT_SPIN_LIMIT_MILLIS` in `NaiveSharedList` (30 s), a wedge
guard rather than a knob. A naive waiter whose peer holds an account for longer than that aborts with
`IllegalStateException`, and `Benchmark.runRound` turns a lost test into a failed run — taking the
static and broker rows and the TTL demo with it, not just the naive row. So raising
`WORK_DURATION_MILLIS` past 30 s means editing that constant too.

Three experiments worth running, with what they produced on the machine above (baseline: naive
45 ms, static 280 ms, broker 139 ms):

| Change                           | What happens                                                                                                       |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `POOL_SIZE` 8 → 16               | broker **139 → 92 ms**; naive and static unchanged. Only the broker can use a bigger pool.                         |
| `TESTS_PER_STATIC_ID` 6 → 12     | static **279 → 561 ms** on `2 / 8` accounts; naive and broker unchanged. Only the static row feels its own config. |
| `NAIVE_RACE_WINDOW_MILLIS` 2 → 0 | naive still collides (~18 tests), now scattered over `8 / 8` accounts in ~90 ms.                                   |

The last one matters most: removing the window does **not** remove the bug. With 24 threads released
at once the unsynchronised scan races on its own. The window only makes the failure identical every
run — one account, every test — instead of a number that moves around.

## Layout

Two packages, and the split is the point. Take the first one and leave the second.

`src/main/java/io/github/yevhenbozhenko/pool/` **is the library**: `Resource`, `Selector`, `Lease`,
the `ResourcePool` interface, `LeaseBroker`, and `NoResourceAvailableException`. Six files, no
dependencies, nothing about tests or accounts in any of them.

`src/main/java/demo/` **is the demonstration of it**: the two flawed strategies `NaiveSharedList` and
`StaticAssignment`, their shared `Slot`, plus `Benchmark`, `CollisionDetector` and `TtlReclaimDemo`.
It consumes the library exactly as your own project would.

Tests follow the same line. `src/test/java/io/github/yevhenbozhenko/pool/LeaseBrokerTest.java` tests
the library alone; `src/test/java/demo/` holds one test per flawed strategy and the two framework
usage examples. Every class opens with a comment saying what it is for. `pom.xml` is optional.

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
- Tags are plain `String`s rather than an enum. An enum would catch typos at compile time, but it
  would also pin the broker to one vocabulary, and being vocabulary-agnostic is the point. The
  up-front check in `acquire` recovers most of the benefit at run time. The payload is a type
  parameter for the opposite reason: it is the one thing that *is* yours, so the library should
  never see inside it.
- Two known gaps are accepted rather than fixed: the 20 ms reclaim poll wakes waiters more often than
  lease expiry actually requires (fixing it means real logic in the file whose job is to be obviously
  correct), and `ResourcePool.release(Lease)` is public, so a caller can bypass try-with-resources
  if they insist. Narrowing it would make the interface less self-explanatory, and the interface is
  half the teaching material. The poll is wasteful but it is not optional: `reclaimExpiredLeases`
  never signals the condition, so the poll is the only way a parked waiter ever notices an expired
  lease. Widening it to the full remaining timeout leaves that waiter asleep until the acquire times
  out; `parkedWaiterIsWokenByTheReclaimPoll` is the case that catches it.
- The naive strategy's exact collision count is timing-dependent by nature. It should always be
  well above zero with the default race window; that is all the demo claims.

## License

MIT — see [LICENSE](LICENSE).
