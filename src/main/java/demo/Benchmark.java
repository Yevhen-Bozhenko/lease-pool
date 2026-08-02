package demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/** Runs one identical workload against all three strategies and prints a comparison table:
 *  {@code PARALLEL_TESTS} threads start together, each leases an account, does
 *  {@code WORK_DURATION_MILLIS} of pretend work, and releases it.
 *
 *  <p>The collision and accounts-used columns are exact by construction — no randomness, fixed
 *  order, a discarded warm-up, a median. Durations still move 10-15%: that is the sleep in
 *  {@code runOneTest}, not the strategies, so read them as a ratio. See the README. */
public final class Benchmark {

    static final int POOL_SIZE = 8;
    /** Deliberately larger than the pool, as on real CI. */
    static final int PARALLEL_TESTS = 24;
    /** Tests hard-coded to the same account id in the static strategy. */
    static final int TESTS_PER_STATIC_ID = 6;
    static final long WORK_DURATION_MILLIS = 40;
    /** Widens the gap between checking and claiming in strategy 1, so its failure is repeatable. */
    static final long NAIVE_RACE_WINDOW_MILLIS = 2;
    static final long LEASE_TTL_MILLIS = 5_000;
    static final long ACQUIRE_TIMEOUT_MILLIS = 30_000;
    static final String CAPABILITY = "STANDARD";
    static final int WARMUP_ROUNDS = 1;
    /** Odd, so the median is a real measurement rather than an average of two. */
    static final int MEASURED_ROUNDS = 5;

    private Benchmark() {
    }

    public static void main(String[] args) throws Exception {
        List<String> tests = new ArrayList<>();
        for (int i = 0; i < PARALLEL_TESTS; i++) {
            tests.add(String.format("test-%02d", i));
        }
        System.out.printf("test-account-broker%npool=%d, parallel tests=%d, tests per static id=%d,"
                + " work=%d ms/test%n%d warm-up round + %d measured rounds, median reported%n"
                + "ideal wall clock for a fully shared pool: %d ms%n%n",
                POOL_SIZE, PARALLEL_TESTS, TESTS_PER_STATIC_ID, WORK_DURATION_MILLIS,
                WARMUP_ROUNDS, MEASURED_ROUNDS,
                ((PARALLEL_TESTS + POOL_SIZE - 1) / POOL_SIZE) * WORK_DURATION_MILLIS);
        System.out.printf("%-18s %11s %15s %15s   %s%n",
                "Strategy", "Collisions", "Duration (ms)", "Accounts used", "Notes");
        System.out.println("-".repeat(113));

        measureAndPrintRow("NaiveSharedList", "unsynchronised scan; check-then-act race", tests,
                () -> new NaiveSharedList(newPool(), NAIVE_RACE_WINDOW_MILLIS));
        measureAndPrintRow("StaticAssignment", "one fixed account per test; shared ids serialise",
                tests, () -> new StaticAssignment(newPool(), tests, TESTS_PER_STATIC_ID));
        measureAndPrintRow("AccountBroker", "atomic claim + acquire(capability) + TTL leases", tests,
                () -> new AccountBroker(newPool(), LEASE_TTL_MILLIS, ACQUIRE_TIMEOUT_MILLIS));
        TtlReclaimDemo.run();
    }

    /** One round's three columns. Measured together; each is reduced to a median independently. */
    private record Round(long collisions, long millis, long accountsUsed) {
    }

    private static void measureAndPrintRow(String name, String notes, List<String> tests,
            Supplier<AccountStrategy> newStrategy) throws Exception {
        for (int i = 0; i < WARMUP_ROUNDS; i++) {
            runRound(newStrategy.get(), tests); // discarded
        }
        List<Round> rounds = new ArrayList<>();
        for (int i = 0; i < MEASURED_ROUNDS; i++) {
            rounds.add(runRound(newStrategy.get(), tests));
        }
        System.out.printf("%-18s %11d %15d %10d / %-2d   %s%n", name,
                median(rounds, Round::collisions), median(rounds, Round::millis),
                median(rounds, Round::accountsUsed), POOL_SIZE, notes);
    }

    /** Each column is taken independently: they answer separate questions about the same run. */
    private static long median(List<Round> rounds, ToLongFunction<Round> column) {
        long[] sorted = rounds.stream().mapToLong(column).sorted().toArray();
        return sorted[sorted.length / 2];
    }

    private static Round runRound(AccountStrategy strategy, List<String> tests) throws Exception {
        CollisionDetector detector = new CollisionDetector();
        Set<String> failures = ConcurrentHashMap.newKeySet();
        Set<String> completed = ConcurrentHashMap.newKeySet();
        // The gate makes every thread hit the pool at once; trickling in hides the naive race.
        CountDownLatch ready = new CountDownLatch(tests.size());
        CountDownLatch start = new CountDownLatch(1);
        long startNanos;

        try (ExecutorService executor = Executors.newFixedThreadPool(tests.size())) {
            for (String test : tests) {
                executor.execute(() -> {
                    try {
                        ready.countDown();
                        start.await();
                        runOneTest(strategy, test, detector);
                        completed.add(test);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        failures.add(test + ": interrupted");
                    } catch (RuntimeException e) {
                        // toString(), not getMessage(): otherwise a timeout and an NPE read alike.
                        failures.add(test + ": " + e);
                    }
                });
            }
            ready.await(); // every thread parked: do not time pool start-up
            startNanos = System.nanoTime();
            start.countDown();
        } // close() waits for every task to finish
        long millis = (System.nanoTime() - startNanos) / 1_000_000;

        // A round that lost a test measured less work, and the median would hide it. The completion
        // count is what makes this airtight: an Error never reaches the catches above.
        if (!failures.isEmpty() || completed.size() != tests.size()) {
            throw new IllegalStateException(strategy.getClass().getSimpleName() + ": only "
                    + completed.size() + " of " + tests.size() + " tests completed"
                    + (failures.isEmpty() ? "" : ", e.g. " + failures.iterator().next()));
        }
        return new Round(detector.collidedOwnerCount(), millis, detector.accountsUsed());
    }

    private static void runOneTest(AccountStrategy strategy, String test,
            CollisionDetector detector) throws InterruptedException {
        try (Lease lease = strategy.acquire(test)) {
            String id = lease.account().id();
            detector.recordAcquired(id, test);
            try {
                // Identical in all three rows, so only acquire and release differ. Also the
                // variance floor: 40 ms of sleep costs 40-62 ms against Windows' ~15.6 ms tick.
                Thread.sleep(WORK_DURATION_MILLIS);
            } finally {
                detector.recordReleased(id, test);
            }
        }
    }

    /** Called once per round, so no round inherits another's state. */
    private static List<Account> newPool() {
        List<Account> accounts = new ArrayList<>();
        for (int i = 1; i <= POOL_SIZE; i++) {
            accounts.add(new Account(String.format("ACC-%02d", i), CAPABILITY));
        }
        return accounts;
    }
}
