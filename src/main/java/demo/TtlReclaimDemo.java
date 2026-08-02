package demo;

import java.util.List;

/** The broker's third idea, shown rather than measured: it needs a crashed owner to be interesting,
 *  so it does not fit the benchmark's uniform workload. */
final class TtlReclaimDemo {

    private static final long TTL_MILLIS = 150;

    private TtlReclaimDemo() {
    }

    static void run() throws InterruptedException {
        System.out.println("\nTTL reclaim demo (a test that never releases)");
        AccountBroker broker = new AccountBroker(
                List.of(new Account("ACC-STD", "STANDARD"), new Account("ACC-ADM", "ADMIN")),
                TTL_MILLIS, 5_000);

        Lease crashed = broker.acquire("crashing-test", "ADMIN"); // the free STANDARD one is skipped
        System.out.printf("crashing-test leased %s (ttl %d ms), then died without releasing%n",
                crashed.account().id(), TTL_MILLIS);

        Thread.sleep(TTL_MILLIS + 100);

        // No release() ever happened; the next acquirer reclaims the expired lease itself.
        Lease recovered = broker.acquire("next-test", "ADMIN");
        System.out.printf("next-test leased %s after %d expired lease(s) were reclaimed%n",
                recovered.account().id(), broker.reclaimedLeaseCount());

        // The crashed owner returns and releases a lease it lost long ago; the broker refuses.
        crashed.close();
        System.out.printf("crashing-test released its expired lease; %s still held by %s%n",
                recovered.account().id(), recovered.account().holder());
        recovered.close();
    }
}
