package demo;

import io.github.yevhenbozhenko.pool.Lease;
import io.github.yevhenbozhenko.pool.LeaseBroker;
import io.github.yevhenbozhenko.pool.Resource;
import io.github.yevhenbozhenko.pool.Selector;
import java.util.List;
import java.util.Set;

/** The broker's third idea, shown rather than measured: it needs an owner that crashes, which the
 *  benchmark's uniform workload has no room for. Also the one place you can see the payload: a
 *  lease hands back the caller's own object, not an id to look up. */
final class TtlReclaimDemo {

    private static final long TTL_MILLIS = 150;

    private TtlReclaimDemo() {
    }

    static void run() throws InterruptedException {
        System.out.println("\nTTL reclaim demo (a test that never releases)");
        LeaseBroker<String> broker = new LeaseBroker<>(
                List.of(new Resource<>("ACC-STD", Set.of("standard"), "std@example.test"),
                        // Two tags: an admin account is also a perfectly good standard one.
                        new Resource<>("ACC-ADM", Set.of("standard", "admin"), "adm@example.test")),
                TTL_MILLIS, 5_000);

        Lease<String> crashed = broker.acquire("crashing-test", Selector.tagged("admin"));
        System.out.printf("crashing-test leased %s (%s, ttl %d ms), then crashed without releasing%n",
                crashed.resource().id(), crashed.get(), TTL_MILLIS);

        Thread.sleep(TTL_MILLIS + 100);

        // No release() ever happened; the next acquirer reclaims the expired lease itself.
        Lease<String> recovered = broker.acquire("next-test", Selector.tagged("admin"));
        System.out.printf("next-test leased %s after %d expired lease(s) were reclaimed%n",
                recovered.resource().id(), broker.reclaimedLeaseCount());

        // The crashed owner returns and releases a lease it lost long ago; the broker refuses.
        crashed.close();
        System.out.printf("crashing-test released its expired lease; %s still held by %s%n",
                recovered.resource().id(),
                broker.holderOf(recovered.resource().id()).orElse("nobody"));
        recovered.close();
    }
}
