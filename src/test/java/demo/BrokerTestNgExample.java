package demo;

import io.github.yevhenbozhenko.pool.Lease;
import io.github.yevhenbozhenko.pool.LeaseBroker;
import io.github.yevhenbozhenko.pool.Resource;
import io.github.yevhenbozhenko.pool.Selector;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** The intended usage in a real test framework: one broker for the whole suite, one leased resource
 *  per test method, returned however the test ends. Illustration only — compiled so it cannot rot,
 *  but excluded from {@code mvn test}. The annotations are the only framework-specific part; a
 *  JUnit extension has the same shape. */
public class BrokerTestNgExample {

    /** Whatever your tests actually need once they hold an account. The broker never looks inside
     *  it, which is the whole reason the payload is a type parameter. */
    private record Login(String user, String password) {
    }

    /** One broker per suite: the pool is a suite-level resource. The TTL is the safety net — if this
     *  JVM is killed mid-test, the leases expire instead of staying held forever. */
    private static final LeaseBroker<Login> ACCOUNTS = new LeaseBroker<>(
            List.of(new Resource<>("acct-01", Set.of("shopper", "eu"), new Login("u1", "p1")),
                    new Resource<>("acct-02", Set.of("shopper", "eu", "vat"), new Login("u2", "p2")),
                    new Resource<>("acct-03", Set.of("merchant"), new Login("u3", "p3"))),
            TimeUnit.MINUTES.toMillis(5),    // lease TTL
            TimeUnit.MINUTES.toMillis(1));   // how long a test waits for a free account

    /** ThreadLocal because TestNG may run methods in parallel: one lease per test thread. */
    private final ThreadLocal<Lease<Login>> lease = new ThreadLocal<>();

    private final AtomicLong invocation = new AtomicLong();

    @BeforeMethod
    public void leaseAccount(Method method) throws InterruptedException {
        // "shopper" is this suite's own vocabulary; the broker attaches no meaning to it. The method
        // name makes a stuck lease easy to trace, and the counter keeps the label unique per
        // invocation, which is what ResourcePool.acquire asks of concurrent holders and what lets
        // holderOf name the exact run sitting on an account. Sharing a label would not let one
        // test's stale close() free another's lease: release matches on lease identity.
        lease.set(ACCOUNTS.acquire(method.getName() + "#" + invocation.incrementAndGet(),
                Selector.tagged("shopper")));
    }

    @AfterMethod(alwaysRun = true) // alwaysRun: a failing test must still return its account
    public void returnAccount() {
        Lease<Login> held = lease.get();
        if (held != null) {
            held.close();
            lease.remove();
        }
    }

    @Test
    public void checkoutUsesItsOwnAccount() {
        Login login = lease.get().get();
        System.out.println("checkout running as " + login.user());
    }

    @Test
    public void refundUsesItsOwnAccount() {
        System.out.println("refund running as " + lease.get().get().user());
    }
}
