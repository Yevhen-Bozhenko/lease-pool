package demo;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** The intended usage in a real test framework: one broker for the whole suite, one leased account
 *  per test method, returned however the test ends. Illustration only — compiled so it cannot rot,
 *  but excluded from {@code mvn test}. The annotations are the only framework-specific part; a
 *  JUnit extension has the same shape. */
public class BrokerTestNgExample {

    /** One broker per suite: the pool is a suite-level resource. The TTL is the safety net — if this
     *  JVM is killed mid-test, the accounts free themselves instead of staying locked. */
    private static final AccountBroker ACCOUNTS = new AccountBroker(
            List.of(new Account("acct-01", "shopper"),
                    new Account("acct-02", "shopper"),
                    new Account("acct-03", "merchant")),
            TimeUnit.MINUTES.toMillis(5),    // lease TTL
            TimeUnit.MINUTES.toMillis(1));   // how long a test waits for a free account

    /** ThreadLocal because TestNG may run methods in parallel: one lease per test thread. */
    private final ThreadLocal<Lease> lease = new ThreadLocal<>();

    private final AtomicLong invocation = new AtomicLong();

    @BeforeMethod
    public void leaseAccount(Method method) throws InterruptedException {
        // "shopper" is this suite's own vocabulary; the broker attaches no meaning to it. The method
        // name makes a stuck lease easy to trace, and the counter keeps the label unique — two live
        // leases sharing a name would let one test's stale close() free the other's account.
        lease.set(ACCOUNTS.acquire(method.getName() + "#" + invocation.incrementAndGet(), "shopper"));
    }

    @AfterMethod(alwaysRun = true) // alwaysRun: a failing test must still return its account
    public void returnAccount() {
        Lease held = lease.get();
        if (held != null) {
            held.close();
            lease.remove();
        }
    }

    private Account account() {
        return lease.get().account();
    }

    @Test
    public void checkoutUsesItsOwnAccount() {
        System.out.println("checkout running as " + account().id());
    }

    @Test
    public void refundUsesItsOwnAccount() {
        System.out.println("refund running as " + account().id());
    }
}
