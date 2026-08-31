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

/** The intended usage in a real test framework: one broker per suite, one leased resource per test
 *  method, returned however the test ends. Nothing runs this file: it is not named {@code ...Test}
 *  and there is no TestNG engine. See {@link BrokerJUnitExampleTest} for the JUnit 5 shape. */
public class BrokerTestNgExample {

    private record Login(String user, String password) {
    }

    /** One broker per suite. The TTL is the safety net if this JVM is killed mid-test. */
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
        // The counter keeps the owner label unique per invocation, which is what acquire asks for.
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
