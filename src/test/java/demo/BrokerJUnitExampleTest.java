package demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.github.yevhenbozhenko.pool.Lease;
import io.github.yevhenbozhenko.pool.LeaseBroker;
import io.github.yevhenbozhenko.pool.Resource;
import io.github.yevhenbozhenko.pool.Selector;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

/** The same wiring as {@link BrokerTestNgExample}, for JUnit 5. Copy the extension into your own
 *  project: shipping it in the library would put JUnit on the library's compile path. The name ends
 *  in Test so surefire runs it, which is what keeps the example from rotting. */
@ExtendWith(BrokerJUnitExampleTest.LeasedAccount.class)
class BrokerJUnitExampleTest {

    private record Login(String user, String password) {
    }

    /** Acquire before each test, hand the payload to the method, release after. */
    static final class LeasedAccount
            implements BeforeEachCallback, AfterEachCallback, ParameterResolver {

        private static final List<Resource<Login>> POOL = List.of(
                new Resource<>("acct-01", Set.of("shopper", "eu"), new Login("u1", "p1")),
                new Resource<>("acct-02", Set.of("shopper", "eu", "vat"), new Login("u2", "p2")),
                new Resource<>("acct-03", Set.of("merchant"), new Login("u3", "p3")));

        /** One broker per suite. The TTL is the safety net if this JVM is killed mid-test. */
        private static final LeaseBroker<Login> ACCOUNTS = new LeaseBroker<>(POOL,
                TimeUnit.MINUTES.toMillis(5),    // lease TTL
                TimeUnit.MINUTES.toMillis(1));   // how long a test waits for a free account

        private static final ExtensionContext.Namespace NS =
                ExtensionContext.Namespace.create(LeasedAccount.class);

        /** A non-generic key: Store.get cannot express Lease&lt;Login&gt;.class. */
        private record Held(Lease<Login> lease) {
        }

        @Override
        public void beforeEach(ExtensionContext context) throws InterruptedException {
            // getUniqueId is distinct per test and per parallel invocation, which is what acquire
            // asks of concurrent holders and what lets holderOf name the run holding an account.
            context.getStore(NS).put(Held.class,
                    new Held(ACCOUNTS.acquire(context.getUniqueId(), Selector.tagged("shopper"))));
        }

        @Override
        public void afterEach(ExtensionContext context) {
            Held held = context.getStore(NS).remove(Held.class, Held.class);
            if (held != null) {
                held.lease().close();
            }
        }

        @Override
        public boolean supportsParameter(ParameterContext parameter, ExtensionContext context) {
            return parameter.getParameter().getType() == Login.class
                    && context.getTestMethod().isPresent();
        }

        @Override
        public Object resolveParameter(ParameterContext parameter, ExtensionContext context) {
            return context.getStore(NS).get(Held.class, Held.class).lease().get();
        }
    }

    @AfterAll
    static void everyLeaseWasReturned() {
        for (String id : LeasedAccount.POOL.stream().map(Resource::id).toList()) {
            assertEquals(Optional.empty(), LeasedAccount.ACCOUNTS.holderOf(id), id);
        }
    }

    @Test
    void checkoutGetsALeasedAccount(Login login) {
        assertNotNull(login.user());
    }

    @Test
    void refundGetsALeasedAccount(Login login) {
        assertNotNull(login.user());
    }
}
