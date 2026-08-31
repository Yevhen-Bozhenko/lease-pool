package demo;

import io.github.yevhenbozhenko.pool.Resource;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/** The one claim about strategy 1 that the benchmark cannot make: its spin is bounded. */
class NaiveSharedListTest {

    @Test
    @DisplayName("the naive strategy aborts rather than spinning forever on a fully-held pool")
    // SEPARATE_THREAD because a regression here is an uninterruptible spin: an in-thread timeout is
    // only measured after the method returns, which it never would.
    @Timeout(value = 5, threadMode = SEPARATE_THREAD)
    void naiveSpinIsBounded() throws Exception {
        // 100 ms through the test seam, not the shipped 30 s: the guard is under test, not the wait.
        NaiveSharedList<String> naive = new NaiveSharedList<>(
                List.of(new Resource<>("ACC-01", "login-for-ACC-01")), 0, 100);
        naive.acquire("someone-else"); // never closed, and nothing here reclaims it

        IllegalStateException wedged =
                assertThrows(IllegalStateException.class, () -> naive.acquire("test-1"));
        assertTrue(wedged.getMessage().contains("test-1"),
                "the diagnostic must name who gave up: " + wedged.getMessage());
    }
}
