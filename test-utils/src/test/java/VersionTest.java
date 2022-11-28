import com.couchbase.client.test.ClusterVersion;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VersionTest {
    private static final ClusterVersion version6_0_0 = new ClusterVersion(6, 0, 0, false);
    private static final ClusterVersion version7_0_0 = new ClusterVersion(7, 0, 0, false);
    private static final ClusterVersion version7_1_0 = new ClusterVersion(7, 1, 0, false);
    private static final ClusterVersion version7_1_1 = new ClusterVersion(7, 1, 1, false);

    @Test
    void test1() {
        assertTrue(version7_0_0.isGreaterThan(version6_0_0));
        assertFalse(version7_1_0.isGreaterThan(version7_1_0));
        assertTrue(version7_1_1.isGreaterThan(version7_1_0));
        assertFalse(version7_1_0.isGreaterThan(version7_1_1));
        assertFalse(version7_0_0.isGreaterThan(version7_1_0));
        assertFalse(version7_0_0.isGreaterThan(version7_1_1));
        assertFalse(version6_0_0.isGreaterThan(version7_0_0));
    }
}
