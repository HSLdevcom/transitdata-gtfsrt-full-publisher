package fi.hsl.transitdata.publisher.sink;

import org.junit.Test;

import static fi.hsl.transitdata.publisher.TestData.config;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class AzureSinkTest {
    @Test
    public void storageKeyIsReadFromEnvironmentNotFromConfig() throws Exception {
        String key = System.getenv("TRANSITDATA_AZURE_STORAGE_KEY");
        assumeTrue(key == null || key.isEmpty());

        try {
            // bundler.output.azure.accountKeyPath is not used for the key
            AzureSink.newInstance(config("bundler.output.azure.accountKeyPath", "c29tZS1rZXk="));
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Azure storage key not found.", e.getMessage());
        }
    }
}
