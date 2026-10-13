package fi.hsl.transitdata.publisher.sink;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobProperties;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Constructor;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * {@link AzureSink} against the Azurite Blob Storage emulator.
 * <p>
 * AzureSink always builds an {@code https://<account>.blob.core.windows.net} connection string. The test appends an
 * explicit {@code BlobEndpoint} to the account key, which the Azure SDK prefers over the derived endpoint, so the real
 * upload code runs against Azurite unchanged.
 */
public class AzureSinkIT {
    private static final String ACCOUNT = "devstoreaccount1";
    // Azurite's well-known development key
    private static final String KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    private static final int BLOB_PORT = 10000;

    @ClassRule
    public static final GenericContainer<?> AZURITE = new GenericContainer<>(
            DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:3.34.0"))
            .withCommand("azurite-blob", "--blobHost", "0.0.0.0", "--skipApiVersionCheck", "--loose")
            .withExposedPorts(BLOB_PORT).waitingFor(Wait.forListeningPort());

    private static String blobEndpoint;
    private static BlobServiceClient verifier;

    private String container;

    @BeforeClass
    public static void setUpClient() {
        blobEndpoint = "http://" + AZURITE.getHost() + ":" + AZURITE.getMappedPort(BLOB_PORT) + "/" + ACCOUNT;
        verifier = new BlobServiceClientBuilder().connectionString("DefaultEndpointsProtocol=http;AccountName="
                + ACCOUNT + ";AccountKey=" + KEY + ";BlobEndpoint=" + blobEndpoint).buildClient();
    }

    @Before
    public void uniqueContainer() {
        container = "c" + UUID.randomUUID().toString().replace("-", "");
    }

    private static AzureSink sink(long cacheMaxAgeSecs) throws Exception {
        Constructor<AzureSink> constructor = AzureSink.class.getDeclaredConstructor(String.class, String.class,
                long.class);
        constructor.setAccessible(true);
        return constructor.newInstance(ACCOUNT, KEY + ";BlobEndpoint=" + blobEndpoint, cacheMaxAgeSecs);
    }

    @Test
    public void createsMissingContainerAndUploadsWithHeaders() throws Exception {
        AzureSink sink = sink(5);
        assertFalse(verifier.getBlobContainerClient(container).exists());

        long before = System.nanoTime();
        sink.put(container, "HSL_TripUpdates_Full.pb", new byte[]{1, 2, 3});

        assertTrue(verifier.getBlobContainerClient(container).exists());
        BlobClient blob = verifier.getBlobContainerClient(container).getBlobClient("HSL_TripUpdates_Full.pb");
        assertArrayEquals(new byte[]{1, 2, 3}, blob.downloadContent().toBytes());
        BlobProperties properties = blob.getProperties();
        assertEquals("application/x-protobuf", properties.getContentType());
        assertEquals("max-age=5", properties.getCacheControl());
        assertTrue(sink.getLastPublishTime() >= before);
    }

    @Test
    public void overwritesExistingBlobInExistingContainer() throws Exception {
        AzureSink sink = sink(1);
        sink.put(container, "f.pb", new byte[]{1, 2, 3, 4, 5});
        sink.put(container, "f.pb", new byte[]{9});

        BlobClient blob = verifier.getBlobContainerClient(container).getBlobClient("f.pb");
        assertArrayEquals(new byte[]{9}, blob.downloadContent().toBytes());
        assertEquals("max-age=1", blob.getProperties().getCacheControl());
    }

    @Test
    public void fileNameWithSlashIsAVirtualDirectory() throws Exception {
        // OUTPUT_FILENAME is "v2/hsl" in the AKS manifests
        sink(5).put(container, "v2/hsl", new byte[]{7});

        assertArrayEquals(new byte[]{7},
                verifier.getBlobContainerClient(container).getBlobClient("v2/hsl").downloadContent().toBytes());
    }

    @Test
    public void lastPublishTimeIsConstructionTimeUntilFirstPut() throws Exception {
        long before = System.nanoTime();
        AzureSink sink = sink(5);
        assertTrue(sink.getLastPublishTime() >= before && sink.getLastPublishTime() <= System.nanoTime());
    }

    @Test
    public void failedUploadPropagatesAndDoesNotUpdateLastPublishTime() throws Exception {
        AzureSink sink = sink(5);
        long constructed = sink.getLastPublishTime();
        try {
            // Invalid container name (upper case and too short)
            sink.put("X", "f.pb", new byte[]{1});
            fail("expected an exception");
        } catch (Exception expected) {
            assertEquals(constructed, sink.getLastPublishTime());
        }
    }
}
