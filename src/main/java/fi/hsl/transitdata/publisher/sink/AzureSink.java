package fi.hsl.transitdata.publisher.sink;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AzureSink implements ISink {
    private static final Logger log = LoggerFactory.getLogger(AzureSink.class);

    private final String accountName;
    private final String accountKey;
    private final long cacheMaxAgeSeconds;

    private AtomicLong lastPublishTime = new AtomicLong(System.nanoTime());

    private AzureSink(String accountName, String accountKey, long cacheMaxAgeSecs) {
        this.accountName = accountName;
        this.accountKey = accountKey;
        this.cacheMaxAgeSeconds = cacheMaxAgeSecs;
    }

    public static AzureSink newInstance(Config config) throws Exception {
        String name = config.getString("bundler.output.azure.accountName");
        long maxAge = config.getDuration("bundler.output.azure.cacheMaxAge", TimeUnit.SECONDS);

        //We'll use Docker secrets for getting the key
        String keyPath = config.getString("bundler.output.azure.accountKeyPath");
        String key = new String(Files.readAllBytes(Paths.get(keyPath)), StandardCharsets.UTF_8);

        return new AzureSink(name, key, maxAge);
    }

    @Override
    public void put(String containerName, String fileName, byte[] data) throws Exception {
        upload(containerName, fileName, data);
        lastPublishTime.set(System.nanoTime());
    }

    @Override
    public long getLastPublishTime() {
        return lastPublishTime.get();
    }

    private void upload(String containerName, String name, byte[] data) throws Exception {
        log.info("Uploading file {} with {} kB to Azure Blob storage", name, (data.length / 1024));
        final long startTime = System.nanoTime();

        final String storageConnectionString = "DefaultEndpointsProtocol=https;" +
                "AccountName=" + accountName + ";" +
                "AccountKey=" + accountKey + ";" +
                "EndpointSuffix=core.windows.net";

        try {
            final BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(storageConnectionString).buildClient();

            BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
            if (!blobContainerClient.exists()) {
                blobContainerClient = blobServiceClient.createBlobContainer(containerName);
            }

            final BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(name).getBlockBlobClient();
            writeToBlob(blockBlobClient, data);

            final BlobHttpHeaders blobHttpHeaders = new BlobHttpHeaders()
                    .setCacheControl("max-age=" + cacheMaxAgeSeconds)
                    .setContentType("application/x-protobuf");

            blockBlobClient.setHttpHeaders(blobHttpHeaders);
        } catch (Exception ex) {
            log.error("Unknown exception while uploading file to storage", ex);
            throw ex;
        } finally {
            long now = System.nanoTime();
            log.info("Upload finished in {} ms", Duration.ofNanos(now - startTime).getSeconds());
        }
    }

    private static void writeToBlob(BlockBlobClient blockBlobClient, byte[] data) throws IOException {
        try (OutputStream os = blockBlobClient.getBlobOutputStream(true)){
            os.write(data);
        }
    }
}
