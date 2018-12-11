package fi.hsl.transitdata.gtfsbundler;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class AzureSink implements ISink {
    private static final Logger log = LoggerFactory.getLogger(AzureSink.class);

    private final String accountName;
    private final String accountKey;
    private final String containerName;

    public AzureSink(Config config) {
        //TODO read from docker secrets
        accountName = config.getString("bundler.output.azure.accountName");
        accountKey = config.getString("bundler.output.azure.accountKey");
        containerName = config.getString("bundler.output.azure.containerName");
    }

    @Override
    public void put(String name, byte[] data) throws Exception {
        upload(name, data, containerName, accountName, accountKey);
    }

    private static void upload(String name, byte[] data, String containerName, String accountName, String accountKey) {
        log.info("Uploading file {} with {} kB to Azure Blob storage", name, (data.length / 1024));
        final long startTime = System.currentTimeMillis();
        final String storageConnectionString = (new StringBuilder())
                .append("DefaultEndpointsProtocol=https;")
                .append("AccountName=").append(accountName).append(";")
                .append("AccountKey=").append(accountKey).append(";")
                .toString();

        try {
            // Parse the connection string and create a blob client to interact with Blob storage
            final CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
            log.debug("Got reference to StorageAccount");
            final CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            log.debug("Got reference to BlobClient");
            final CloudBlobContainer container = blobClient.getContainerReference(containerName);
            log.debug("Got reference to CloudBlobContainer");

            // Create the container if it does not exist with public access.
            // TODO we might want to keep the access private and distribute the file via CDN.
            boolean created = container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());
            if (created) {
                log.warn("New container named {} created because existing wasn't found", container.getName());
            }

            CloudBlockBlob blob = container.getBlockBlobReference(name);
            log.debug("Got reference to CloudBlockBlob with name {}", blob.getName());
            blob.getProperties().setContentType("application/x-protobuf");

            final InputStream inputStream = new ByteArrayInputStream(data);
            final int length = data.length;

            //Creating blob and uploading file to it
            log.debug("Uploading the file ");
            blob.upload(inputStream, length);

            //Listing contents of container
            log.info("Container contains files: ");
            for (ListBlobItem blobItem : container.listBlobs()) {
                log.info(blobItem.getUri().toString());
            }

        }
        catch (StorageException ex) {
            log.error("Error returned from the service. Http code: {} and error code: {}", ex.getHttpStatusCode(), ex.getErrorCode());
            log.warn("Full stack trace", ex);
        }
        catch (Exception ex) {
            log.error("Unknown exception while uploading file to storage", ex);
        }
        finally {
            long now = System.currentTimeMillis();
            log.info("Upload finished in {} ms", (now - startTime));
        }
    }

}
