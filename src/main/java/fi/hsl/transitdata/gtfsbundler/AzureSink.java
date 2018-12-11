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
    //do this first: https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=portal

    //TODO https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-java
    // https://github.com/Azure-Samples/storage-blobs-java-quickstart/blob/master/blobAzureApp/src/main/java/blobQuickstart/blobAzureApp/AzureApp.java

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

        final String storageConnectionString = (new StringBuilder())
                .append("DefaultEndpointsProtocol=https;")
                .append("AccountName=").append(accountName).append(";")
                .append("AccountKey=").append(accountKey).append(";")
                .toString();

        try {
            // Parse the connection string and create a blob client to interact with Blob storage
            final CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
            final CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            final CloudBlobContainer container = blobClient.getContainerReference(containerName);

            // Create the container if it does not exist with public access.
            log.info("Creating container: " + container.getName());
            container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());
            //TODO create snapshot? Or upload as compressed unique filename along with other.
            //Creating a sample file
            /*sourceFile = File.createTempFile("sampleFile", ".txt");
            System.out.println("Creating a sample file at: " + sourceFile.toString());
            BufferedWriter output = new BufferedWriter(new FileWriter(sourceFile));
            output.write("Hello Azure!");
            output.close();*/

            //Getting a blob reference
            CloudBlockBlob blob = container.getBlockBlobReference(name);
            final InputStream inputStream = new ByteArrayInputStream(data);
            final int length = data.length;

            //Creating blob and uploading file to it
            System.out.println("Uploading the sample file ");
            blob.upload(inputStream, length);
            //blob.uploadFromFile(sourceFile.getAbsolutePath());

            //Listing contents of container
            for (ListBlobItem blobItem : container.listBlobs()) {
                System.out.println("URI of blob is: " + blobItem.getUri());
            }

            // Download blob. In most cases, you would have to retrieve the reference
            // to cloudBlockBlob here. However, we created that reference earlier, and
            // haven't changed the blob we're interested in, so we can reuse it.
            // Here we are creating a new file to download to. Alternatively you can also pass in the path as a string into downloadToFile method: blob.downloadToFile("/path/to/new/file").
            //downloadedFile = new File(sourceFile.getParentFile(), "downloadedFile.txt");
            //blob.downloadToFile(downloadedFile.getAbsolutePath());
        }
        catch (StorageException ex)
        {
            log.error("Error returned from the service. Http code: {} and error code: {}", ex.getHttpStatusCode(), ex.getErrorCode());
        }
        catch (Exception ex)
        {
            log.error("Exception while uploading file to storage", ex);
        }
        finally
        {

            /*
            //Pausing for input
            Scanner sc = new Scanner(System.in);
            sc.nextLine();

            System.out.println("Deleting the container");
            try {
                if(container != null)
                    container.deleteIfExists();
            }
            catch (StorageException ex) {
                System.out.println(String.format("Service error. Http code: %d and error code: %s", ex.getHttpStatusCode(), ex.getErrorCode()));
            }

            System.out.println("Deleting the source, and downloaded files");

            if(downloadedFile != null)
                downloadedFile.deleteOnExit();

            if(sourceFile != null)
                sourceFile.deleteOnExit();

            //Closing scanner
            sc.close();*/
        }
    }

}
