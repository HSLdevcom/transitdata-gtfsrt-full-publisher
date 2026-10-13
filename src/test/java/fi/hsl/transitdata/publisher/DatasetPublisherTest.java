package fi.hsl.transitdata.publisher;

import fi.hsl.transitdata.publisher.publisher.ServiceAlertPublisher;
import fi.hsl.transitdata.publisher.publisher.TripUpdatePublisher;
import fi.hsl.transitdata.publisher.publisher.VehiclePositionPublisher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.EnumMap;
import java.util.Map;

import static fi.hsl.transitdata.publisher.TestData.config;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class DatasetPublisherTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void createsPublisherMatchingDataType() throws Exception {
        Map<DatasetPublisher.DataType, Class<? extends DatasetPublisher>> expected = new EnumMap<>(
                DatasetPublisher.DataType.class);
        expected.put(DatasetPublisher.DataType.TripUpdate, TripUpdatePublisher.class);
        expected.put(DatasetPublisher.DataType.ServiceAlert, ServiceAlertPublisher.class);
        expected.put(DatasetPublisher.DataType.VehiclePosition, VehiclePositionPublisher.class);
        assertEquals("expected table must cover every DataType", DatasetPublisher.DataType.values().length,
                expected.size());

        for (DatasetPublisher.DataType type : DatasetPublisher.DataType.values()) {
            DatasetPublisher publisher = DatasetPublisher.newInstance(config("bundler.dataType", type.name(),
                    "bundler.output.destination", "local", "bundler.output.local.path", tmp.getRoot().getPath()));
            assertEquals(type.name(), expected.get(type), publisher.getClass());
        }
    }

    @Test
    public void destinationsAreLocalAndAzure() {
        assertArrayEquals(new DatasetPublisher.Destination[]{DatasetPublisher.Destination.local,
                DatasetPublisher.Destination.azure}, DatasetPublisher.Destination.values());
    }

    @Test(expected = IllegalArgumentException.class)
    public void unknownDestinationThrows() throws Exception {
        DatasetPublisher.newInstance(config("bundler.output.destination", "s3"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void destinationIsCaseSensitive() throws Exception {
        DatasetPublisher.newInstance(config("bundler.output.destination", "LOCAL"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void unknownDataTypeThrows() throws Exception {
        DatasetPublisher.newInstance(config("bundler.output.destination", "local", "bundler.output.local.path",
                tmp.getRoot().getPath(), "bundler.dataType", "Cancellation"));
    }

    @Test
    public void azureDestinationWithoutStorageKeyEnvThrows() throws Exception {
        String key = System.getenv("TRANSITDATA_AZURE_STORAGE_KEY");
        assumeTrue(key == null || key.isEmpty());
        try {
            DatasetPublisher.newInstance(config("bundler.output.destination", "azure"));
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Azure storage key not found.", e.getMessage());
        }
    }

    @Test
    public void lastPublishTimeComesFromTheSink() {
        TestData.RecordingSink sink = new TestData.RecordingSink();
        DatasetPublisher publisher = new ServiceAlertPublisher(config(), sink);
        assertEquals(42L, publisher.getLastPublishTime());
    }
}
