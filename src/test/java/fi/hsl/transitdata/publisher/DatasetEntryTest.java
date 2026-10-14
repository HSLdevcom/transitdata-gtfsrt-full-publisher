package fi.hsl.transitdata.publisher;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import org.junit.Test;

import java.util.EnumMap;
import java.util.Map;

import static fi.hsl.transitdata.publisher.TestData.*;
import static org.junit.Assert.*;

public class DatasetEntryTest {
    private static final GtfsRealtime.FeedMessage FEED_MESSAGE = feedMessage(alertEntity("a1", "text"));

    @Test
    public void idIsTheMessageKeyOnlyForTripUpdates() throws Exception {
        Map<DatasetPublisher.DataType, String> expectedId = new EnumMap<>(DatasetPublisher.DataType.class);
        expectedId.put(DatasetPublisher.DataType.TripUpdate, "key-1");
        expectedId.put(DatasetPublisher.DataType.ServiceAlert, null);
        expectedId.put(DatasetPublisher.DataType.VehiclePosition, null);
        assertEquals("expected table must cover every DataType", DatasetPublisher.DataType.values().length,
                expectedId.size());

        for (DatasetPublisher.DataType type : DatasetPublisher.DataType.values()) {
            DatasetEntry entry = DatasetEntry.newEntry(pulsarMessage(FEED_MESSAGE.toByteArray(), "key-1", 1234L, null),
                    type);

            assertEquals(type.name(), expectedId.get(type), entry.getId());
            assertEquals(type.name(), 1234L, entry.getEventTimeUtcMs());
            assertEquals(type.name(), FEED_MESSAGE, entry.getFeedMessage());
            assertEquals(type.name(), FEED_MESSAGE.getEntityList(), entry.getEntities());
        }
    }

    @Test
    public void tripUpdateWithoutKeyHasNullId() throws Exception {
        DatasetEntry entry = DatasetEntry.newEntry(pulsarMessage(FEED_MESSAGE.toByteArray(), null, 1L, null),
                DatasetPublisher.DataType.TripUpdate);
        assertNull(entry.getId());
    }

    @Test
    public void messageWithoutEventTimeHasZeroEventTime() throws Exception {
        DatasetEntry entry = DatasetEntry.newEntry(pulsarMessage(FEED_MESSAGE.toByteArray(), null, 0L, null),
                DatasetPublisher.DataType.ServiceAlert);
        assertEquals(0L, entry.getEventTimeUtcMs());
    }

    @Test
    public void emptyPayloadThrowsBecauseHeaderIsRequired() throws Exception {
        try {
            DatasetEntry.newEntry(pulsarMessage(new byte[0], "k", 1L, null), DatasetPublisher.DataType.TripUpdate);
            fail("expected InvalidProtocolBufferException");
        } catch (InvalidProtocolBufferException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("header"));
        }
    }

    @Test
    public void feedMessageWithOnlyHeaderHasNoEntities() throws Exception {
        GtfsRealtime.FeedMessage headerOnly = feedMessage();
        DatasetEntry entry = DatasetEntry.newEntry(pulsarMessage(headerOnly.toByteArray(), "k", 1L, null),
                DatasetPublisher.DataType.TripUpdate);
        assertEquals(headerOnly, entry.getFeedMessage());
        assertTrue(entry.getEntities().isEmpty());
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void malformedPayloadThrows() throws Exception {
        DatasetEntry.newEntry(pulsarMessage(new byte[]{(byte) 0xff, 0x01, 0x02}, "k", 1L, null),
                DatasetPublisher.DataType.TripUpdate);
    }

    @Test
    public void nullDataTypeThrowsIllegalArgument() {
        try {
            DatasetEntry.newEntry(pulsarMessage(FEED_MESSAGE.toByteArray(), "k", 1L, null), null);
            fail("expected IllegalArgumentException");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("Invalid data type to expectnull", e.getMessage());
        }
    }
}
