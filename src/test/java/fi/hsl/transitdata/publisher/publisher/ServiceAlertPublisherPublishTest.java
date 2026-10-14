package fi.hsl.transitdata.publisher.publisher;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.transitdata.publisher.DatasetEntry;
import fi.hsl.transitdata.publisher.DatasetPublisher;
import fi.hsl.transitdata.publisher.TestData;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static fi.hsl.transitdata.publisher.TestData.*;
import static org.junit.Assert.*;

public class ServiceAlertPublisherPublishTest {
    private TestData.RecordingSink sink;
    private ServiceAlertPublisher publisher;

    @Before
    public void setUp() {
        sink = new TestData.RecordingSink();
        publisher = new ServiceAlertPublisher(
                config("bundler.serviceAlert.containerName", "alerts", "bundler.output.fileName", "file.pb"), sink);
    }

    private static DatasetEntry alert(String id, long eventTimeMs) throws Exception {
        return entry(feedMessage(alertEntity(id, "text " + id)), null, eventTimeMs,
                DatasetPublisher.DataType.ServiceAlert);
    }

    @Test
    public void nothingIsPublishedWithoutNewMessages() throws Exception {
        publisher.publish(new ArrayList<>());
        assertTrue(sink.puts.isEmpty());
    }

    @Test
    public void latestFeedMessageByEventTimeIsPublishedAsIs() throws Exception {
        DatasetEntry older = alert("old", 1000);
        DatasetEntry latest = alert("latest", 3000);
        DatasetEntry middle = alert("middle", 2000);

        publisher.publish(new ArrayList<>(Arrays.asList(older, latest, middle)));

        assertEquals(1, sink.puts.size());
        TestData.RecordingSink.Put put = sink.puts.get(0);
        assertEquals("alerts", put.containerName);
        assertEquals("file.pb", put.fileName);
        // The input FeedMessage is not re-wrapped: header (DIFFERENTIAL, timestamp 1) is kept
        assertArrayEquals(latest.getFeedMessage().toByteArray(), put.data);
        assertEquals(GtfsRealtime.FeedHeader.Incrementality.DIFFERENTIAL,
                put.feedMessage().getHeader().getIncrementality());
    }

    @Test
    public void onEqualEventTimeTheFirstMessageWins() throws Exception {
        DatasetEntry first = alert("first", 1000);
        DatasetEntry second = alert("second", 1000);

        publisher.publish(new ArrayList<>(Arrays.asList(first, second)));

        assertEquals("first", sink.puts.get(0).feedMessage().getEntity(0).getId());
    }

    @Test
    public void stateIsNotCachedBetweenPublishes() throws Exception {
        publisher.publish(new ArrayList<>(Arrays.asList(alert("a", 1000))));
        publisher.publish(new ArrayList<>());
        assertEquals(1, sink.puts.size());
    }

    @Test
    public void emptyFeedMessageIsPublished() throws Exception {
        publisher.publish(new ArrayList<>(
                Arrays.asList(entry(feedMessage(), null, 1000, DatasetPublisher.DataType.ServiceAlert))));
        assertEquals(1, sink.puts.size());
        assertEquals(0, sink.puts.get(0).feedMessage().getEntityCount());
    }
}
