package fi.hsl.transitdata.publisher.publisher;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.transitdata.publisher.DatasetEntry;
import fi.hsl.transitdata.publisher.DatasetPublisher;
import fi.hsl.transitdata.publisher.sink.ISink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class ServiceAlertPublisher extends DatasetPublisher {
    private static final Logger log = LoggerFactory.getLogger(DatasetPublisher.class);

    public ServiceAlertPublisher(Config config, ISink sink) {
        super(config, sink);
    }

    @Override
    public void publish(List<DatasetEntry> newMessages) throws Exception {
        //We're only interested in the first item in the list.
        Optional<DatasetEntry> latest = newMessages.stream()
                .sorted(Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs).reversed()) // Sort by event time, latest first
                .findFirst();
        if (latest.isPresent()) {
            GtfsRealtime.FeedMessage msg = latest.get().getFeedMessage();

            log.info("Publishing a new Service Alert");
            byte[] data = msg.toByteArray();
            sink.put(fileName, data);
        }
    }


}
