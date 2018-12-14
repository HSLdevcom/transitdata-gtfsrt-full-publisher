package fi.hsl.transitdata.publisher;

import com.typesafe.config.Config;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class DatasetPublisher {
    private static final Logger log = LoggerFactory.getLogger(DatasetPublisher.class);

    protected final String fileName;
    protected final ISink sink;

    private final long bootstrapPeriodInSecs;
    private final String adminHttpUrl = "http://localhost:8080";

    public enum Destination {
        local, azure
    }

    public enum DataType {
        TripUpdate, ServiceAlert
    }

    protected DatasetPublisher(Config config, ISink sink) {
        fileName = config.getString("bundler.output.fileName");
        bootstrapPeriodInSecs = config.getDuration("bundler.bootstrapPeriod", TimeUnit.SECONDS);
        this.sink = sink;
    }

    public static DatasetPublisher newInstance(Config config) throws Exception {
        Destination destination = Destination.valueOf(config.getString("bundler.output.destination"));
        log.info("Using file destination: {}", destination);
        ISink sink = null;
        if (destination == Destination.azure) {
            sink = AzureSink.newInstance(config);
        } else if (destination == Destination.local) {
            sink = new LocalSink(config);
        } else {
            throw new IllegalArgumentException("Invalid Destination, should be local or azure");
        }

        DataType type = DataType.valueOf(config.getString("bundler.dataType"));
        if (type == DataType.ServiceAlert) {
            return new ServiceAlertPublisher(config, sink);
        } else if (type == DataType.TripUpdate) {
            return new TripUpdatePublisher(config, sink);
        } else {
            throw new IllegalArgumentException("Invalid DataType, should be TripUpdate or ServiceAlert");
        }
    }

    public void bootstrap(Consumer consumer) {
        try {
            PulsarAdmin admin = PulsarAdmin.builder()
                    .serviceHttpUrl(adminHttpUrl)
                    .build();
            long rewindTo = Instant.now().minus(bootstrapPeriodInSecs, ChronoUnit.SECONDS).toEpochMilli();
            log.info("Resetting Pulsar topic cursor with {} seconds to {} (epoch ms).", bootstrapPeriodInSecs, rewindTo);
            admin.topics().resetCursor(consumer.getTopic(), consumer.getSubscription(), rewindTo);
        }
        catch (Exception e) {
            log.error("Failed to reset Pulsar cursor!", e);
        }
    }

    public abstract void publish(List<DatasetEntry> newMessages) throws Exception;

}
