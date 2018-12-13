package fi.hsl.transitdata.gtfsbundler;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class DatasetBundler {
    private static final Logger log = LoggerFactory.getLogger(DatasetBundler.class);

    protected final String fileName;
    protected final ISink sink;

    public enum Destination {
        local, azure
    }

    public enum DataType {
        TripUpdate, ServiceAlert
    }

    protected DatasetBundler(Config config, ISink sink) {
        fileName = config.getString("bundler.output.fileName");
        this.sink = sink;
    }

    public static DatasetBundler newInstance(Config config) throws Exception {
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
            return new ServiceAlertBundler(config, sink);
        } else if (type == DataType.TripUpdate) {
            return new TripUpdateBundler(config, sink);
        } else {
            throw new IllegalArgumentException("Invalid DataType, should be TripUpdate or ServiceAlert");
        }
    }

    public abstract void bundle(List<DatasetEntry> newMessages) throws Exception;

}
