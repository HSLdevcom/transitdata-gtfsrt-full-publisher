package fi.hsl.transitdata.publisher;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Launching Pulsar GTFS Bundler.");

        Config config = ConfigParser.createConfig();

        log.info("Configurations read, launching Pulsar Application");

        try (PulsarApplication app = PulsarApplication.newInstance(config)) {
            MessageProcessor processor = MessageProcessor.newInstance(app);

            log.info("Starting to process messages");

            app.launchWithHandler(processor);

        }
        catch (Exception e) {
            log.error("Exception at main", e);
        }

    }
}
