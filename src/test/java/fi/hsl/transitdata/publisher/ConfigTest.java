package fi.hsl.transitdata.publisher;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

/**
 * Defaults of environment.conf (on top of transitdata-common's common.conf) when no env vars are set.
 */
public class ConfigTest {
    private final Config config = ConfigParser.createConfig();

    @Test
    public void pulsarDefaults() {
        assertEquals("localhost", config.getString("pulsar.host"));
        assertEquals(6650, config.getInt("pulsar.port"));
        assertTrue(config.getBoolean("pulsar.consumer.enabled"));
        assertEquals("trip-update", config.getString("pulsar.consumer.topic"));
        assertEquals("pulsar-gtfs-bundler", config.getString("pulsar.consumer.subscription"));
        assertEquals("Shared", config.getString("pulsar.consumer.subscriptionType"));
        assertFalse(config.getBoolean("pulsar.consumer.cursor.resetToLatest"));
        assertFalse(config.getBoolean("pulsar.producer.enabled"));
    }

    @Test
    public void healthDefaults() {
        assertFalse(config.getBoolean("health.enabled"));
        assertEquals(8090, config.getInt("health.port"));
        assertEquals("/health", config.getString("health.endpoint"));
        assertEquals(Duration.ofMinutes(5), config.getDuration("health.unhealthyTimeout"));
    }

    @Test
    public void bundlerDefaults() {
        assertEquals(Duration.ofSeconds(5), config.getDuration("bundler.dumpInterval"));
        assertEquals("TripUpdate", config.getString("bundler.dataType"));
    }

    @Test
    public void tripUpdateDefaults() {
        assertEquals(Duration.ofMinutes(5), config.getDuration("bundler.tripUpdate.contentMaxAge"));
        assertEquals(Duration.ofHours(2), config.getDuration("bundler.tripUpdate.maxAgeAfterStart"));
        assertEquals("Europe/Helsinki", config.getString("bundler.tripUpdate.timezone"));
        assertEquals("trip-updates", config.getString("bundler.tripUpdate.fullContainerName"));
        assertEquals("trip-updates-for-google", config.getString("bundler.tripUpdate.googleContainerName"));
    }

    @Test
    public void vehiclePositionDefaults() {
        assertEquals(Duration.ofMinutes(2), config.getDuration("bundler.vehiclePosition.contentMaxAge"));
        assertEquals("vehicle-positions", config.getString("bundler.vehiclePosition.fullContainerName"));
        assertEquals("vehicle-positions-bustram", config.getString("bundler.vehiclePosition.busTramContainerName"));
        assertEquals("vehicle-positions-trainmetro",
                config.getString("bundler.vehiclePosition.trainMetroContainerName"));
    }

    @Test
    public void serviceAlertDefaults() {
        assertEquals("service-alerts", config.getString("bundler.serviceAlert.containerName"));
    }

    @Test
    public void outputDefaults() {
        assertEquals("HSL_TripUpdates_Full.pb", config.getString("bundler.output.fileName"));
        assertEquals("local", config.getString("bundler.output.destination"));
        assertEquals(".", config.getString("bundler.output.local.path"));
        assertEquals("name", config.getString("bundler.output.azure.accountName"));
        assertFalse(config.hasPath("bundler.output.azure.accountKeyPath"));
        assertEquals(Duration.ofSeconds(5), config.getDuration("bundler.output.azure.cacheMaxAge"));
    }
}
