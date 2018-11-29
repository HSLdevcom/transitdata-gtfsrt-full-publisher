package fi.hsl.pulsar.transitdata.gtfsbundler;

import static com.google.transit.realtime.GtfsRealtime.*;
import com.typesafe.config.Config;

import java.util.*;

public class DatasetBundler {

    final HashMap<Long, DatasetEntry> feedEntityCache = new HashMap<>();

    public DatasetBundler(Config config) {

    }

    public void initialize() throws Exception {
        //TODO warm up the cache by reading the latest dump?
    }

    public void bundle(List<DatasetEntry> newMessages) throws Exception {


        //filter old ones out

        //create GTFS RT Full dataset

        //upload to somewhere.
    }

    static void mergeEventsToCache(List<DatasetEntry> newMessages, Map<Long, DatasetEntry> cache) {
        //Messages should already come sorted by event time but let's make sure, it doesn't cost much
        Collections.sort(newMessages, Comparator.comparingLong(DatasetEntry::getEventTimeUtcMs));
        //merge with previous entries. Only keep latest.
        newMessages.forEach(entry -> cache.put(entry.getDvjId(), entry));
    }
}
