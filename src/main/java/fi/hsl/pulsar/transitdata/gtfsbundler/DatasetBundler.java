package fi.hsl.pulsar.transitdata.gtfsbundler;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;

import java.util.Queue;

public class DatasetBundler {

    public DatasetBundler(Config config) {

    }

    public void initialize() throws Exception {
        //TODO warm up the cache by reading the latest dump?
    }

    public void bundle(Queue<GtfsRealtime.TripUpdate> newMessages) {
        //merge with previous entries. Only keep latest.

        //filter old ones out

        //create GTFS RT Full dataset

        //upload to somewhere.
    }
}
