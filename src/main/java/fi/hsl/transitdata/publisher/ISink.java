package fi.hsl.transitdata.publisher;

public interface ISink {
    void put(String name, byte[] data) throws Exception;

    /**
     * @return Time (in elapsed nanoseconds, see {@link System#nanoTime()}) when the data was last published to the sink
     */
    long getLastPublishTime();
}
