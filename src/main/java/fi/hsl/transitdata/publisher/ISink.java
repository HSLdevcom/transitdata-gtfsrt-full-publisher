package fi.hsl.transitdata.publisher;

public interface ISink {
    void put(String name, byte[] data) throws Exception;
}
