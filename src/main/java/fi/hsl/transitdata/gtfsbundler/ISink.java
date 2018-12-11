package fi.hsl.transitdata.gtfsbundler;

public interface ISink {
    void put(String name, byte[] data) throws Exception;
}
