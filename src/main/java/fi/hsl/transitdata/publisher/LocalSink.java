package fi.hsl.transitdata.publisher;

import com.typesafe.config.Config;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

public class LocalSink implements ISink {
    final String path;
    private final AtomicLong lastPublishTime = new AtomicLong(System.nanoTime());

    public LocalSink(Config config) {
        String rawPath = config.getString("bundler.output.local.path");
        path = rawPath.endsWith("/") ? rawPath : rawPath + "/";
    }

    @Override
    public void put(String name, byte[] data) throws Exception {
        String fullPath = path + name;
        Files.write(Paths.get(fullPath), data);
        lastPublishTime.set(System.nanoTime());
    }

    @Override
    public long getLastPublishTime() {
        return lastPublishTime.get();
    }
}
