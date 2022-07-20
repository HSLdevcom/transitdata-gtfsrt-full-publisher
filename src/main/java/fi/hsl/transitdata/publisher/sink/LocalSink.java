package fi.hsl.transitdata.publisher.sink;

import com.typesafe.config.Config;

import java.nio.file.Files;
import java.nio.file.Path;
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
    public void put(String containerName, String fileName, byte[] data) throws Exception {
        Path container = Paths.get(path + containerName);
        Files.createDirectories(container);

        Path file = container.resolve(fileName);
        Files.write(file, data);
        lastPublishTime.set(System.nanoTime());
    }

    @Override
    public long getLastPublishTime() {
        return lastPublishTime.get();
    }
}
