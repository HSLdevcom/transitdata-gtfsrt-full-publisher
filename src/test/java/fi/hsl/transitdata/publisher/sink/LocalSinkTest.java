package fi.hsl.transitdata.publisher.sink;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static fi.hsl.transitdata.publisher.TestData.config;
import static org.junit.Assert.*;

public class LocalSinkTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void writesFileIntoContainerDirectoryCreatingIt() throws Exception {
        LocalSink sink = new LocalSink(config("bundler.output.local.path", tmp.getRoot().getPath()));

        sink.put("container/nested", "file.pb", new byte[]{1, 2, 3});

        Path file = tmp.getRoot().toPath().resolve("container/nested/file.pb");
        assertArrayEquals(new byte[]{1, 2, 3}, Files.readAllBytes(file));
    }

    @Test
    public void pathWithAndWithoutTrailingSlashAreEquivalent() {
        String root = tmp.getRoot().getPath();
        assertEquals(root + "/", new LocalSink(config("bundler.output.local.path", root)).path);
        assertEquals(root + "/", new LocalSink(config("bundler.output.local.path", root + "/")).path);
    }

    @Test
    public void putOverwritesExistingFile() throws Exception {
        LocalSink sink = new LocalSink(config("bundler.output.local.path", tmp.getRoot().getPath()));

        sink.put("c", "f.pb", new byte[]{1, 2, 3, 4});
        sink.put("c", "f.pb", new byte[]{9});

        assertArrayEquals(new byte[]{9}, Files.readAllBytes(tmp.getRoot().toPath().resolve("c/f.pb")));
    }

    @Test
    public void fileNameWithDirectoryFailsBecauseOnlyTheContainerDirectoryIsCreated() throws Exception {
        // OUTPUT_FILENAME is "v2/hsl" in the AKS manifests; that only works with the Azure sink
        LocalSink sink = new LocalSink(config("bundler.output.local.path", tmp.getRoot().getPath()));
        try {
            sink.put("c", "v2/hsl", new byte[]{1});
            fail("expected NoSuchFileException");
        } catch (NoSuchFileException expected) {
            // pinned current behavior
        }
    }

    @Test
    public void lastPublishTimeIsConstructionTimeUntilFirstPut() throws Exception {
        long beforeConstruction = System.nanoTime();
        LocalSink sink = new LocalSink(config("bundler.output.local.path", tmp.getRoot().getPath()));
        long constructed = sink.getLastPublishTime();
        assertTrue(constructed >= beforeConstruction && constructed <= System.nanoTime());

        long beforePut = System.nanoTime();
        sink.put("c", "f.pb", new byte[]{1});
        assertTrue(sink.getLastPublishTime() >= beforePut);
    }

    @Test
    public void failedPutDoesNotUpdateLastPublishTime() throws Exception {
        Files.write(tmp.getRoot().toPath().resolve("not-a-dir"), new byte[]{0});
        LocalSink sink = new LocalSink(config("bundler.output.local.path", tmp.getRoot().getPath()));
        long constructed = sink.getLastPublishTime();

        try {
            sink.put("not-a-dir", "f.pb", new byte[]{1});
            fail("expected an exception");
        } catch (Exception expected) {
            assertEquals(constructed, sink.getLastPublishTime());
        }
    }
}
