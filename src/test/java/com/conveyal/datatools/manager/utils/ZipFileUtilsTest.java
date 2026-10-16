package com.conveyal.datatools.manager.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ZipFileUtilsTest {

    @TempDir
    Path tempDir;
    private int zipCounter;

    @Test
    void testEntryOrder() throws IOException {
        Map<String, byte[]> firstEntries = new LinkedHashMap<>();
        firstEntries.put("agency.txt", bytes("agency"));
        firstEntries.put("stops.txt", bytes("stops"));

        Map<String, byte[]> secondEntries = new LinkedHashMap<>();
        secondEntries.put("stops.txt", bytes("stops"));
        secondEntries.put("agency.txt", bytes("agency"));

        assertContentChanged(firstEntries, secondEntries, false);
    }

    @Test
    void testAddedFile() throws IOException {
        assertContentChanged(
            entries("stops.txt", "stops", "routes.txt", "routes"),
            entries("stops.txt", "stops"),
            true
        );
    }

    @Test
    void testRemovedFile() throws IOException {
        assertContentChanged(
            entries("stops.txt", "stops"),
            entries("stops.txt", "stops", "routes.txt", "routes"),
            true
        );
    }

    @Test
    void testContentWithSameSize() throws IOException {
        assertContentChanged(
            entries("stops.txt", "stop1"),
            entries("stops.txt", "stop2"),
            true
        );
    }

    @Test
    void testContentWithDifferentSize() throws IOException {
        assertContentChanged(
            entries("stops.txt", "old"),
            entries("stops.txt", "updated"),
            true
        );
    }

    @Test
    void testForEmptyFilesAndExplicitDirectoryEntries() throws IOException {
        Map<String, byte[]> firstEntries = entries("stops.txt", "");
        firstEntries.put("empty/", null);
        Map<String, byte[]> secondEntries = entries("stops.txt", "");

        assertContentChanged(firstEntries, secondEntries, false);
    }

    @Test
    void testFilesAddedToOrRemovedFromEmptyArchives() throws IOException {
        assertContentChanged(entries(), entries(), false);
        assertContentChanged(entries(), entries("stops.txt", "stops"), true);
        assertContentChanged(entries("stops.txt", "stops"), entries(), true);
    }

    @Test
    void testChangesBeyondFirstReadBuffer() throws IOException {
        String record = "stop_id,stop_name\nstop-1,Main Street\n";
        String content = record.repeat(ZipFileUtils.BUFFER_SIZE / record.length() + 2);
        String changedContent = content.substring(0, content.length() - 1) + " ";

        assertContentChanged(
            entries("stops.txt", content),
            entries("stops.txt", content),
            false
        );
        assertContentChanged(
            entries("stops.txt", content),
            entries("stops.txt", changedContent),
            true
        );
    }

    private void assertContentChanged(
        Map<String, byte[]> newEntries,
        Map<String, byte[]> latestEntries,
        boolean expectedContentChanged
    ) throws IOException {
        Path newZipPath = createZip("new", newEntries);
        Path latestZipPath = createZip("latest", latestEntries);
        assertEquals(
            expectedContentChanged,
            ZipFileUtils.hasContentChanged(newZipPath.toFile(), latestZipPath.toFile())
        );
    }

    private Path createZip(String name, Map<String, byte[]> entries) throws IOException {
        Path zipPath = tempDir.resolve(name + "-" + zipCounter++ + ".zip");
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(zipPath))) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                zip.putNextEntry(new ZipEntry(entry.getKey()));
                if (entry.getValue() != null) {
                    zip.write(entry.getValue());
                }
                zip.closeEntry();
            }
        }
        return zipPath;
    }

    private static Map<String, byte[]> entries(String... nameAndContent) {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        for (int i = 0; i < nameAndContent.length; i += 2) {
            entries.put(nameAndContent[i], bytes(nameAndContent[i + 1]));
        }
        return entries;
    }

    private static byte[] bytes(String content) {
        return content.getBytes(StandardCharsets.UTF_8);
    }

}
