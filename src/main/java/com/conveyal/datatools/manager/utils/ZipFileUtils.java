package com.conveyal.datatools.manager.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ZipFileUtils {

    public static final int BUFFER_SIZE = 64 * 1024;

    private static final Logger LOG = LoggerFactory.getLogger(ZipFileUtils.class);

    /**
     * Check if the content of two zip files has changed. This method compares the entries in the zip files and their content.
     */
    public static boolean hasContentChanged(File oldFile, File newFile) {

        try (
            ZipFile oldZipFile = new ZipFile(oldFile);
            ZipFile newZipFile = new ZipFile(newFile)
        ) {

            // Check for files that have been added or changed
            Enumeration<? extends ZipEntry> newEntries = oldZipFile.entries();

            while (newEntries.hasMoreElements()) {
                ZipEntry newEntry = newEntries.nextElement();

                if (newEntry.isDirectory()) {
                    continue;
                }

                ZipEntry latestEntry = newZipFile.getEntry(newEntry.getName());

                if (latestEntry == null) {
                    LOG.debug("New file {} has been added", newEntry.getName());
                    return true;
                }

                if (newEntry.getSize() != latestEntry.getSize()) {
                    LOG.debug("Uncompressed size of file {} has changed", newEntry.getName());
                    return true;
                }

                // Same filename and size, compare actual content.
                if (!contentEquals(oldZipFile, newEntry, newZipFile, latestEntry)) {
                    LOG.debug("File {} content has changed", newEntry.getName());
                    return true;
                }
            }

            // Check for files that have been removed
            Enumeration<? extends ZipEntry> latestEntries = newZipFile.entries();

            while (latestEntries.hasMoreElements()) {
                ZipEntry latestEntry = latestEntries.nextElement();

                if (latestEntry.isDirectory()) {
                    continue;
                }

                if (oldZipFile.getEntry(latestEntry.getName()) == null) {
                    LOG.debug("File {} has been removed", latestEntry.getName());
                    return true;
                }
            }

        } catch (IOException e) {
            LOG.warn(
                "Unable to compare feed versions {} and {}, assuming not the same",
                oldFile.getName(),
                newFile.getName(),
                e
            );
            return false;
        }

        LOG.debug("ZIP contents are logically identical for {} and {}", oldFile.getName(), newFile.getName());
        return false;
    }

    /**
     * Compare the content of two zip entries from two different zip files.
     */
    private static boolean contentEquals(
        ZipFile firstZip,
        ZipEntry firstEntry,
        ZipFile secondZip,
        ZipEntry secondEntry
    ) throws IOException {

        try (
            InputStream first = firstZip.getInputStream(firstEntry);
            InputStream second = secondZip.getInputStream(secondEntry)
        ) {

            byte[] firstBuffer = new byte[BUFFER_SIZE];
            byte[] secondBuffer = new byte[BUFFER_SIZE];

            while (true) {
                int firstRead = first.readNBytes(firstBuffer, 0, BUFFER_SIZE);
                int secondRead = second.readNBytes(secondBuffer, 0, BUFFER_SIZE);

                if (firstRead != secondRead) {
                    return false;
                }

                if (firstRead == 0) {
                    return true;
                }

                if (!Arrays.equals(
                    firstBuffer, 0, firstRead,
                    secondBuffer, 0, secondRead)) {
                    return false;
                }
            }
        }
    }
}