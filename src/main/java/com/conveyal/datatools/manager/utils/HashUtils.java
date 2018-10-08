package com.conveyal.datatools.manager.utils;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.MessageDigest;


public class HashUtils {

    public static String hashString(String input)  {

        try {

            byte[] bytesOfMessage = input.getBytes("UTF-8");

            return DigestUtils.md5Hex(bytesOfMessage);

        }
        catch(Exception e) {

            return "";
        }
    }

    /**
     * Get MD5 hash for the specified file.
     */
    public static String hashFile(File file)  {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            FileInputStream fis = new FileInputStream(file);
            DigestInputStream dis = new DigestInputStream(fis, md);
            // hash the size
            dis.read(ByteBuffer.allocate(8).putLong(file.length()).array());
            // hash first 1000 bytes
            int i = 0;
            while (dis.read() != -1 && i < 1000) {
                i++;
            }
            // hash  5000 bytes starting in the middle or the remainder of the file if under 10000
            if(file.length() > 10000) {
                dis.skip(file.length() / 2);
                i = 0;
                while (dis.read() != -1 && i < 5000) {
                    i++;
                }
            }
            else {
                while (dis.read() != -1) { }
            }
            dis.close();
            return new String(Hex.encodeHex(md.digest()));
        }
        catch(Exception e) {
            return "";
        }
    }

}
