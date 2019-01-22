package com.conveyal.datatools;

public class TestUtils {
    public static boolean getBooleanEnvVar (String var) {
        String CI = System.getenv(var);
        return CI != null && CI.equals("true");
    }

    public static boolean isCi () {
        return getBooleanEnvVar("CI");
    }
}
