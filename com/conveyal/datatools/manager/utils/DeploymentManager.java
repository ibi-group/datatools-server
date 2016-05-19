package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.DataManager;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by landon on 5/18/16.
 */
public class DeploymentManager {
    private static Map<String, Map<String, Object>> getServers () {
        return (Map<String, Map<String, Object>>) DataManager.config.get("deployment").get("servers");
    }

    /**
     * Get all of the names for the deployments
     * @param admin is the user an admin? if false, only show the deployments that don't require administrative privileges.
     */
    public static Set<String> getDeploymentNames (boolean admin) {
        Map<String, Map<String, Object>> servers = getServers();

        if (admin)
            return servers.keySet();
        else {
            Set<String> ret = new HashSet<String>();
            for (String server : servers.keySet()) {
                if (!isDeploymentAdmin(server)) {
                    ret.add(server);
                }
            }

            return ret;
        }
    };

    /** Get the servers for a particular deployment */
    public static List<String> getDeploymentUrls (String name) {
        return (List<String>) getServers().get(name).get("internal");
    }

    /**
     * If true, only admins should be allowed to deploy to this deployment.
     */
    public static boolean isDeploymentAdmin (String name) {
        Map<String, Object> server = getServers().get(name);
        if (!server.containsKey("admin"))
            return false;

        else return (Boolean) server.get("admin");
    }

    /** Get the public otp.js server for a particular deployment */
    public static String getPublicUrl(String name) {
        return (String) getServers().get(name).get("public");
    }

    /** Get the s3 bucket for a particular deployment */
    public static String getS3Bucket(String name) {
        return (String) getServers().get(name).get("s3bucket");
    }

    /** Get the s3 credentials filename for a particular deployment */
    public static String getS3Credentials(String name) {
        return (String) getServers().get(name).get("s3credentials");
    }
}
