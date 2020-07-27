package com.conveyal.datatools.common.utils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;

import java.util.HashMap;

public abstract class AWSClientManager<T> {
    private final T defaultClient;
    private HashMap<String, T> nonRoleClientsByRegion = new HashMap<>();

    public AWSClientManager (T defaultClient) {
        this.defaultClient = defaultClient;
    }

    public abstract T buildDefaultClientWithRegion();

    public T getClient (String role, String region) throws AWSUtils.NonRuntimeAWSException {
        // return default client for null region and role
        if (role == null && region == null) {
            return defaultClient;
        }

        // if the role is null, return a potentially cached EC2 client with the region configured
        T client;
        if (role == null) {
            client = nonRoleClientsByRegion.get(region);
            if (client == null) {
                client = buildDefaultClientWithRegion();
                nonRoleClientsByRegion.put(region, EC2Client);
            }
            return EC2Client;
        }

        // check for the availability of a EC2 client already associated with the given role and region
        String roleRegionKey = makeRoleRegionKey(role, region);
        AWSUtils.AmazonEC2ClientWithRole EC2ClientWithRole = EC2ClientsByRoleAndRegion.get(roleRegionKey);
        if (EC2ClientWithRole != null && EC2ClientWithRole.isActive()) return EC2ClientWithRole.EC2Client;

        // Either a new client hasn't been created or it has expired. Create a new client and cache it.
        AWSStaticCredentialsProvider credentials = getCredentialsForRole(role);
        AmazonEC2ClientBuilder builder = AmazonEC2Client.builder().withCredentials(credentials);
        if (region != null) {
            builder = builder.withRegion(region);
        }
        EC2Client = builder.build();
        EC2ClientsByRoleAndRegion.put(roleRegionKey, new AWSUtils.AmazonEC2ClientWithRole(EC2Client));
        return EC2Client;
    }

    private static String makeRoleRegionKey(String role, String region) {
        return String.format("role=%s,region=%s", role, region);
    }
}
