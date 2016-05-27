package com.conveyal.datatools.manager.auth;

import com.conveyal.datatools.manager.DataManager;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by demory on 1/18/16.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class Auth0UserProfile {
    String email;
    String user_id;
    AppMetadata app_metadata;

    public Auth0UserProfile() {
    }


    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getEmail() {
        return email;
    }

    public void setApp_metadata(AppMetadata app_metadata) {
        this.app_metadata = app_metadata;
    }

    @JsonIgnore
    public void setDatatoolsInfo(DatatoolsInfo datatoolsInfo) {
        this.app_metadata.getDatatoolsInfo().setClientId(datatoolsInfo.clientId);
        this.app_metadata.getDatatoolsInfo().setPermissions(datatoolsInfo.permissions);
        this.app_metadata.getDatatoolsInfo().setProjects(datatoolsInfo.projects);
        this.app_metadata.getDatatoolsInfo().setSubscriptions(datatoolsInfo.subscriptions);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AppMetadata {
        ObjectMapper mapper = new ObjectMapper();
        @JsonProperty("datatools")
        List<DatatoolsInfo> datatools;

        public AppMetadata() {
        }

        @JsonIgnore
        public void setDatatoolsInfo(DatatoolsInfo datatools) {
            for(int i = 0; i < this.datatools.size(); i++) {
                if (this.datatools.get(i).clientId.equals(DataManager.config.get("auth0").get("clientId").asText())) {
                    this.datatools.set(i, datatools);
                }
            }
        }
        @JsonIgnore
        public DatatoolsInfo getDatatoolsInfo() {
            for(int i = 0; i < this.datatools.size(); i++) {
                DatatoolsInfo dt = this.datatools.get(i);
                if (dt.clientId.equals(DataManager.config.get("auth0").get("client_id").asText())) {
                    return dt;
                }
            }
            return null;
        }
    }
    public static class DatatoolsInfo {
        @JsonProperty("client_id")
        String clientId;
        Project[] projects;
        Permission[] permissions;
        Subscription[] subscriptions;

        public DatatoolsInfo() {
        }

        public DatatoolsInfo(String clientId, Project[] projects, Permission[] permissions, Subscription[] subscriptions) {
            this.clientId = clientId;
            this.projects = projects;
            this.permissions = permissions;
            this.subscriptions = subscriptions;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public void setProjects(Project[] projects) {
            this.projects = projects;
        }

        public void setPermissions(Permission[] permissions) {
            this.permissions = permissions;
        }

        public void setSubscriptions(Subscription[] subscriptions) {
            this.subscriptions = subscriptions;
        }

    }


    public static class Project {

        String project_id;
        Permission[] permissions;
        String[] defaultFeeds;

        public Project() {
        }

        public Project(String project_id, Permission[] permissions, String[] defaultFeeds) {
            this.project_id = project_id;
            this.permissions = permissions;
            this.defaultFeeds = defaultFeeds;
        }

        public void setProject_id(String project_id) {
            this.project_id = project_id;
        }

        public void setPermissions(Permission[] permissions) { this.permissions = permissions; }

        public void setDefaultFeeds(String[] defaultFeeds) {
            this.defaultFeeds = defaultFeeds;
        }

    }

    public static class Permission {

        String type;
        String[] feeds;

        public Permission() {
        }

        public Permission(String type, String[] feeds) {
            this.type = type;
            this.feeds = feeds;
        }

        public void setType(String type) {
            this.type = type;
        }

        public void setFeeds(String[] feeds) {
            this.feeds = feeds;
        }
    }

    public static class Subscription {

        String type;
        String[] target;

        public Subscription() {
        }

        public Subscription(String type, String[] target) {
            this.type = type;
            this.target = target;
        }

        public void setType(String type) {
            this.type = type;
        }

        public void setTarget(String[] target) {
            this.target = target;
        }
    }

    public int getProjectCount() {
        return app_metadata.getDatatoolsInfo().projects.length;
    }

    public boolean hasProject(String projectID) {
        if(app_metadata.getDatatoolsInfo() == null || app_metadata.getDatatoolsInfo().projects == null) return false;
        for(Project project : app_metadata.getDatatoolsInfo().projects) {
            if (project.project_id.equals(projectID)) return true;
        }
        return false;
    }

    public boolean canAdministerApplication() {
        if(app_metadata.getDatatoolsInfo() != null && app_metadata.getDatatoolsInfo().permissions != null) {
            for(Permission permission : app_metadata.getDatatoolsInfo().permissions) {
                if(permission.type.equals("administer-application")) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean canAdministerProject(String projectID) {
        if(canAdministerApplication()) return true;
        for(Project project : app_metadata.getDatatoolsInfo().projects) {
            if (project.project_id.equals(projectID)) {
                for(Permission permission : project.permissions) {
                    if(permission.type.equals("administer-project")) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean canViewFeed(String projectID, String feedID) {
        // datatools.stream().filter(datatoolsInfo -> datatoolsInfo.clientId === DataManager.config.get("auth0").get("clientId"))
        for(Project project : app_metadata.getDatatoolsInfo().projects) {
            if (project.project_id.equals(projectID)) {
                String feeds[] = project.defaultFeeds;

                // check for permission-specific feeds
                for(Permission permission : project.permissions) {
                    if(permission.type.equals("view-feed")) {
                        if(permission.feeds != null) feeds = permission.feeds;
                    }
                }

                for(String thisFeedID : feeds) {
                    if(thisFeedID.equals(feedID) || thisFeedID.equals("*")) return true;
                }
            }
        }
        return false;
    }

    public boolean canManageFeed(String projectID, String feedID) {
        for(Project project : app_metadata.getDatatoolsInfo().projects) {
            if (project.project_id.equals(projectID)) {
                for(Permission permission : project.permissions) {
                    if(permission.type.equals("manage-feed")) {
                        for(String thisFeedID : permission.feeds) {
                            if(thisFeedID.equals(feedID) || thisFeedID.equals("*")) return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}
