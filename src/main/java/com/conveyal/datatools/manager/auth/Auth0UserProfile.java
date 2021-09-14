package com.conveyal.datatools.manager.auth;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Label;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by demory on 1/18/16.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class Auth0UserProfile {
    String email;
    String user_id;
    AppMetadata app_metadata;

    public Auth0UserProfile() {}

    /**
     * Constructor for creating a mock user (app admin) for testing environment.
     * @param email
     * @param user_id
     */
    public Auth0UserProfile(String email, String user_id) {
        setEmail(email);
        setUser_id(user_id);
        setApp_metadata(new AppMetadata());
    }

    /**
     * Utility method for creating a test admin (with application-admin permissions) user.
     */
    public static Auth0UserProfile createTestAdminUser() {
        return createAdminUser("mock@example.com", "user_id:string");
    }

    /**
     * Utility method for creating a test standard (with no special permissions) user.
     */
    public static Auth0UserProfile createTestViewOnlyUser(String projectId) {
        // Create view feed permission
        Permission viewFeedPermission = new Permission("view-feed", null);
        // Construct user project from project ID with view permissions for all feeds.
        Project project = new Project(projectId, new Permission[]{viewFeedPermission}, new String[]{"*"});
        Auth0UserProfile.DatatoolsInfo standardUserDatatoolsInfo = new Auth0UserProfile.DatatoolsInfo();
        standardUserDatatoolsInfo.projects = new Project[]{project};
        standardUserDatatoolsInfo.setClientId(DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID"));
        Auth0UserProfile.AppMetadata viewOnlyAppMetaData = new Auth0UserProfile.AppMetadata();
        viewOnlyAppMetaData.setDatatoolsInfo(standardUserDatatoolsInfo);

        Auth0UserProfile standardUser = new Auth0UserProfile("nonadminmock@example.com", "user_id:view_only_string");
        standardUser.setApp_metadata(viewOnlyAppMetaData);
        return standardUser;
    }

    /**
     * Utility method for creating a system user (for autonomous server jobs).
     */
    public static Auth0UserProfile createSystemUser() {
        return createAdminUser("system", "user_id:system");
    }

    /**
     * Create an {@link Auth0UserProfile} with application admin permissions.
     */
    private static Auth0UserProfile createAdminUser(String email, String userId) {
        Auth0UserProfile.DatatoolsInfo adminDatatoolsInfo = new Auth0UserProfile.DatatoolsInfo();
        adminDatatoolsInfo.setPermissions(
            new Auth0UserProfile.Permission[]{
                new Auth0UserProfile.Permission("administer-application", new String[]{})
            }
        );
        adminDatatoolsInfo.setClientId(DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID"));
        Auth0UserProfile.AppMetadata adminAppMetaData = new Auth0UserProfile.AppMetadata();
        adminAppMetaData.setDatatoolsInfo(adminDatatoolsInfo);
        Auth0UserProfile adminUser = new Auth0UserProfile(email, userId);
        adminUser.setApp_metadata(adminAppMetaData);
        return adminUser;
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

    public AppMetadata getApp_metadata() { return app_metadata; }

    @JsonIgnore
    public void setDatatoolsInfo(DatatoolsInfo datatoolsInfo) {
        this.app_metadata.getDatatoolsInfo().setClientId(datatoolsInfo.clientId);
        this.app_metadata.getDatatoolsInfo().setPermissions(datatoolsInfo.permissions);
        this.app_metadata.getDatatoolsInfo().setProjects(datatoolsInfo.projects);
        this.app_metadata.getDatatoolsInfo().setSubscriptions(datatoolsInfo.subscriptions);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AppMetadata {
        @JsonProperty("datatools")
        List<DatatoolsInfo> datatools;

        public AppMetadata() {}

        @JsonIgnore
        public void setDatatoolsInfo(DatatoolsInfo datatools) {
            // check if the datatools field hasn't yet been created. Although new users that get created automatically
            // have this set, when running in a test environment, this won't be set, so it should be created.
            if (this.datatools == null) {
                this.datatools = new ArrayList<>();
                this.datatools.add(datatools);
                return;
            }

            for(int i = 0; i < this.datatools.size(); i++) {
                if (this.datatools.get(i).clientId.equals(DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID"))) {
                    this.datatools.set(i, datatools);
                }
            }
        }
        @JsonIgnore
        public DatatoolsInfo getDatatoolsInfo() {
            for(int i = 0; i < this.datatools.size(); i++) {
                DatatoolsInfo dt = this.datatools.get(i);
                if (dt.clientId.equals(DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID"))) {
                    return dt;
                }
            }
            return null;
        }
    }
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DatatoolsInfo {
        @JsonProperty("client_id")
        String clientId;
        Organization[] organizations;
        Project[] projects;
        Permission[] permissions;
        Subscription[] subscriptions;

        public DatatoolsInfo() {}

        public DatatoolsInfo(String clientId, Project[] projects, Permission[] permissions, Organization[] organizations, Subscription[] subscriptions) {
            this.clientId = clientId;
            this.projects = projects;
            this.permissions = permissions;
            this.organizations = organizations;
            this.subscriptions = subscriptions;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public void setProjects(Project[] projects) {
            this.projects = projects;
        }
        public void setOrganizations(Organization[] organizations) {
            this.organizations = organizations;
        }
        public void setPermissions(Permission[] permissions) {
            this.permissions = permissions;
        }

        public void setSubscriptions(Subscription[] subscriptions) {
            this.subscriptions = subscriptions;
        }

        public Subscription[] getSubscriptions() { return subscriptions == null ? new Subscription[0] : subscriptions; }

    }


    public static class Project {

        String project_id;
        Permission[] permissions;
        String[] defaultFeeds;

        public Project() {}

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

        public Permission() {}

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
    public static class Organization {
        @JsonProperty("organization_id")
        String organizationId;
        Permission[] permissions;
//        String name;
//        UsageTier usageTier;
//        Extension[] extensions;
//        Date subscriptionDate;
//        String logoUrl;

        public Organization() {
        }

        public Organization(String organizationId, Permission[] permissions) {
            this.organizationId = organizationId;
            this.permissions = permissions;
        }

        public void setOrganizationId(String organizationId) {
            this.organizationId = organizationId;
        }

        public void setPermissions(Permission[] permissions) {
            this.permissions = permissions;
        }
    }
    public static class Subscription {

        String type;
        String[] target;

        public Subscription() {}

        public Subscription(String type, String[] target) {
            this.type = type;
            this.target = target;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getType() { return type; }

        public void setTarget(String[] target) {
            this.target = target;
        }

        public String[] getTarget() { return target; }
    }

    public int getProjectCount() {
        return app_metadata.getDatatoolsInfo().projects.length;
    }

    public boolean hasProject(String projectID, String organizationId) {
        if (canAdministerApplication()) return true;
        if (canAdministerOrganization(organizationId)) return true;
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

    public boolean canAdministerOrganization() {
        if (canAdministerApplication()) return true;
        Organization org = getAuth0Organization();
        if(app_metadata.getDatatoolsInfo() != null && org != null) {
            for(Permission permission : org.permissions) {
                if(permission.type.equals("administer-organization")) {
                    return true;
                }
            }
        }
        return false;
    }

    public Organization getAuth0Organization() {
        if(app_metadata.getDatatoolsInfo() != null && app_metadata.getDatatoolsInfo().organizations != null && app_metadata.getDatatoolsInfo().organizations.length != 0) {
            return app_metadata.getDatatoolsInfo().organizations[0];
        }
        return null;
    }

    public String getOrganizationId() {
        Organization org = getAuth0Organization();
        if (org != null) {
            return org.organizationId;
        }
        return null;
    }

    public boolean canAdministerOrganization(String organizationId) {
//      TODO: adapt for specific org
        if (organizationId == null) {
            return false;
        }
        Organization org = getAuth0Organization();
        if (org != null && org.organizationId.equals(organizationId)) {
            for(Permission permission : org.permissions) {
                if(permission.type.equals("administer-organization")) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean canAdministerProject(String projectID, String organizationId) {
        if(canAdministerApplication()) return true;
        if(canAdministerOrganization(organizationId)) return true;
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

    public boolean canAdministerProject(FeedSource feedSource) {
        return canAdministerProject(feedSource.projectId, feedSource.organizationId());
    }

    public boolean canAdministerProject(com.conveyal.datatools.manager.models.Project project) {
        return canAdministerProject(project.id, project.organizationId);
    }

    public boolean canAdministerProject(Label label) {
        return canAdministerProject(label.projectId, label.organizationId());
    }

    public boolean canAdministerProject(Deployment deployment) {
        return canAdministerProject(deployment.projectId, deployment.organizationId());
    }

    public boolean canAdministerProject(OtpServer server) {
        return canAdministerProject(server.projectId, server.organizationId());
    }

    /** Check that user can administer project. Organization ID is drawn from persisted project. */
    public boolean canAdministerProject(String projectId) {
        if (canAdministerApplication()) return true;
        com.conveyal.datatools.manager.models.Project p = Persistence.projects.getById(projectId);
        if (p != null && canAdministerOrganization(p.organizationId)) return true;
        for (Project project : app_metadata.getDatatoolsInfo().projects) {
            if (project.project_id.equals(projectId)) {
                for (Permission permission : project.permissions) {
                    if (permission.type.equals("administer-project")) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean canViewFeed(String organizationId, String projectID, String feedID) {
        if (canAdministerApplication() || canAdministerProject(projectID, organizationId)) {
            return true;
        }
        for(Project project : app_metadata.getDatatoolsInfo().projects) {
            if (project.project_id.equals(projectID)) {
                return checkFeedPermission(project, feedID, "view-feed");
            }
        }
        return false;
    }

    public boolean canViewFeed(FeedSource feedSource) {
        if (canAdministerApplication() || canAdministerProject(feedSource.projectId, feedSource.organizationId())) {
            return true;
        }
        for(Project project : app_metadata.getDatatoolsInfo().projects) {
            if (project.project_id.equals(feedSource.projectId)) {
                return checkFeedPermission(project, feedSource.id, "view-feed");
            }
        }
        return false;
    }

    /** Check that user has manage feed or view feed permissions. */
    public boolean canManageOrViewFeed(String organizationId, String projectID, String feedID) {
        return canManageFeed(organizationId, projectID, feedID) || canViewFeed(organizationId, projectID, feedID);
    }

    private boolean canManageFeed(String organizationId, String projectID, String feedID) {
        if (canAdministerApplication() || canAdministerProject(projectID, organizationId)) {
            return true;
        }
        Project[] projectList = app_metadata.getDatatoolsInfo().projects;
        for(Project project : projectList) {
            if (project.project_id.equals(projectID)) {
                return checkFeedPermission(project, feedID, "manage-feed");
            }
        }
        return false;
    }

    public boolean canManageFeed(FeedSource feedSource) {
        if (canAdministerApplication() || canAdministerProject(feedSource.projectId, feedSource.organizationId())) {
            return true;
        }
        Project[] projectList = app_metadata.getDatatoolsInfo().projects;
        for(Project project : projectList) {
            if (project.project_id.equals(feedSource.projectId)) {
                return checkFeedPermission(project, feedSource.id, "manage-feed");
            }
        }
        return false;
    }

    private boolean canEditGTFS(String organizationId, String projectID, String feedID) {
        if (canAdministerApplication() || canAdministerProject(projectID, organizationId)) {
            return true;
        }
        Project[] projectList = app_metadata.getDatatoolsInfo().projects;
        for(Project project : projectList) {
            if (project.project_id.equals(projectID)) {
                return checkFeedPermission(project, feedID, "edit-gtfs");
            }
        }
        return false;
    }

    public boolean canEditGTFS(FeedSource feedSource) {
        if (canAdministerApplication() || canAdministerProject(feedSource.projectId, feedSource.organizationId())) {
            return true;
        }
        Project[] projectList = app_metadata.getDatatoolsInfo().projects;
        for(Project project : projectList) {
            if (project.project_id.equals(feedSource.projectId)) {
                return checkFeedPermission(project, feedSource.id, "edit-gtfs");
            }
        }
        return false;
    }

    public boolean canApproveGTFS(String organizationId, String projectID, String feedID) {
        if (canAdministerApplication() || canAdministerProject(projectID, organizationId)) {
            return true;
        }
        Project[] projectList = app_metadata.getDatatoolsInfo().projects;
        for(Project project : projectList) {
            if (project.project_id.equals(projectID)) {
                return checkFeedPermission(project, feedID, "approve-gtfs");
            }
        }
        return false;
    }

    public boolean checkFeedPermission(Project project, String feedID, String permissionType) {
        String feeds[] = project.defaultFeeds;

        // check for permission-specific feeds
        for (Permission permission : project.permissions) {
            if(permission.type.equals(permissionType)) {
                // if specific feeds apply to permission (rather than default set), reassign feeds list
                if(permission.feeds != null) {
                    feeds = permission.feeds;
                }
                // if permission is found in project, check that it applies to the feed requested
                for(String thisFeedID : feeds) {
                    if (thisFeedID.equals(feedID) || thisFeedID.equals("*")) {
                        return true;
                    }
                }
            }
        }
        // if no permissionType + feedID combo was found
        return false;
    }

    @JsonIgnore
    public com.conveyal.datatools.manager.models.Organization getOrganization () {
        Organization[] orgs = getApp_metadata().getDatatoolsInfo().organizations;
        if (orgs != null && orgs.length != 0) {
            return orgs[0] != null ? Persistence.organizations.getById(orgs[0].organizationId) : null;
        }
        return null;
    }
}
