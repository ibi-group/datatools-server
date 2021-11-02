package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.auth.Actions;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Label;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.getPOJOFromRequestBody;
import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;


public class LabelController {
    private static JsonManager<Label> json = new JsonManager<>(Label.class, JsonViews.UserInterface.class);


    /**
     * Spark HTTP endpoint to get a single label by ID
     */
    private static Label getLabel(Request req, Response res) {
        return requestLabelById(req, Actions.VIEW);
    }

    /**
     * Spark HTTP endpoint that handles getting all labels for a handful of use cases:
     * - for a single project (if projectId query param provided)
     */
    private static Collection<Label> getAllLabels(Request req, Response res) {
        Auth0UserProfile user = req.attribute("user");
        String projectId = req.queryParams("projectId");
        Project project = Persistence.projects.getById(projectId);
        if (project == null) {
            logMessageAndHalt(req, 400, "Must provide valid projectId query param to retrieve labels.");
        }

        boolean isProjectAdmin = user.canAdministerProject(project);

        return project.retrieveProjectLabels(isProjectAdmin);
    }

    /**
     * HTTP endpoint to create a new label
     */
    private static Label createLabel(Request req, Response res) throws IOException {
        Label newLabel = getPOJOFromRequestBody(req, Label.class);
        validate(req, newLabel);
        // User may not be allowed to create a new label
        checkLabelPermissions(req, newLabel, Actions.CREATE);

        try {
            Persistence.labels.create(newLabel);
            return newLabel;
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Unknown error encountered creating label.", e);
            return null;
        }
    }

    /**
     * Check that updated or new label object is valid. This method should be called before a label is
     * persisted to the database.
     */
    private static void validate(Request req, Label label) {
        // Label is quite forgiving (sets defaults if null) and the boolean value is type checked,
        // so there is little to validate.
        if (label.name.length() > 25) {
            logMessageAndHalt(
                    req,
                    HttpStatus.BAD_REQUEST_400,
                    "Request was invalid, the name may not be longer than 25 characters."
            );
        }
        if (label.description.length() > 50) {
            logMessageAndHalt(
                    req,
                    HttpStatus.BAD_REQUEST_400,
                    "Request was invalid, the description may not be longer than 50 characters."
            );
        }
        if (StringUtils.isEmpty(label.name)) {
            logMessageAndHalt(
                    req,
                    HttpStatus.BAD_REQUEST_400,
                    "Request was invalid, the name field must not be empty."
            );
        }
    }

    /**
     * Spark HTTP endpoint to update a label. Requires that the JSON body represent all
     * fields the updated label should contain. See a similar note on FeedSourceController for more info
     */
    private static Label updateLabel(Request req, Response res) throws IOException {
        String labelId = req.params("id");
        Label formerLabel = requestLabelById(req, Actions.MANAGE);
        Label updatedLabel = getPOJOFromRequestBody(req, Label.class);

        // Some things shouldn't be updated
        updatedLabel.projectId = formerLabel.projectId;
        updatedLabel.id = formerLabel.id;

        validate(req, updatedLabel);

        Persistence.labels.replace(labelId, updatedLabel);

        return updatedLabel;
    }
    /**
     * Spark HTTP endpoint to delete a label
     *
     * FIXME: Should this just set a "deleted" flag instead of removing from the database entirely?
     */
    private static Label deleteLabel(Request req, Response res) {
        Label label = requestLabelById(req, Actions.MANAGE);

        // More specific error message if label doesn't exist
        if (label == null) {
            logMessageAndHalt(req, 403, "Label does not exist.");
            return null;
        }

        try {
            label.delete();
            return label;
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Unknown error occurred while deleting label.", e);
            return null;
        }
    }

    /**
     * Helper function returns label if user has permission for specified action.
     * @param req spark Request object from API request
     * @param action action type (either "view" or Permission.MANAGE)
     * @return label object for ID
     */
    private static Label requestLabelById(Request req, Actions action) {
        String id = req.params("id");
        if (id == null) {
            logMessageAndHalt(req, 400, "Please specify id param.");
        }
        Label label = Persistence.labels.getById(id);
        checkLabelPermissions(req, label, action);

        return label;
    }

    /** Helper method will halt execution and throw an error message if a given
     * label is not allowed to be accessed by the current user (taken from the req object)
     *
     * @param req       Spark request used for determining user permissions
     * @param label     Label to check
     * @param action    Action to be taken on label, which changes who can do what
     */
    public static void checkLabelPermissions(Request req, Label label, Actions action) {
        Auth0UserProfile userProfile = req.attribute("user");
        // check for null label
        if (label == null) {
            logMessageAndHalt(req, 400, "Label ID does not exist.");
            return;
        }

        boolean isProjectAdmin = userProfile.canAdministerProject(label);
        switch (action) {
            case CREATE:
            case MANAGE:
            case EDIT:
                // Only project admins can edit, manage, or create labels
                if (!isProjectAdmin) {
                    logMessageAndHalt(req, 403, "User is not admin so cannot update or create labels.");
                }
                break;
            case VIEW:
                if (label.adminOnly && !isProjectAdmin) {
                    logMessageAndHalt(req, 403, "User is not admin so cannot view admin-only label.");
                }
                break;
            default:
                // Incorrect query, so fail
                logMessageAndHalt(req, 400, "Not enough information supplied to determine label access.");
                break;
        }

        // If we make it here, user has permission and the requested label is valid
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/label/:id", LabelController::getLabel, json::write);
        get(apiPrefix + "secure/label", LabelController::getAllLabels, json::write);
        post(apiPrefix + "secure/label", LabelController::createLabel, json::write);
        put(apiPrefix + "secure/label/:id", LabelController::updateLabel, json::write);
        delete(apiPrefix + "secure/label/:id", LabelController::deleteLabel, json::write);
    }
}
