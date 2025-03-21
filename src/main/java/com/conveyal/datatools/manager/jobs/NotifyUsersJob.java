package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.DataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.datatools.manager.controllers.api.UserController.inTestingEnvironment;

public abstract class NotifyUsersJob implements Runnable {

    public static final Logger LOG = LoggerFactory.getLogger(NotifyUsersJob.class);
    protected final String applicationName;
    protected static final String DEFAULT_NAME = "Data Tools";
    protected static final String APPLICATION_NAME = DataManager.getConfigPropertyAsText("application.title");
    protected static final String APPLICATION_URL = DataManager.getConfigPropertyAsText("application.public_url");

    protected NotifyUsersJob() {
        if (APPLICATION_NAME == null) {
            LOG.warn("Configuration property \"application.title\" must be set to customize notifications.");
            applicationName = DEFAULT_NAME;
        } else {
            applicationName = APPLICATION_NAME;
        }
    }

    protected static boolean hasInvalidApplicationURL() {
        if (APPLICATION_URL == null || !(APPLICATION_URL.startsWith("https://") || APPLICATION_URL.startsWith("http://"))) {
            LOG.error("application.public_url (value={}) property must be set to a valid URL in order to send notifications to users.", APPLICATION_URL);
            return true;
        }
        return false;
    }

    @Override
    public void run() {
        if (!inTestingEnvironment()) {
            // Only notify users if not in testing environment.
            notifyUsers();
        }
    }

    protected abstract void notifyUsers();
}
