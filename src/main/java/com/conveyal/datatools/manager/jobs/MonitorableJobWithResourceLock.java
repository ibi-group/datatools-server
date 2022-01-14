package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Contains logic to lock/release feeds and other objects to ensure
 * that jobs on such resources are not executed concurrently.
 */
public abstract class MonitorableJobWithResourceLock<T extends Model> extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(MonitorableJobWithResourceLock.class);

    protected final T resource;
    private final String resourceName;
    private final String resourceClass;
    private final String jobClass;

    /**
     * A set of resources (ids) which have been locked by a instance of {@link MonitorableJobWithResourceLock}
     * to prevent repeat auto-deploy, auto-publishing, etc.
     */
    private static final Set<String> lockedResources = Collections.synchronizedSet(new HashSet<>());

    protected MonitorableJobWithResourceLock(
        Auth0UserProfile owner,
        String name,
        JobType jobType,
        T resource,
        String resourceName
    ) {
        super(owner, name, jobType);
        this.resource = resource;
        this.resourceName = resourceName;
        resourceClass = resource.getClass().getSimpleName();
        jobClass = this.getClass().getSimpleName();
    }

    protected abstract void innerJobLogic() throws Exception;

    @Override
    public void jobLogic() {
        // Determine if the resource is not locked for this job.
        if (
            lockedResources.contains(resource.id)
        ) {
            String message = String.format(
                "%s '%s' skipped for %s execution (another such job is in progress)",
                resourceClass,
                resourceName,
                jobClass
            );
            LOG.info(message);
            status.fail(message);
            return;
        }

        try {
            synchronized (lockedResources) {
                if (!lockedResources.contains(resource.id)) {
                    lockedResources.add(resource.id);
                    LOG.info("{} lock added for {} id '{}'", jobClass, resourceClass, resource.id);
                } else {
                    LOG.warn("Unable to acquire lock for {} '{}'", resourceClass, resourceName);
                    status.fail(String.format("%s '%s' is locked for %s.", resourceClass, resourceName, jobClass));
                    return;
                }
            }
            innerJobLogic();
        } catch (Exception e) {
            status.fail(
                String.format("%s failed for %s '%s'!", jobClass, resourceClass, resourceName),
                e
            );
        } finally {
            lockedResources.remove(resource.id);
            LOG.info("{} lock removed for {} id: '{}'", jobClass, resourceClass, resource.id);
        }
    }
}
