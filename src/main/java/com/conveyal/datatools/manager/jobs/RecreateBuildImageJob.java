package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.ec2.model.CreateImageRequest;
import com.amazonaws.services.ec2.model.CreateImageResult;
import com.amazonaws.services.ec2.model.DeregisterImageRequest;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Instance;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.aws.EC2Utils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.TimeTracker;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Job that is dispatched during a {@link DeployJob} that spins up EC2 instances. This handles waiting for a graph build
 * image to be created after a graph build has completed. If an error occurs, or if the image was created successfully
 * and has a separate graph build instance type, the EC2 instance will be terminated.
 */
public class RecreateBuildImageJob extends MonitorableJob {
    private final List<Instance> graphBuildingInstances;
    private final OtpServer otpServer;
    private final DeployJob parentDeployJob;

    public RecreateBuildImageJob(
        DeployJob parentDeployJob,
        Auth0UserProfile owner,
        List<Instance> graphBuildingInstances
    ) {
        super(
            owner,
            String.format("Recreating build image for %s", parentDeployJob.getOtpServer().name),
            JobType.RECREATE_BUILD_IMAGE
        );
        this.parentDeployJob = parentDeployJob;
        this.otpServer = parentDeployJob.getOtpServer();
        this.graphBuildingInstances = graphBuildingInstances;
    }

    @Override
    public void jobLogic() {
        status.update("Creating build image", 5);
        // Create a new image of this instance.
        CreateImageRequest createImageRequest = new CreateImageRequest()
            .withInstanceId(graphBuildingInstances.get(0).getInstanceId())
            .withName(otpServer.ec2Info.buildImageName)
            .withDescription(otpServer.ec2Info.buildImageDescription);
        CreateImageResult createImageResult = null;
        try {
            createImageResult = parentDeployJob
                .getEC2ClientForDeployJob()
                .createImage(createImageRequest);
        } catch (Exception e) {
            status.fail("Failed to make a request to create a new image!", e);
            return;
        }

        // Wait for the image to be created (it can take a few minutes). Also, make sure the parent DeployJob hasn't
        // failed this job already.
        TimeTracker imageCreationTracker = new TimeTracker(1, TimeUnit.HOURS);
        String createdImageId = createImageResult.getImageId();
        status.update("Waiting for graph build image to be created...", 25);
        boolean imageCreated = false;
        DescribeImagesRequest describeImagesRequest = new DescribeImagesRequest()
            .withImageIds(createdImageId);
        while (!imageCreated && !status.error) {
            DescribeImagesResult describeImagesResult = null;
            try {
                describeImagesResult = parentDeployJob
                    .getEC2ClientForDeployJob()
                    .describeImages(describeImagesRequest);
            } catch (Exception e) {
                terminateInstanceAndFailWithMessage("Failed to make request to get image creation status!", e);
                return;
            }
            for (Image image : describeImagesResult.getImages()) {
                if (image.getImageId().equals(createdImageId)) {
                    // obtain the image state.
                    // See https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/ec2/model/ImageState.html
                    String imageState = image.getState().toLowerCase();
                    if (imageState.equals("pending")) {
                        if (imageCreationTracker.hasTimedOut()) {
                            terminateInstanceAndFailWithMessage(
                                "It has taken over an hour for the graph build image to be created! Check the AWS console to see if the image was created successfully."
                            );
                            return;
                        }
                    } else if (imageState.equals("available")) {
                        // success! Set imageCreated to true.
                        imageCreated = true;
                    } else {
                        // Any other image state is assumed to be a failure
                        terminateInstanceAndFailWithMessage(
                            String.format("Graph build image creation failed! Image state became `%s`", imageState)
                        );
                        return;
                    }
                }
            }
            // wait 2.5 seconds before making next request
            try {
                Thread.sleep(2500);
            } catch (InterruptedException e) {
                terminateInstanceAndFailWithMessage(
                    "Failed while waiting for graph build image creation to complete!",
                    e
                );
                return;
            }
        }
        // If the parent DeployJob has already failed this job, exit immediately.
        if (status.error) return;
        status.update("Graph build image successfully created!", 70);
        // Deregister old image if it exists and is not the default datatools AMI ID and is not the server AMI ID
        String graphBuildAmiId = otpServer.ec2Info.buildAmiId;
        if (
            graphBuildAmiId != null &&
                !EC2Utils.DEFAULT_AMI_ID.equals(graphBuildAmiId) &&
                !graphBuildAmiId.equals(otpServer.ec2Info.amiId)
        ) {
            status.message = "Deregistering old build image";
            DeregisterImageRequest deregisterImageRequest = new DeregisterImageRequest()
                .withImageId(graphBuildAmiId);
            try {
                parentDeployJob.getEC2ClientForDeployJob().deregisterImage(deregisterImageRequest);
            } catch (Exception e) {
                terminateInstanceAndFailWithMessage("Failed to deregister previous graph building image!", e);
                return;
            }
        }
        status.update("Updating Server build AMI info", 80);
        // Update OTP Server info
        otpServer.ec2Info.buildAmiId = createdImageId;
        otpServer.ec2Info.recreateBuildImage = false;
        Persistence.servers.replace(otpServer.id, otpServer);
        status.update("Server build AMI info updated", 90);
        // terminate graph building instance if needed
        if (otpServer.ec2Info.hasSeparateGraphBuildConfig()) {
            status.message = "Terminating graph building instance";
            try {
                EC2Utils.terminateInstances(parentDeployJob.getEC2ClientForDeployJob(), graphBuildingInstances);
            } catch (Exception e) {
                status.fail(
                    "Graph build image successfully created, but failed to terminate graph building instance!",
                    e
                );
                return;
            }
        }
        status.completeSuccessfully("Graph build image successfully created!");
    }

    private void terminateInstanceAndFailWithMessage(String message) {
        terminateInstanceAndFailWithMessage(message, null);
    }

    /**
     * Terminates the graph building instance and fails with the given message and Exception.
     */
    private void terminateInstanceAndFailWithMessage(String message, Exception e) {
        try {
            EC2Utils.terminateInstances(parentDeployJob.getEC2ClientForDeployJob(), graphBuildingInstances);
        } catch (Exception terminationException) {
            status.fail(
                String.format("%s Also, the graph building instance failed to terminate!", message),
                terminationException
            );
            return;
        }
        status.fail(message, e);
    }
}
