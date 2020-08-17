package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.CreateImageRequest;
import com.amazonaws.services.ec2.model.CreateImageResult;
import com.amazonaws.services.ec2.model.DeregisterImageRequest;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Instance;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.controllers.api.ServerController;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.persistence.Persistence;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.conveyal.datatools.manager.models.EC2Info.AMI_CONFIG_PATH;

/**
 * Job that is dispatched during a {@link DeployJob} that spins up EC2 instances. This handles waiting for a graph build
 * image to be created after a graph build has completed.
 */
public class RecreateBuildImageJob extends MonitorableJob {
    private final AmazonEC2 ec2;
    private final List<Instance> graphBuildingInstances;
    private final OtpServer otpServer;

    public RecreateBuildImageJob(
        Auth0UserProfile owner,
        List<Instance> graphBuildingInstances,
        OtpServer otpServer,
        AmazonEC2 ec2
    ) {
        super(owner, String.format("Recreating build image for %s", otpServer.name), JobType.RECREATE_BUILD_IMAGE);
        this.graphBuildingInstances = graphBuildingInstances;
        this.otpServer = otpServer;
        this.ec2 = ec2;
    }

    @Override
    public void jobLogic() throws Exception {
        status.update("Creating build image", 5);
        // Create a new image of this instance.
        CreateImageRequest createImageRequest = new CreateImageRequest()
            .withInstanceId(graphBuildingInstances.get(0).getInstanceId())
            .withName(otpServer.ec2Info.buildImageName)
            .withDescription(otpServer.ec2Info.buildImageDescription);
        CreateImageResult createImageResult = ec2.createImage(createImageRequest);
        // Wait for image creation to complete (it can take a few minutes)
        String createdImageId = createImageResult.getImageId();
        status.update("Waiting for graph build image to be created...", 25);
        boolean imageCreated = false;
        DescribeImagesRequest describeImagesRequest = new DescribeImagesRequest()
            .withImageIds(createdImageId);
        // wait for the image to be created. Also, make sure the parent DeployJob hasn't failed this job already.
        while (!imageCreated && !status.error) {
            DescribeImagesResult describeImagesResult = ec2.describeImages(describeImagesRequest);
            for (Image image : describeImagesResult.getImages()) {
                if (image.getImageId().equals(createdImageId)) {
                    // obtain the image state.
                    // See https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/ec2/model/ImageState.html
                    String imageState = image.getState().toLowerCase();
                    if (imageState.equals("pending")) {
                        if (System.currentTimeMillis() - status.startTime > TimeUnit.HOURS.toMillis(1)) {
                            terminateInstanceAndFailWithMessage(
                                "It has taken over an hour for the graph build image to be created! Check the AWS console to see if the image was created successfully."
                            );
                            return;
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
        }
        // If the parent DeployJob has already failed this job, exit immediately.
        if (status.error) return;
        status.update("Graph build image successfully created!", 70);
        // Deregister old image if it exists and is not the default datatools AMI ID and is not the server AMI ID
        String graphBuildAmiId = otpServer.ec2Info.buildAmiId;
        if (
            graphBuildAmiId != null &&
                !DataManager.getConfigPropertyAsText(AMI_CONFIG_PATH).equals(graphBuildAmiId) &&
                !graphBuildAmiId.equals(otpServer.ec2Info.amiId)
        ) {
            status.message = "Deregistering old build image";
            DeregisterImageRequest deregisterImageRequest = new DeregisterImageRequest()
                .withImageId(graphBuildAmiId);
            ec2.deregisterImage(deregisterImageRequest);
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
            ServerController.terminateInstances(ec2, graphBuildingInstances);
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
        ServerController.terminateInstances(ec2, graphBuildingInstances);
        status.fail(message, e);
    }
}
