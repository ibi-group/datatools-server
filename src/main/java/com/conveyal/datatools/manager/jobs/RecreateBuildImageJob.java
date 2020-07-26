package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
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

import static com.conveyal.datatools.manager.models.EC2Info.AMI_CONFIG_PATH;

/**
 * Job that is dispatched during a {@link DeployJob} that spins up EC2 instances. This handles waiting for a graph build
 * image to be created after a graph build has completed.
 */
public class RecreateBuildImageJob extends MonitorableJob {
    private final DeployJob parentDeployJob;
    private final List<Instance> graphBuildingInstances;
    private final OtpServer otpServer;

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
    public void jobLogic() throws Exception {
        status.update("Creating build image", 5);
        // Create a new image of this instance.
        CreateImageRequest createImageRequest = new CreateImageRequest()
            .withInstanceId(graphBuildingInstances.get(0).getInstanceId())
            .withName(otpServer.ec2Info.buildImageName)
            .withDescription(otpServer.ec2Info.buildImageDescription);
        CreateImageResult createImageResult = parentDeployJob.getEC2Client().createImage(createImageRequest);
        // Wait for image creation to complete
        String createdImageId = createImageResult.getImageId();
        status.update("Waiting for graph build image to be created...", 25);
        boolean imageCreated = false;
        DescribeImagesRequest describeImagesRequest = new DescribeImagesRequest()
            .withImageIds(createdImageId);
        while (!imageCreated) {
            DescribeImagesResult describeImagesResult = parentDeployJob
                .getEC2Client()
                .describeImages(describeImagesRequest);
            for (Image image : describeImagesResult.getImages()) {
                if (image.getImageId().equals(createdImageId)) {
                    // obtain the image state.
                    // See https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/ec2/model/ImageState.html
                    String imageState = image.getState().toLowerCase();
                    if (imageState.equals("pending")) {
                        // wait 2.5 seconds before making next request
                        Thread.sleep(2500);
                    } else if (imageState.equals("available")) {
                        // success! Set imageCreated to true.
                        imageCreated = true;
                    } else {
                        // Any other image state is assumed to be a failure
                        status.fail(
                            String.format("Graph build image creation failed! Image state became `%s`", imageState)
                        );
                        return;
                    }
                }
            }
        }
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
            parentDeployJob.getEC2Client().deregisterImage(deregisterImageRequest);
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
                ServerController.terminateInstances(parentDeployJob.getEC2Client(), graphBuildingInstances);
            } catch (AmazonEC2Exception e) {
                status.fail(
                    "Graph build image successfully created, but failed to terminate graph building instance!",
                    e
                );
            }
        }
        status.completeSuccessfully("Graph build image successfully created!");
    }
}
