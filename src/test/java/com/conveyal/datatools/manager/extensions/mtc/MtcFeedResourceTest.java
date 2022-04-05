package com.conveyal.datatools.manager.extensions.mtc;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Date;

import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.parseJson;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MtcFeedResourceTest extends UnitTest {
    private static Project project;
    private static FeedSource feedSource;
    private static WireMockServer wireMockServer;

    private static final String AGENCY_CODE = "DE";

    /**
     * Add project, server, and deployment to prepare for tests.
     */
    @BeforeAll
    static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        // Create a project, feed sources.
        project = new Project();
        project.name = String.format("Test %s", new Date());
        Persistence.projects.create(project);

        feedSource = new FeedSource("Test feed source");
        feedSource.projectId = project.id;
        Persistence.feedSources.create(feedSource);

        // This sets up a mock server that accepts requests and sends predefined responses to mock an Auth0 server.
        wireMockServer = new WireMockServer(
            options()
                .usingFilesUnderDirectory("src/test/resources/com/conveyal/datatools/mtc-rtd-mock-responses/")
        );
        wireMockServer.start();
    }

    @AfterAll
    static void tearDown() {
        wireMockServer.stop();
        if (project != null) {
            project.delete();
        }
    }

    @Test
    void shouldConvertRtdNullToEmptyString() {
        assertThat(MtcFeedResource.convertRtdString("null"), equalTo(""));
        assertThat(MtcFeedResource.convertRtdString("Other text"), equalTo("Other text"));
    }

    @Test
    void canUpdateFeedExternalPropertiesToMongo() throws IOException {
        final String rtdCarrierApiPath = "/api/Carrier/" + AGENCY_CODE;

        // create wiremock stub for get users endpoint
        wireMockServer.stubFor(
            get(urlPathEqualTo(rtdCarrierApiPath))
                .willReturn(
                    aResponse()
                        .withBodyFile("rtdGetResponse.json")
                )
        );

        // Set up some entries in the ExternalFeedSourceProperties collection.
        // This one (AgencyId) should not change.
        ExternalFeedSourceProperty agencyIdProp = new ExternalFeedSourceProperty(
            feedSource,
            "MTC",
            "AgencyId",
            AGENCY_CODE
        );
        Persistence.externalFeedSourceProperties.create(agencyIdProp);

        // This one (AgencyPublicId) should be deleted after this test (not in RTD response).
        ExternalFeedSourceProperty agencyPublicIdProp = new ExternalFeedSourceProperty(
            feedSource,
            "MTC",
            "AgencyPublicId",
            AGENCY_CODE
        );
        Persistence.externalFeedSourceProperties.create(agencyPublicIdProp);

        // This one (AgencyEmail) should be updated with this test.
        ExternalFeedSourceProperty agencyEmailProp = new ExternalFeedSourceProperty(
            feedSource,
            "MTC",
            "AgencyEmail",
            "old@email.example.com"
        );
        Persistence.externalFeedSourceProperties.create(agencyEmailProp);

        // make RTD request and parse the json response
        JsonNode rtdResponse = parseJson(
            given()
                .get(rtdCarrierApiPath)
                .then()
                .extract()
                .response()
                .asString()
        );
        // Also extract desired values from response
        String responseEmail = rtdResponse.get("AgencyEmail").asText();
        String responseAgencyName = rtdResponse.get("AgencyName").asText();

        // Update MTC Feed properties in Mongo based response.
        new MtcFeedResource().updateMongoExternalFeedProperties(feedSource, rtdResponse);

        // Existing field AgencyId should retain the same value.
        ExternalFeedSourceProperty updatedAgencyIdProp = Persistence.externalFeedSourceProperties.getById(agencyIdProp.id);
        assertThat(updatedAgencyIdProp.value, equalTo(agencyIdProp.value));

        // Existing field AgencyEmail should be updated from RTD response.
        ExternalFeedSourceProperty updatedEmailProp = Persistence.externalFeedSourceProperties.getById(agencyEmailProp.id);
        assertThat(updatedEmailProp.value, equalTo(responseEmail));

        // New field AgencyName (not set up above) from RTD response should be added to Mongo.
        ExternalFeedSourceProperty newAgencyNameProp = Persistence.externalFeedSourceProperties.getOneFiltered(
            and(
                eq("feedSourceId", feedSource.id),
                eq("resourceType", "MTC"),
                eq("name", "AgencyName")            )
        );
        assertThat(newAgencyNameProp, notNullValue());
        assertThat(newAgencyNameProp.value, equalTo(responseAgencyName));

        // Removed field AgencyPublicId from RTD should be deleted from Mongo.
        ExternalFeedSourceProperty removedPublicIdProp = Persistence.externalFeedSourceProperties.getById(agencyPublicIdProp.id);
        assertThat(removedPublicIdProp, nullValue());

        Persistence.externalFeedSourceProperties.removeById(agencyIdProp.id);
        Persistence.externalFeedSourceProperties.removeById(agencyPublicIdProp.id);
        Persistence.externalFeedSourceProperties.removeById(agencyEmailProp.id);
    }

    @Test
    void shouldTolerateNullObjectInExternalPropertyAgencyId() throws IOException {
        // Add an entry in the ExternalFeedSourceProperties collection
        // with AgencyId value set to null.
        ExternalFeedSourceProperty agencyIdProp = new ExternalFeedSourceProperty(
            feedSource,
            "MTC",
            "AgencyId",
            null
        );
        Persistence.externalFeedSourceProperties.create(agencyIdProp);

        // Trigger the feed update process (it should not upload anything to S3).
        FeedVersion feedVersion = createFeedVersion(feedSource,  zipFolderFiles("mini-bart-new"));
        MtcFeedResource mtcFeedResource = new MtcFeedResource();
        assertDoesNotThrow(() -> mtcFeedResource.feedVersionCreated(feedVersion, null));

        Persistence.externalFeedSourceProperties.removeById(agencyIdProp.id);
    }

    @ParameterizedTest
    @ValueSource(strings = {"123 Transit Avenue, Anywhere USA 99123", "  ", ""})
    void shouldUpdateRtdCarrierProperty(String address) throws JsonProcessingException {
        // If a property changes, the blob sent to RTD should contain the new property value.
        // (For this test, it does not matter which property gets updated.)
        ExternalFeedSourceProperty newAddressProp = new ExternalFeedSourceProperty(
            feedSource,
            "MTC",
            "AgencyAddress",
            address
        );

        RtdCarrier carrier = new RtdCarrier(feedSource);
        carrier.updateProperty(newAddressProp);
        assertThat(carrier.AgencyAddress, equalTo(address));

        String jsonAddress = address.trim().isEmpty() ? "null" : String.format("\"%s\"", address);
        assertTrue(
            carrier.toJson().contains(String.format(",\"AgencyAddress\":%s,", jsonAddress))
        );
    }
}
