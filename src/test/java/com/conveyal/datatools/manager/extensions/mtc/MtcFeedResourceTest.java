package com.conveyal.datatools.manager.extensions.mtc;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.SqlAssert;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.parseJson;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource.STOP_CODE_PRIMARY_PREFIX_FIELD_NAME;
import static com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource.STOP_CODE_SECONDARY_PREFIXES_FIELD_NAME;
import static com.conveyal.gtfs.error.NewGTFSErrorType.MISSING_STOP_CODE_PREFIX;
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
        Auth0Connection.setAuthDisabled(true);
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
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
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
        String agencyIdPropId = createExternalFeedSourceProperties("AgencyId", AGENCY_CODE);

        // This one (AgencyPublicId) should be deleted after this test (not in RTD response).
        String agencyPublicIdPropId = createExternalFeedSourceProperties("AgencyPublicId", AGENCY_CODE);

        // This one (AgencyEmail) should be updated with this test.
        String agencyEmailPropId = createExternalFeedSourceProperties("AgencyEmail", "old@email.example.com");

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
        String responsePrimaryPrefix = rtdResponse.get(STOP_CODE_PRIMARY_PREFIX_FIELD_NAME).asText();
        String responseSecondaryPrefixes = rtdResponse.get(STOP_CODE_SECONDARY_PREFIXES_FIELD_NAME).asText();

        // Update MTC Feed properties in Mongo based response.
        new MtcFeedResource().updateMongoExternalFeedProperties(feedSource, rtdResponse);

        // Existing field AgencyId should retain the same value.
        ExternalFeedSourceProperty updatedAgencyIdProp = Persistence.externalFeedSourceProperties.getById(agencyIdPropId);
        assertThat(updatedAgencyIdProp.value, equalTo(AGENCY_CODE));

        // Existing field AgencyEmail should be updated from RTD response.
        ExternalFeedSourceProperty updatedEmailProp = Persistence.externalFeedSourceProperties.getById(agencyEmailPropId);
        assertThat(updatedEmailProp.value, equalTo(responseEmail));

        checkForRTDPropInMongo("AgencyName", responseAgencyName);
        String primaryPrefixPropId = checkForRTDPropInMongo(STOP_CODE_PRIMARY_PREFIX_FIELD_NAME, responsePrimaryPrefix);
        String secondaryPrefixesPropId = checkForRTDPropInMongo(STOP_CODE_SECONDARY_PREFIXES_FIELD_NAME, responseSecondaryPrefixes);

        // Removed field AgencyPublicId from RTD should be deleted from Mongo.
        ExternalFeedSourceProperty removedPublicIdProp = Persistence.externalFeedSourceProperties.getById(agencyPublicIdPropId);
        assertThat(removedPublicIdProp, nullValue());

        Persistence.externalFeedSourceProperties.removeById(agencyIdPropId);
        Persistence.externalFeedSourceProperties.removeById(agencyPublicIdPropId);
        Persistence.externalFeedSourceProperties.removeById(agencyEmailPropId);
        Persistence.externalFeedSourceProperties.removeById(primaryPrefixPropId);
        Persistence.externalFeedSourceProperties.removeById(secondaryPrefixesPropId);
    }

    /**
     * Check that new fields from RTD response are added to Mongo.
     */
    private String checkForRTDPropInMongo(String propName, String expectedValue) {
        ExternalFeedSourceProperty prop = Persistence.externalFeedSourceProperties.getOneFiltered(
            and(
                eq("feedSourceId", feedSource.id),
                eq("resourceType", "MTC"),
                eq("name", propName)
            )
        );
        assertThat(prop, notNullValue());
        assertThat(prop.value, equalTo(expectedValue));
        return prop.id;
    }

    @Test
    void shouldTolerateNullObjectInExternalPropertyAgencyId() throws IOException {
        // Add an entry in the ExternalFeedSourceProperties collection
        // with AgencyId value set to null.
        String agencyIdProp = createExternalFeedSourceProperties("AgencyId", null);
        // Trigger the feed update process (it should not upload anything to S3).
        FeedVersion feedVersion = createFeedVersion(feedSource, zipFolderFiles("mtc-feed-resource-test"));
        MtcFeedResource mtcFeedResource = new MtcFeedResource();
        assertDoesNotThrow(() -> mtcFeedResource.feedVersionCreated(feedVersion, null));
        Persistence.externalFeedSourceProperties.removeById(agencyIdProp);
    }

    @Test
    void shouldValidateStopCodePrefixes() throws IOException, SQLException {
        String primaryPrefixPropId = createExternalFeedSourceProperties(STOP_CODE_PRIMARY_PREFIX_FIELD_NAME, "primary-1");
        String secondaryPrefixesPropId = createExternalFeedSourceProperties(STOP_CODE_SECONDARY_PREFIXES_FIELD_NAME, "secondary-1,secondary-2");

        FeedVersion feedVersion = createFeedVersion(feedSource, zipFolderFiles("mtc-feed-resource-test"));
        MtcFeedResource mtcFeedResource = new MtcFeedResource();
        assertDoesNotThrow(() -> mtcFeedResource.feedVersionCreated(feedVersion, null));
        SqlAssert sqlAssert = new SqlAssert(feedVersion);
        sqlAssert.errors.assertCount(1, String.format("error_type='%s'", MISSING_STOP_CODE_PREFIX.name()));
        Persistence.externalFeedSourceProperties.removeById(primaryPrefixPropId);
        Persistence.externalFeedSourceProperties.removeById(secondaryPrefixesPropId);
    }

    private String createExternalFeedSourceProperties(String name, String value) {
        ExternalFeedSourceProperty prop = new ExternalFeedSourceProperty(
            feedSource,
            "MTC",
            name,
            value
        );
        Persistence.externalFeedSourceProperties.create(prop);
        return prop.id;
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
