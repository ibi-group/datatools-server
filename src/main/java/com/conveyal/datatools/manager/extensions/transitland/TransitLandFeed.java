package com.conveyal.datatools.manager.extensions.transitland;

import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by demory on 3/31/16.
 */

public class TransitLandFeed {

    @JsonProperty
    String onestop_id;

    @JsonProperty
    String url;

    @JsonProperty
    String feed_format;

    @JsonProperty
    String tags;

    @JsonIgnore
    String geometry;

    @JsonIgnore
    String type;

    @JsonIgnore
    String coordinates;

    @JsonProperty
    String license_name;

    @JsonProperty
    String license_url;

    @JsonProperty
    String license_use_without_attribution;

    @JsonProperty
    String license_create_derived_product;

    @JsonProperty
    String license_redistribute;

    @JsonProperty
    String license_attribution_text;

    @JsonProperty
    String last_fetched_at;

    @JsonProperty
    String last_imported_at;

    @JsonProperty
    String latest_fetch_exception_log;

    @JsonProperty
    String import_status;

    @JsonProperty
    String created_at;

    @JsonProperty
    String updated_at;

    @JsonProperty
    String feed_versions_count;

    @JsonProperty
    String feed_versions_url;

    @JsonProperty
    String[] feed_versions;

    @JsonProperty
    String active_feed_version;

    @JsonProperty
    String import_level_of_active_feed_version;

    @JsonProperty
    String created_or_updated_in_changeset_id;

    @JsonIgnore
    String changesets_imported_from_this_feed;

    @JsonIgnore
    String operators_in_feed;

    @JsonIgnore
    String gtfs_agency_id;

    @JsonIgnore
    String operator_onestop_id;

    @JsonIgnore
    String feed_onestop_id;

    @JsonIgnore
    String operator_url;

    @JsonIgnore
    String feed_url;

    public TransitLandFeed(JsonNode jsonMap){
        this.url = jsonMap.get("url").asText();
        this.onestop_id = jsonMap.get("onestop_id").asText();
        this.feed_format = jsonMap.get("feed_format").asText();
        this.tags = jsonMap.get("tags").asText();
        this.license_name = jsonMap.get("license_name").asText();
        this.license_url = jsonMap.get("license_url").asText();
        this.license_use_without_attribution = jsonMap.get("license_use_without_attribution").asText();
        this.license_create_derived_product = jsonMap.get("license_create_derived_product").asText();
        this.license_redistribute = jsonMap.get("license_redistribute").asText();
        this.license_attribution_text = jsonMap.get("license_attribution_text").asText();
        this.last_fetched_at = jsonMap.get("last_fetched_at").asText();
        this.last_imported_at = jsonMap.get("last_imported_at").asText();
//        this.latest_fetch_exception_log = jsonMap.retrieveById("latest_fetch_exception_log").asText();
        this.import_status = jsonMap.get("import_status").asText();
        this.created_at = jsonMap.get("created_at").asText();
        this.updated_at = jsonMap.get("updated_at").asText();
        this.feed_versions_count = jsonMap.get("feed_versions_count").asText();
        this.feed_versions_url = jsonMap.get("feed_versions_url").asText();
//            this.feed_versions = jsonMap.retrieveById("feed_versions").asText();
        this.active_feed_version = jsonMap.get("active_feed_version").asText();
        this.import_level_of_active_feed_version = jsonMap.get("import_level_of_active_feed_version").asText();
        this.created_or_updated_in_changeset_id = jsonMap.get("created_or_updated_in_changeset_id").asText();
        this.changesets_imported_from_this_feed = jsonMap.get("changesets_imported_from_this_feed").asText();
        this.operators_in_feed = jsonMap.get("operators_in_feed").asText();
//            this.gtfs_agency_id = jsonMap.retrieveById("gtfs_agency_id").asText();
//            this.operator_onestop_id = jsonMap.retrieveById("operator_onestop_id").asText();
//            this.feed_onestop_id = jsonMap.retrieveById("feed_onestop_id").asText();
//            this.operator_url = jsonMap.retrieveById("operator_url").asText();
//            this.feed_url = jsonMap.retrieveById("feed_url").asText();
    }
}