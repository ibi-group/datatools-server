package com.conveyal.datatools.editor.models.transit;

import com.conveyal.datatools.editor.models.Model;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Agency extends Model implements Cloneable, Serializable, Comparable {
    public static final long serialVersionUID = 1;
    public static final Logger LOG = LoggerFactory.getLogger(Agency.class);
    //public String gtfsAgencyId;
    public String name;
    public String url;
    public String timezone;
    public String lang;
    public String phone;
    public String email;
    public String feedId;
    public String agencyBrandingUrl;
    public String agencyFareUrl;

    /*public String color;

    public Double defaultLat;
    public Double defaultLon;
    
    public String routeTypeId;

    public String sourceId;*/
    /*
    @JsonCreator
    public static Agency factory(long id) {
      return Agency.findById(id);
    }

    @JsonCreator
    public static Agency factory(String id) {
      return Agency.findById(Long.parseLong(id));
    }
    */
    
    public Agency(com.conveyal.gtfs.model.Agency agency, EditorFeed feed) {
        this.id = agency.agency_id;
        this.name = agency.agency_name;
        this.url = agency.agency_url != null ? agency.agency_url.toString() : null;
        this.timezone = agency.agency_timezone;
        this.lang = agency.agency_lang;
        this.phone = agency.agency_phone;
        this.feedId = feed.id;
    }
    
    public Agency(EditorFeed feed, String gtfsAgencyId, String name, String url, String timezone, String lang, String phone) {
        this.name = name;
        this.url = url;
        this.timezone = timezone;
        this.lang = lang;
        this.phone = phone;
        this.feedId = feed.id;
    }
    
    public Agency () {}

    public com.conveyal.gtfs.model.Agency toGtfs() {
        com.conveyal.gtfs.model.Agency ret = new com.conveyal.gtfs.model.Agency();

        String gtfsAgencyId = id.toString();

        ret.agency_id = gtfsAgencyId;
        ret.agency_name = name;
        try {
            ret.agency_url = new URL(url);
        } catch (MalformedURLException e) {
            LOG.warn("Unable to coerce agency URL {} to URL", url);
            ret.agency_url = null;
        }
        try {
            ret.agency_branding_url = new URL(agencyBrandingUrl);
        } catch (MalformedURLException e) {
            LOG.warn("Unable to coerce agency branding URL {} to URL", agencyBrandingUrl);
            ret.agency_branding_url = null;
        }
        try {
            ret.agency_fare_url = new URL(agencyFareUrl);
        } catch (MalformedURLException e) {
            LOG.warn("Unable to coerce agency fare URL {} to URL", agencyFareUrl);
            ret.agency_fare_url = null;
        }
        ret.agency_timezone = timezone;
        ret.agency_lang = lang;
        ret.agency_phone = phone;
//        ret.agency_email = email;

        return ret;
    }

    public int compareTo (Object other) {
        if (!(other instanceof Agency))
            return -1;

        Agency o = (Agency) other;

        if (this.name == null)
            return -1;

        if (o.name == null)
            return 1;

        return this.name.compareTo(o.name);
    }

    public Agency clone () throws CloneNotSupportedException {
        return (Agency) super.clone();
    }
}
