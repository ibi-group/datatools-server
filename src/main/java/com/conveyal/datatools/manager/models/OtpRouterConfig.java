package com.conveyal.datatools.manager.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Collection;

/**
 * Created by demory on 3/8/15.
 *
 * TODO: Figure out how to remove this class without causing problems in the DB migration
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OtpRouterConfig implements Serializable {
    private static final long serialVersionUID = 1L;

}
