package com.conveyal.datatools.editor.models.transit;


import com.conveyal.datatools.editor.models.Model;

import java.io.Serializable;

public class RouteType extends Model implements Serializable {
    public static final long serialVersionUID = 1;

    public String localizedVehicleType;
    public String description;

    public GtfsRouteType gtfsRouteType;
    
    public HvtRouteType hvtRouteType;
    
    /*
    @JsonCreator
    public static RouteType factory(long id) {
      return RouteType.findById(id);
    }

    @JsonCreator
    public static RouteType factory(String id) {
      return RouteType.findById(Long.parseLong(id));
    }
    */

    
}
