package com.conveyal.datatools.editor.models.transit;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by landon on 6/22/16.
 */
public class Fare {
    public static final long serialVersionUID = 1;

    public String feedId;
    public String gtfsFareId;
    public String description;
    public Double price;
    public String currencyType;
    public Integer paymentMethod;
    public Integer transfers;
    public Integer transferDuration;
    public List fareRules  = Lists.newArrayList();

    public Fare() {};

    public Fare(com.conveyal.gtfs.model.FareAttribute fare, List<com.conveyal.gtfs.model.FareRule> rules) {
        this.gtfsFareId = fare.fare_id;
        this.price = fare.price;
        this.currencyType = fare.currency_type;
        this.paymentMethod = fare.payment_method;
        this.transfers = fare.transfers;
        this.transferDuration = fare.transfer_duration;
        this.fareRules.addAll(rules);
        inferName();
    }

    /**
     * Infer the name of this calendar
     */
    public void inferName () {
        StringBuilder sb = new StringBuilder(14);

        if (price != null)
            sb.append(price);
        if (currencyType != null)
            sb.append(currencyType);

        this.description = sb.toString();

        if (this.description.equals("") && this.gtfsFareId != null)
            this.description = gtfsFareId;
    }
}
