package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.editor.utils.JacksonSerializers;
import com.conveyal.gtfs.validator.ValidationResult;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.bson.Document;

import javax.print.Doc;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class FeedVersionDeployed {
    public String id;

    @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
    @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
    public LocalDate startDate;

    @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
    @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
    public LocalDate endDate;

    public FeedVersionDeployed() {
    }

    public FeedVersionDeployed(Document feedVersionDocument) {
        this.id = feedVersionDocument.getString("_id");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        Document validationResult = (Document) feedVersionDocument.get("validationResult");
        String first = validationResult.getString("firstCalendarDate");
        String last = validationResult.getString("lastCalendarDate");
        this.startDate = (first == null) ? null : LocalDate.parse(first, formatter);
        this.endDate = (last == null) ? null : LocalDate.parse(last, formatter);
    }

    public FeedVersionDeployed(FeedVersion feedVersion) {
        this.id = feedVersion.id;
        this.startDate = feedVersion.validationSummary().startDate;
        this.endDate = feedVersion.validationSummary().endDate;
    }
}
