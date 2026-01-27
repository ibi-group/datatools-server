package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.editor.utils.JacksonSerializers;
import com.conveyal.datatools.manager.gtfsplus.GtfsPlusValidation;
import com.conveyal.datatools.manager.gtfsplus.ValidationIssue;
import com.conveyal.gtfs.validator.ValidationResult;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.bson.Document;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Includes summary data (a subset of fields) for a feed version.
 */
public class FeedVersionSummary extends Model implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    public static Boolean hasBlockingIssueForPublishingForTesting = null;

    public FeedRetrievalMethod retrievalMethod;
    public int version;
    public String feedSourceId;
    public String name;
    public String namespace;
    public String originNamespace;
    public Long fileSize;
    public Date updated;
    /** Only a subset of the validation results are serialized to JSON via getValidationSummary. */
    @JsonIgnore
    public ValidationResult validationResult;
    private PartialValidationSummary validationSummary;
    public Date processedByExternalPublisher;
    public Date sentToExternalPublisher;
    public GtfsPlusValidation gtfsPlusValidation;
    public String feedSourcePublishedVersionId;

    public Integer publishedFeedVersionErrorCount;
    public LocalDate publishedFeedVersionStartDate;
    public LocalDate publishedFeedVersionEndDate;

    public PartialValidationSummary getValidationSummary() {
        if (validationSummary == null) {
            validationSummary = new PartialValidationSummary();
        }
        return validationSummary;
    }

    /** Empty constructor for serialization */
    public FeedVersionSummary() {
        // Do nothing
    }

    public FeedVersionSummary(
        String feedVersionKey,
        boolean hasChildValidationResultDocument,
        Document feedVersionDocument
    ) {
        id = feedVersionDocument.getString(feedVersionKey);
        processedByExternalPublisher = feedVersionDocument.getDate("processedByExternalPublisher");
        sentToExternalPublisher = feedVersionDocument.getDate("sentToExternalPublisher");
        gtfsPlusValidation = getGtfsPlusValidation(id, feedVersionDocument);
        namespace = feedVersionDocument.getString("namespace");
        validationResult = getValidationResult(hasChildValidationResultDocument, feedVersionDocument);

        // The feed source's published feed version. Feed source's publishedVersionId mapped to feed version's namespace.
        feedSourcePublishedVersionId = feedVersionDocument.getString("publishedVersionId");

        publishedFeedVersionErrorCount = feedVersionDocument.getInteger("publishedFeedVersionErrorCount");
        publishedFeedVersionStartDate = getDateFromString(feedVersionDocument.getString("publishedFeedVersionStartDate"));
        publishedFeedVersionEndDate = getDateFromString(feedVersionDocument.getString("publishedFeedVersionEndDate"));
    }

    /**
     * Holds a subset of fields from {@link:FeedValidationResultSummary} for UI use only.
     */
    public class PartialValidationSummary {
        /** Copied from FeedVersion */
        @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
        @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
        public LocalDate startDate;

        /** Copied from FeedVersion */
        @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
        @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
        public LocalDate endDate;

        PartialValidationSummary() {
            // Older feeds created in datatools may not have validationResult
            if (validationResult != null) {
                this.startDate = validationResult.firstCalendarDate;
                this.endDate = validationResult.lastCalendarDate;
            }
        }
    }

    /**
     * Build GtfsPlusValidation object from feed version document.
     */
    private static GtfsPlusValidation getGtfsPlusValidation(String feedVersionId, Document feedVersionDocument) {
        Document gtfsPlusValidationDocument = getDocumentChild(feedVersionDocument, "gtfsPlusValidation");
        if (gtfsPlusValidationDocument == null) {
            return null;
        }
        List<ValidationIssue> issues = null;
        if (gtfsPlusValidationDocument.get("issues") != null) {
            List<Document> issueDocs = gtfsPlusValidationDocument.getList("issues", Document.class);
            issues = issueDocs
                .stream()
                .map(doc -> mapper.convertValue(doc, ValidationIssue.class))
                .collect(Collectors.toList());
        }
        boolean published = Boolean.TRUE.equals(gtfsPlusValidationDocument.getBoolean("published"));
        return new GtfsPlusValidation(feedVersionId, published, issues);
    }

    /**
     * Build validation result from feed version document.
     */
    private static ValidationResult getValidationResult(boolean hasChildValidationResultDocument, Document feedVersionDocument) {
        ValidationResult validationResult = new ValidationResult();
        validationResult.errorCount = getValidationResultErrorCount(hasChildValidationResultDocument, feedVersionDocument);
        validationResult.firstCalendarDate = getValidationResultDate(hasChildValidationResultDocument, feedVersionDocument, "firstCalendarDate");
        validationResult.lastCalendarDate = getValidationResultDate(hasChildValidationResultDocument, feedVersionDocument, "lastCalendarDate");
        return validationResult;
    }

    /**
     * Convert String date (if not null) into LocalDate.
     */
    private static LocalDate getDateFromString(String date) {
        return (date == null) ? null : LocalDate.parse(date, formatter);
    }

    /**
     * Extract child document matching provided name.
     */
    private static Document getDocumentChild(Document document, String name) {
        return (Document) document.get(name);
    }

    /**
     * Extract date value from parent document or child validation result document.
     */
    private static LocalDate getValidationResultDate(
        boolean hasChildValidationResultDocument,
        Document feedVersionDocument,
        String key
    ) {
        return (hasChildValidationResultDocument)
            ? getDateFieldFromDocument(feedVersionDocument, key)
            : getDateFromString(feedVersionDocument.getString(key));
    }

    /**
     * Extract date value from validation result document.
     */
    private static LocalDate getDateFieldFromDocument(Document document, String dateKey) {
        Document validationResult = getDocumentChild(document, "validationResult");
        return (validationResult != null)
            ? getDateFromString(validationResult.getString(dateKey))
            : null;
    }

    /**
     * Extract the error count from the parent document or child validation result document. If the error count is not
     * available, return -1.
     */
    private static int getValidationResultErrorCount(boolean hasChildValidationResultDocument, Document feedVersionDocument) {
        int errorCount;
        try {
            errorCount = (hasChildValidationResultDocument)
                ? getErrorCount(feedVersionDocument)
                : feedVersionDocument.getInteger("errorCount");
        } catch (NullPointerException e) {
            errorCount = -1;
        }
        return errorCount;
    }

    /**
     * Get the child validation result document and extract the error count from this.
     */
    private static int getErrorCount(Document document) {
        return getDocumentChild(document, "validationResult").getInteger("errorCount");
    }

    /**
     * Determine the published state of the feed version.
     */
    public PublishState getPublishState() {
        if (isPublished()) {
            return PublishState.PUBLISHED;
        } else if (isPublishing()) {
            return PublishState.PUBLISHING;
        } else if (isPublishBlocked()) {
            return PublishState.PUBLISH_BLOCKED;
        }
        return PublishState.READY_TO_PUBLISH;
    }

    /**
     * Determine the published state of the feed version.
     */
    private boolean isPublished() {
        return namespace != null && namespace.equals(feedSourcePublishedVersionId);
    }

    /**
     * Deemed to be publishing if it has been sent to external publisher but not yet processed.
     */
    private boolean isPublishing() {
        return sentToExternalPublisher != null && processedByExternalPublisher == null;
    }

    /**
     * Determine if publishing is blocked due to validation, expiration, blocking issues or loading.
     */
    private boolean isPublishBlocked() {
        return
            gtfsPlusValidation == null ||
            gtfsPlusValidation.issues == null ||
            !gtfsPlusValidation.issues.isEmpty() ||
            !gtfsPlusValidation.published ||
            FeedVersion.hasExpired(validationResult) ||
            hasBlockingIssuesForPublishing();
    }

    /**
     * Determine if there are blocking issues for publishing.
     */
    private boolean hasBlockingIssuesForPublishing() {
        return Objects.requireNonNullElseGet(hasBlockingIssueForPublishingForTesting, () -> FeedVersion.hasBlockingIssuesForPublishing(
            validationResult,
            namespace,
            name
        ));
    }

    public static void setHasBlockingIssueForPublishingOverrideForTesting(Boolean value) {
        hasBlockingIssueForPublishingForTesting = value;
    }
}
