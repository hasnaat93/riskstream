package com.riskstream.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.List;

/** The one error shape every endpoint returns. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApiError(
        Instant timestamp,
        int status,
        String errorCode,
        String message,
        String path,
        List<FieldViolation> validationErrors) {

    public record FieldViolation(String field, String message) {
    }
}
