/*
 * Copyright (c) 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.performer.core.util;

import com.couchbase.client.protocol.shared.CouchbaseExceptionType;

import javax.annotation.Nullable;

public class ErrorUtil {
    private ErrorUtil() {}

    public static @Nullable CouchbaseExceptionType convertException(Throwable err) {
        // We do string comparisons to avoid core-fit-performer needing to depend on core-io, which introduced a world
        // of Maven versioning hurt when trying to build the performers against specific SDK versions.
        var simpleName = err.getClass().getSimpleName();

        // Make sure to return most specific error
        if (simpleName.equals("AmbiguousTimeoutException")) return CouchbaseExceptionType.SDK_AMBIGUOUS_TIMEOUT_EXCEPTION;
        if (simpleName.equals("UnambiguousTimeoutException")) return CouchbaseExceptionType.SDK_UNAMBIGUOUS_TIMEOUT_EXCEPTION;
        if (simpleName.equals("TimeoutException")) return CouchbaseExceptionType.SDK_TIMEOUT_EXCEPTION;

        if (simpleName.equals("RequestCanceledException")) return CouchbaseExceptionType.SDK_REQUEST_CANCELLED_EXCEPTION;
        if (simpleName.equals("InvalidArgumentException")) return CouchbaseExceptionType.SDK_INVALID_ARGUMENT_EXCEPTION;
        if (simpleName.equals("ServiceNotAvailableException")) return CouchbaseExceptionType.SDK_SERVICE_NOT_AVAILABLE_EXCEPTION;
        if (simpleName.equals("InternalServerFailureException")) return CouchbaseExceptionType.SDK_INTERNAL_SERVER_FAILURE_EXCEPTION;
        if (simpleName.equals("AuthenticationFailureException")) return CouchbaseExceptionType.SDK_AUTHENTICATION_FAILURE_EXCEPTION;
        if (simpleName.equals("TemporaryFailureException")) return CouchbaseExceptionType.SDK_TEMPORARY_FAILURE_EXCEPTION;
        if (simpleName.equals("ParsingFailureException")) return CouchbaseExceptionType.SDK_PARSING_FAILURE_EXCEPTION;
        if (simpleName.equals("CasMismatchException")) return CouchbaseExceptionType.SDK_CAS_MISMATCH_EXCEPTION;
        if (simpleName.equals("BucketNotFoundException")) return CouchbaseExceptionType.SDK_BUCKET_NOT_FOUND_EXCEPTION;
        if (simpleName.equals("CollectionNotFoundException")) return CouchbaseExceptionType.SDK_COLLECTION_NOT_FOUND_EXCEPTION;
        if (simpleName.equals("FeatureNotAvailableException")) return CouchbaseExceptionType.SDK_FEATURE_NOT_AVAILABLE_EXCEPTION;
        if (simpleName.equals("ScopeNotFoundException")) return CouchbaseExceptionType.SDK_SCOPE_NOT_FOUND_EXCEPTION;
        if (simpleName.equals("IndexNotFoundException")) return CouchbaseExceptionType.SDK_INDEX_NOT_FOUND_EXCEPTION;
        if (simpleName.equals("IndexExistsException")) return CouchbaseExceptionType.SDK_INDEX_EXISTS_EXCEPTION;
        if (simpleName.equals("EncodingFailureException")) return CouchbaseExceptionType.SDK_ENCODING_FAILURE_EXCEPTION;
        if (simpleName.equals("DecodingFailureException")) return CouchbaseExceptionType.SDK_DECODING_FAILURE_EXCEPTION;

        if (simpleName.equals("DocumentNotFoundException")) return CouchbaseExceptionType.SDK_DOCUMENT_NOT_FOUND_EXCEPTION;
        if (simpleName.equals("DocumentUnretrievableException")) return CouchbaseExceptionType.SDK_DOCUMENT_UNRETRIEVABLE_EXCEPTION;
        if (simpleName.equals("DocumentLockedException")) return CouchbaseExceptionType.SDK_DOCUMENT_LOCKED_EXCEPTION;
        if (simpleName.equals("ValueTooLargeException")) return CouchbaseExceptionType.SDK_VALUE_TOO_LARGE_EXCEPTION;
        if (simpleName.equals("DocumentExistsException")) return CouchbaseExceptionType.SDK_DOCUMENT_EXISTS_EXCEPTION;
        if (simpleName.equals("DurabilityLevelNotAvailableException")) return CouchbaseExceptionType.SDK_DURABILITY_LEVEL_NOT_AVAILABLE_EXCEPTION;
        if (simpleName.equals("DurabilityImpossibleException")) return CouchbaseExceptionType.SDK_DURABILITY_IMPOSSIBLE_EXCEPTION;
        if (simpleName.equals("DurableWriteInProgressException")) return CouchbaseExceptionType.SDK_DURABLE_WRITE_IN_PROGRESS_EXCEPTION;
        if (simpleName.equals("DurableWriteReCommitInProgressException")) return CouchbaseExceptionType.SDK_DURABLE_WRITE_RECOMMIT_IN_PROGRESS_EXCEPTION;
        if (simpleName.equals("PathNotFoundException")) return CouchbaseExceptionType.SDK_PATH_NOT_FOUND_EXCEPTION;
        if (simpleName.equals("PathMismatchException")) return CouchbaseExceptionType.SDK_PATH_MISMATCH_EXCEPTION;
        if (simpleName.equals("PathInvalidException")) return CouchbaseExceptionType.SDK_PATH_INVALID_EXCEPTION;
        // Java does not have PathTooBigException, gets mapped to PathTooDeepException instead
        if (simpleName.equals("PathTooDeepException")) return CouchbaseExceptionType.SDK_PATH_TOO_DEEP_EXCEPTION;
        if (simpleName.equals("ValueTooDeepException")) return CouchbaseExceptionType.SDK_VALUE_TOO_DEEP_EXCEPTION;
        if (simpleName.equals("ValueInvalidException")) return CouchbaseExceptionType.SDK_VALUE_INVALID_EXCEPTION;
        if (simpleName.equals("DocumentNotJsonException")) return CouchbaseExceptionType.SDK_DOCUMENT_NOT_JSON_EXCEPTION;
        if (simpleName.equals("NumberTooBigException")) return CouchbaseExceptionType.SDK_NUMBER_TOO_BIG_EXCEPTION;
        if (simpleName.equals("DeltaInvalidException")) return CouchbaseExceptionType.SDK_DELTA_INVALID_EXCEPTION;
        if (simpleName.equals("PathExistsException")) return CouchbaseExceptionType.SDK_PATH_EXISTS_EXCEPTION;
        if (simpleName.equals("XattrUnknownMacroException")) return CouchbaseExceptionType.SDK_XATTR_UNKNOWN_MACRO_EXCEPTION;
        if (simpleName.equals("XattrInvalidKeyComboException")) return CouchbaseExceptionType.SDK_XATTR_INVALID_KEY_COMBO_EXCEPTION;
        if (simpleName.equals("XattrUnknownVirtualAttributeException")) return CouchbaseExceptionType.SDK_XATTR_UNKNOWN_VIRTUAL_ATTRIBUTE_EXCEPTION;
        if (simpleName.equals("XattrCannotModifyVirtualAttributeException")) return CouchbaseExceptionType.SDK_XATTR_CANNOT_MODIFY_VIRTUAL_ATTRIBUTE_EXCEPTION;

        if (simpleName.equals("PlanningFailureException")) return CouchbaseExceptionType.SDK_PLANNING_FAILURE_EXCEPTION;
        if (simpleName.equals("IndexFailureException")) return CouchbaseExceptionType.SDK_INDEX_FAILURE_EXCEPTION;
        if (simpleName.equals("PreparedStatementFailureException")) return CouchbaseExceptionType.SDK_PREPARED_STATEMENT_FAILURE_EXCEPTION;

        if (simpleName.equals("CompilationFailureException")) return CouchbaseExceptionType.SDK_COMPILATION_FAILURE_EXCEPTION;
        if (simpleName.equals("JobQueueFullException")) return CouchbaseExceptionType.SDK_JOB_QUEUE_FULL_EXCEPTION;
        if (simpleName.equals("DatasetNotFoundException")) return CouchbaseExceptionType.SDK_DATASET_NOT_FOUND_EXCEPTION;
        if (simpleName.equals("DataverseNotFoundException")) return CouchbaseExceptionType.SDK_DATAVERSE_NOT_FOUND_EXCEPTION;
        if (simpleName.equals("DatasetExistsException")) return CouchbaseExceptionType.SDK_DATASET_EXISTS_EXCEPTION;
        if (simpleName.equals("DataverseExistsException")) return CouchbaseExceptionType.SDK_DATAVERSE_EXISTS_EXCEPTION;
        if (simpleName.equals("LinkNotFoundException")) return CouchbaseExceptionType.SDK_LINK_NOT_FOUND_EXCEPTION;

        if (simpleName.equals("ViewNotFoundException")) return CouchbaseExceptionType.SDK_VIEW_NOT_FOUND_EXCEPTION;
        if (simpleName.equals("DesignDocumentNotFoundException")) return CouchbaseExceptionType.SDK_DESIGN_DOCUMENT_NOT_FOUND_EXCEPTION;

        if (simpleName.equals("CollectionExistsException")) return CouchbaseExceptionType.SDK_COLLECTION_EXISTS_EXCEPTION;
        if (simpleName.equals("ScopeExistsException")) return CouchbaseExceptionType.SDK_SCOPE_EXISTS_EXCEPTION;
        if (simpleName.equals("UserNotFoundException")) return CouchbaseExceptionType.SDK_USER_NOT_FOUND_EXCEPTION;
        if (simpleName.equals("GroupNotFoundException")) return CouchbaseExceptionType.SDK_GROUP_NOT_FOUND_EXCEPTION;
        if (simpleName.equals("BucketExistsException")) return CouchbaseExceptionType.SDK_BUCKET_EXISTS_EXCEPTION;
        // Java does not have UserExistsException
        if (simpleName.equals("BucketNotFlushableException")) return CouchbaseExceptionType.SDK_BUCKET_NOT_FLUSHABLE_EXCEPTION;

        if (err.getClass().getSimpleName().equals("RateLimitedException")) return CouchbaseExceptionType.SDK_RATE_LIMITED_EXCEPTION;
        if (err.getClass().getSimpleName().equals("QuotaLimitedException")) return CouchbaseExceptionType.SDK_QUOTA_LIMITED_EXCEPTION;
        if (err.getClass().getSimpleName().equals("DmlFailureException")) return CouchbaseExceptionType.SDK_DML_FAILURE_EXCEPTION;
        if (err.getClass().getSimpleName().equals("XattrNoAccessException")) return CouchbaseExceptionType.SDK_XATTR_NO_ACCESS_EXCEPTION;

        return null;
    }
}
