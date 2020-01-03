/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty.view;

import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.context.ViewErrorContext;
import com.couchbase.client.core.io.netty.chunk.ChunkedMessageHandler;
import com.couchbase.client.core.msg.view.ViewChunkHeader;
import com.couchbase.client.core.msg.view.ViewChunkRow;
import com.couchbase.client.core.msg.view.ViewChunkTrailer;
import com.couchbase.client.core.msg.view.ViewError;
import com.couchbase.client.core.msg.view.ViewRequest;
import com.couchbase.client.core.msg.view.ViewResponse;
import com.couchbase.client.core.retry.RetryReason;

import java.util.Optional;

class ChunkedViewMessageHandler
        extends ChunkedMessageHandler<ViewChunkHeader, ViewChunkRow, ViewChunkTrailer, ViewResponse, ViewRequest> {

    ChunkedViewMessageHandler(BaseEndpoint endpoint, EndpointContext endpointContext) {
        super(endpoint, endpointContext, new ViewChunkResponseParser());
    }

    @Override
    protected Optional<RetryReason> qualifiesForRetry(CouchbaseException exception) {
        if (!(exception.context() instanceof ViewErrorContext)) {
            return Optional.empty();
        }
        return shouldRetry((ViewErrorContext) exception.context());
    }

    /**
     * Analyses status codes and checks if a retry needs to happen.
     *
     * Some status codes are ambiguous, so their contents are inspected further.
     *
     * @param context the error context with information.
     * @return true if retry is needed, false otherwise.
     */
    private static Optional<RetryReason> shouldRetry(final ViewErrorContext context) {
        switch (context.httpStatus()) {
            case 404:
                return analyse404Response(context.error());
            case 500:
                return analyse500Response(context.error());
            case 302:
                return Optional.of(RetryReason.VIEWS_NO_ACTIVE_PARTITION);
            case 300:
            case 301:
            case 303:
            case 307:
            case 401:
            case 408:
            case 409:
            case 412:
            case 416:
            case 417:
            case 501:
            case 502:
            case 503:
            case 504:
                return Optional.of(RetryReason.VIEWS_TEMPORARY_FAILURE);
            default:
                return Optional.empty();
        }
    }

    /**
     * Analyses the content of a 404 response to see if it is legible for retry.
     *
     * If the content contains ""reason":"missing"", it is a clear indication that the responding node
     * is unprovisioned and therefore it should be retried. All other cases indicate a provisioned node,
     * but the design document/view is not found, which should not be retried.
     *
     * @param error the parsed error content.
     * @return true if it needs to be retried, false otherwise.
     */
    private static Optional<RetryReason> analyse404Response(final ViewError error) {
        return error.reason().contains("missing") ? Optional.of(RetryReason.VIEWS_TEMPORARY_FAILURE) : Optional.empty();
    }

    /**
     * Analyses the content of a 500 response to see if it is legible for retry.
     *
     * @param error the parsed error content.
     * @return true if it needs to be retried, false otherwise.
     */
    private static Optional<RetryReason> analyse500Response(final ViewError error) {
        if (error.reason().contains("{not_found, missing_named_view}") || error.reason().contains("badarg")) {
            return Optional.empty();
        }
        return Optional.of(RetryReason.VIEWS_TEMPORARY_FAILURE);
    }
}

