/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DefaultErrorUtil;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.error.context.SubDocumentErrorContext;
import com.couchbase.client.core.error.subdoc.DocumentAlreadyAliveException;
import com.couchbase.client.core.error.subdoc.DocumentNotJsonException;
import com.couchbase.client.core.error.subdoc.DocumentTooDeepException;
import com.couchbase.client.core.error.subdoc.XattrInvalidKeyComboException;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import reactor.util.annotation.Nullable;

/**
 * In sub-doc the lookupIn and mutateIn paths are very similar on the server, so DRY
 * shared logic between them.
 */
public class SubdocUtil {
  /**
   * These are errors that will result in the mutateIn or lookupIn call failing with an exception.
   */
  public static CouchbaseException handleNonFieldLevelErrors(BaseKeyValueRequest<?> request,
                                                             short status,
                                                             @Nullable MemcacheProtocol.FlexibleExtras flexibleExtras,
                                                             @Nullable String serverError) {

    // See comment in SubdocGetRequest.decode() for why this logic.
    boolean isFailure = !(status == MemcacheProtocol.Status.SUCCESS.status()
      || status == MemcacheProtocol.Status.SUBDOC_SUCCESS_DELETED_DOCUMENT.status()
      || status == MemcacheProtocol.Status.SUBDOC_MULTI_PATH_FAILURE.status()
      || status == MemcacheProtocol.Status.SUBDOC_MULTI_PATH_FAILURE_DELETED.status());

    ResponseStatus decodedStatus = MemcacheProtocol.decodeStatus(status);

    if (status == MemcacheProtocol.Status.SUBDOC_DOC_NOT_JSON.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(request, SubDocumentOpResponseStatus.DOC_NOT_JSON, flexibleExtras, serverError);
      return new DocumentNotJsonException(e);
    } else if (status == MemcacheProtocol.Status.SUBDOC_DOC_TOO_DEEP.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(request, SubDocumentOpResponseStatus.DOC_TOO_DEEP, flexibleExtras, serverError);
      return new DocumentTooDeepException(e);
    } else if (status == MemcacheProtocol.Status.SUBDOC_XATTR_INVALID_KEY_COMBO.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(request, SubDocumentOpResponseStatus.XATTR_INVALID_KEY_COMBO, flexibleExtras, serverError);
      return new XattrInvalidKeyComboException(e);
    } else if (status == MemcacheProtocol.Status.SUBDOC_CAN_ONLY_REVIVE_DELETED_DOCUMENTS.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(request, SubDocumentOpResponseStatus.CAN_ONLY_REVIVE_DELETED_DOCUMENTS, flexibleExtras, serverError);
      return new DocumentAlreadyAliveException(e);
    } else if (status == MemcacheProtocol.Status.SUBDOC_INVALID_COMBO.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(request, SubDocumentOpResponseStatus.INVALID_COMBO, flexibleExtras, serverError);
      String msg = "Sub-document failed with error code INVALID_COMBO," +
        " which probably means too many sub-document operations in a single request.";
      return new InvalidArgumentException(msg, null, e);
    } else if (status == MemcacheProtocol.Status.INVALID_REQUEST.status()) {
      KeyValueErrorContext ec = new KeyValueErrorContext(request, decodedStatus, null);
      return new InvalidArgumentException("Server error is: " + serverError, null, ec);
    } else if (isFailure) {
      KeyValueErrorContext ec = new KeyValueErrorContext(request, decodedStatus, null);
      String errorMessage = serverError != null ? ("Server error is: " + serverError) : null;
      return DefaultErrorUtil.keyValueStatusToException(request, decodedStatus, ec, errorMessage);
    }

    return null;
  }

  private static SubDocumentErrorContext createSubDocumentExceptionContext(BaseKeyValueRequest<?> request,
                                                                           SubDocumentOpResponseStatus status,
                                                                           @Nullable MemcacheProtocol.FlexibleExtras flexibleExtras,
                                                                           @Nullable String serverError) {
    return new SubDocumentErrorContext(
      KeyValueErrorContext.completedRequest(request, ResponseStatus.SUBDOC_FAILURE, flexibleExtras),
      0,
      null,
      status,
      null,
      serverError
    );
  }

}
