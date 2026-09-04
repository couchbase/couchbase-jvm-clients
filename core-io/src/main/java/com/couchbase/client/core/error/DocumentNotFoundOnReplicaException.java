/*
 * Copyright (c) 2026 Couchbase, Inc.
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
package com.couchbase.client.core.error;

import com.couchbase.client.core.error.context.ErrorContext;

/**
 * Indicates a replica read failed because the requested document does not (yet) exist on that
 * replica. Because replicas are eventually consistent, this does not necessarily mean the document
 * does not exist on the active node or other replicas.
 */
public class DocumentNotFoundOnReplicaException extends DocumentNotFoundException {

  public DocumentNotFoundOnReplicaException(final ErrorContext ctx) {
    super("Document with the given id not found on this replica. Note replicas are eventually " +
        "consistent, so the document may still exist on the active node or other replicas.", ctx);
  }

}
