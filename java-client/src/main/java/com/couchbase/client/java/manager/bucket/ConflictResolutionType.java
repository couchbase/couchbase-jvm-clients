/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.manager.bucket;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type of conflict resolution to configure for the bucket.
 * <p>
 * A conflict is caused when the source and target copies of an XDCR-replicated document are updated independently
 * of and dissimilarly to one another, each by a local application. The conflict must be resolved, by determining
 * which of the variants should prevail; and then correspondingly saving both documents in identical form. XDCR
 * provides an automated conflict resolution process.
 */
@Stability.Volatile
public enum ConflictResolutionType {

  /**
   * Conflict resolution based on a timestamp.
   * <p>
   * Timestamp-based conflict resolution (often referred to as Last Write Wins, or LWW) uses the document
   * timestamp (stored in the CAS) to resolve conflicts. The timestamps associated with the most recent
   * updates of source and target documents are compared. The document whose update has the more recent
   * timestamp prevails.
   */
  TIMESTAMP("lww"),

  /**
   * Conflict resolution based on the "Sequence Number".
   * <p>
   * Conflicts can be resolved by referring to documents' sequence numbers. Sequence numbers are maintained
   * per document, and are incremented on every document-update. The sequence numbers of source and
   * target documents are compared; and the document with the higher sequence number prevails.
   */
  SEQUENCE_NUMBER("seqno"),

  /**
   * Custom bucket conflict resolution.
   * <p>
   * In Couchbase Server 7.1, this feature is only available in "developer-preview" mode. See the UI XDCR settings
   * for the custom conflict resolution properties.
   */
  @Stability.Volatile
  CUSTOM("custom");

  private final String alias;

  ConflictResolutionType(final String alias) {
    this.alias = alias;
  }

  public String alias() {
    return alias;
  }

}
