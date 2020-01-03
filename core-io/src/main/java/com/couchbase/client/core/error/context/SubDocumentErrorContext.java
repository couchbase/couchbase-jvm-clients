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

package com.couchbase.client.core.error.context;

import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus;

import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

public class SubDocumentErrorContext extends ErrorContext {

  private final KeyValueErrorContext kvContext;
  private final int index;
  private final String path;
  private final SubDocumentOpResponseStatus status;

  public SubDocumentErrorContext(final KeyValueErrorContext kvContext, final int index, final String path,
                                 final SubDocumentOpResponseStatus status) {
    super(kvContext.responseStatus());
    this.kvContext = kvContext;
    this.index = index;
    this.path = path;
    this.status = status;
  }

  /**
   * Returns the index of the spec which triggered the error.
   */
  public int index() {
    return index;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    if (kvContext != null) {
      kvContext.injectExportableParams(input);
    }
    input.put("index", index);
    if (path != null) {
      input.put("path", redactUser(path));
    }
    if (status != null) {
      input.put("subdocStatus", status);
    }
  }

}
