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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus;
import reactor.util.annotation.Nullable;

import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

@Stability.Uncommitted
public class SubDocumentErrorContext extends ErrorContext {

  private final KeyValueErrorContext kvContext;
  private final int index;
  private final String path;
  private final SubDocumentOpResponseStatus status;
  private final AbstractContext context;

  public SubDocumentErrorContext(final KeyValueErrorContext kvContext, final int index, final String path,
                                 final SubDocumentOpResponseStatus status, @Nullable final AbstractContext context) {
    super(kvContext == null ? null : kvContext.responseStatus());
    this.kvContext = kvContext;
    this.index = index;
    this.path = path;
    this.status = status;
    this.context = context;
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
    if (context != null) {
      context.injectExportableParams(input);
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
