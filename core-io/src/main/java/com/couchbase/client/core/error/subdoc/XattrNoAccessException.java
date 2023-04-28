/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.core.error.subdoc;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.context.ErrorContext;

/**
 * Subdocument exception thrown when an extended attribute cannot be accessed.
 * Usually indicates the attribute is a "system attribute" (name starts with underscore).
 */
public class XattrNoAccessException extends CouchbaseException {

  public XattrNoAccessException(final ErrorContext ctx) {
    super("The user does not have permission to access this attribute." +
        " If the attribute name starts with an underscore, it is considered a 'system' attribute" +
        " and the 'Data Reader / Writer' role is not sufficient;" +
        " to access a system attribute, the user must have a role that grants the 'SystemXattrRead' / `SystemXattrWrite' permission.", ctx);
  }
}
