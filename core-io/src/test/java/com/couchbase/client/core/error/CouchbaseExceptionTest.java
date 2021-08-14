/*
 * Copyright (c) 2021 Couchbase, Inc.
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
import com.couchbase.client.core.msg.ResponseStatus;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CouchbaseExceptionTest {

  @Test
  void includesErrorContextInMessage() {
    String message = "my message";
    ErrorContext ctx = new ErrorContext(ResponseStatus.SUCCESS) {
      @Override
      public void injectExportableParams(Map<String, Object> input) {
        input.put("foo", "bar");
      }
    };
    CouchbaseException ex = new CouchbaseException(message, ctx);
    assertEquals("my message {\"foo\":\"bar\"}", ex.getMessage());
    assertEquals("com.couchbase.client.core.error.CouchbaseException: my message {\"foo\":\"bar\"}", ex.toString());
  }

}