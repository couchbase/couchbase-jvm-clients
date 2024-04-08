/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.columnar.client.java.internal;

// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.columnar.client.java.codec.JacksonDeserializer;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * Separate class so the SDK doesn't explode if the user
 * excludes Jackson from the class path.
 */
@Stability.Internal
public class DefaultJacksonDeserializerHolder {
  public static final JacksonDeserializer DESERIALIZER = new JacksonDeserializer(
    JsonMapper.builder().build()
  );
}
