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

package com.couchbase.client.core.util;

import com.couchbase.client.core.error.CouchbaseException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Defines useful constants and methods regarding bytes.
 */
public class Bytes {

  /**
   * Holds an empty byte array, so we do not need to create one every time.
   */
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[]{};

  public static byte[] readAllBytes(InputStream is) {
    notNull(is, "input stream");
    try {
      ByteArrayOutputStream result = new ByteArrayOutputStream();
      byte[] buffer = new byte[4 * 1024];
      int len;
      while ((len = is.read(buffer)) != -1) {
        result.write(buffer, 0, len);
      }
      return result.toByteArray();
    } catch (IOException e) {
      throw new CouchbaseException("Failed to read bytes from stream.", e);
    }
  }
}

