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

package com.couchbase.client.core.compression.snappy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

class SnappyHelper {
  private static final Logger log = LoggerFactory.getLogger(SnappyHelper.class);

  static final SnappyCodec INSTANCE = createSuitableInstance();

  private static SnappyCodec createSuitableInstance() {
    if (!compatibleByteOrder()) {
      log.info("Using slower Snappy compression codec because this is big-endian hardware.");
      return new SlowSnappyCodec();
    }

    if (!canAccessUnsafe()) {
      log.info("Using slower Snappy compression codec because `sun.misc.Unsafe` is absent from this JVM, or not accessible in this security context.");
      return new SlowSnappyCodec();
    }

    log.info("Using fast Snappy compression codec because this is little-endian hardware and `sun.misc.Unsafe` is accessible.");
    return new FastSnappyCodec();
  }

  private static boolean compatibleByteOrder() {
    return ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
  }

  private static boolean canAccessUnsafe() {
    try {
      Field theUnsafe = SnappyCodec.class.getClassLoader().loadClass("sun.misc.Unsafe").getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      Object ignored = theUnsafe.get(null);
      return true;

    } catch (Exception e) {
      return false;
    }
  }
}
