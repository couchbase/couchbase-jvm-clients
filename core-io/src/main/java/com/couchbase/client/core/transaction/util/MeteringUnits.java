/*
 * Copyright (c) 2022 Couchbase, Inc.
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
package com.couchbase.client.core.transaction.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.UNITS_NOT_PRESENT;

/**
 * Tracks read and write units used.
 */
@Stability.Internal
public class MeteringUnits {
  public final @Nullable Integer readUnits;
  public final @Nullable Integer writeUnits;

  public MeteringUnits(@Nullable Integer readUnits, @Nullable Integer writeUnits) {
    this.readUnits = readUnits;
    this.writeUnits = writeUnits;
  }

  public static @Nullable MeteringUnits from(Throwable err) {
    if (err instanceof CouchbaseException) {
      CouchbaseException ce = (CouchbaseException) err;
      if (ce.context() != null && ce.context() instanceof KeyValueErrorContext) {
        KeyValueErrorContext kvec = (KeyValueErrorContext) ce.context();
        Map<String, Object> map = new HashMap<>();
        kvec.injectExportableParams(map);
        Integer readUnits = (Integer) map.get("readUnits");
        Integer writeUnits = (Integer) map.get("readUnits");
        if (readUnits != null || writeUnits != null) {
          return new MeteringUnits(readUnits, writeUnits);
        }
        return null;
      }
    }
    return null;
  }

  @Stability.Internal
  public static class MeteringUnitsBuilder {
    private int readUnits;
    private int writeUnits;

    public int readUnits() {
      return readUnits;
    }

    public int writeUnits() {
      return writeUnits;
    }

    public void add(@Nullable MemcacheProtocol.FlexibleExtras flexibleExtras) {
      if (flexibleExtras != null) {
        if (flexibleExtras.readUnits != UNITS_NOT_PRESENT) {
          readUnits += flexibleExtras.readUnits;
        }
        if (flexibleExtras.writeUnits != UNITS_NOT_PRESENT) {
          writeUnits += flexibleExtras.writeUnits;
        }
      }
    }

    public void add(Throwable err) {
      add(from(err));
    }

    public void add(@Nullable MeteringUnits units) {
      if (units != null) {
        if (units.readUnits != null) {
          readUnits += units.readUnits;
        }
        if (units.writeUnits != null) {
          writeUnits += units.writeUnits;
        }
      }
    }

    public MeteringUnits build() {
      return new MeteringUnits(readUnits == 0 ? null : readUnits, writeUnits == 0 ? null : writeUnits);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      if (readUnits > 0) {
        sb.append(" RUs=");
        sb.append(readUnits);
      }
      if (writeUnits > 0) {
        sb.append(" WUs=");
        sb.append(writeUnits);
      }
      return sb.toString();
    }
  }
}
