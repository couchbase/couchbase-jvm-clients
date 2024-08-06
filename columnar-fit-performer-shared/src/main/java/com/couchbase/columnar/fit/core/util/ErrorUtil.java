/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.columnar.fit.core.util;

import fit.columnar.ColumnarErrorType;
import fit.columnar.PlatformErrorType;

import javax.annotation.Nullable;


public class ErrorUtil {
  private ErrorUtil() {
    throw new AssertionError("not instantiable");
  }

  private static @Nullable fit.columnar.ColumnarErrorType convertColumnarError(Throwable exception) {
    String simpleName = exception.getClass().getSimpleName();
    return switch (simpleName) {
      case "QueryException" -> ColumnarErrorType.COLUMNAR_EXCEPTION_QUERY;
      case "InvalidCredentialException" -> ColumnarErrorType.COLUMNAR_EXCEPTION_INVALID_CREDENTIAL;
      case "TimeoutException" -> ColumnarErrorType.COLUMNAR_EXCEPTION_TIMEOUT;
      default -> null;
    };
  }

  private static fit.columnar.PlatformErrorType convertPlatformError(Throwable exception) {
    return (exception instanceof IllegalArgumentException)
      ? PlatformErrorType.PLATFORM_ERROR_INVALID_ARGUMENT
      : PlatformErrorType.PLATFORM_ERROR_OTHER;
  }

  public static fit.columnar.Error convertError(Throwable raw) {
    var ret = fit.columnar.Error.newBuilder();

    var type = ErrorUtil.convertColumnarError(raw);

    if (type != null) {
      var out = fit.columnar.ColumnarError.newBuilder()
              .setType(type)
              .setAsString(raw.toString());
      if (raw.getCause() != null) {
        out.setCause(convertError(raw.getCause()));
      }

      ret.setColumnar(out);
    } else {
      ret.setPlatform(fit.columnar.PlatformError.newBuilder()
                      .setType(convertPlatformError(raw))
              .setAsString(raw.toString()));
    }

    return ret.build();
  }
}
