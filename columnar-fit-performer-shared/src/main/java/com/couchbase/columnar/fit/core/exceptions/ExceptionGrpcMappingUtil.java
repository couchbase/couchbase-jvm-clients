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

package com.couchbase.columnar.fit.core.exceptions;

import io.grpc.Status;

import javax.annotation.Nullable;

public class ExceptionGrpcMappingUtil {
  private ExceptionGrpcMappingUtil() {}

  public static Throwable map(Throwable input) {
    Status.Code code = Status.Code.ABORTED;

    if (input instanceof GrpcUnimplemented) {
      code = Status.Code.UNIMPLEMENTED;
    }

    return Status.fromCode(code).withDescription(input.toString()).asException();
  }

  public static Throwable unimplemented(@Nullable String message) {
    return Status.fromCode(Status.Code.UNIMPLEMENTED).withDescription(message).asException();
  }
}
