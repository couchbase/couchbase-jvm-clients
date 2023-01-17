/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.google.protobuf.Any;
import com.couchbase.client.core.deps.com.google.protobuf.InvalidProtocolBufferException;
import com.couchbase.client.core.deps.com.google.rpc.PreconditionFailure;
import com.couchbase.client.core.deps.com.google.rpc.ResourceInfo;
import com.couchbase.client.core.deps.com.google.rpc.Status;
import com.couchbase.client.core.deps.io.grpc.StatusRuntimeException;
import com.couchbase.client.core.deps.io.grpc.protobuf.StatusProto;
import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.InternalServerFailureException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.error.context.ProtostellarErrorContext;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.retry.ProtostellarRequestBehaviour;
import com.couchbase.client.core.retry.RetryOrchestratorProtostellar;
import com.couchbase.client.core.retry.RetryReason;

import java.util.concurrent.ExecutionException;

@Stability.Internal
public class CoreProtostellarErrorHandlingUtil {
  private CoreProtostellarErrorHandlingUtil() {}

  private static final String PRECONDITION_CAS = "CAS";
  private static final String PRECONDITION_LOCKED = "LOCKED";
  private static final String TYPE_URL_PRECONDITION_FAILURE = "type.googleapis.com/google.rpc.PreconditionFailure";
  private static final String TYPE_URL_RESOURCE_INFO = "type.googleapis.com/google.rpc.ResourceInfo";

  public static ProtostellarRequestBehaviour convertKeyValueException(Core core,
                                                                      ProtostellarRequest<?> request,
                                                                      Throwable t) {
    // Handle wrapped CompletableFuture failures.
    if (t instanceof ExecutionException) {
      return convertKeyValueException(core, request, t.getCause());
    }

    if (request.timeoutElapsed()) {
      return request.cancelDueToTimeout();
    }

    ProtostellarErrorContext context = request.context();

    if (t instanceof StatusRuntimeException) {
      // https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto
      StatusRuntimeException sre = (StatusRuntimeException) t;

      Status status = StatusProto.fromThrowable(sre);
      context.put("server", status.getMessage());

      if (status.getDetailsCount() > 0) {
        Any details = status.getDetails(0);
        String typeUrl = details.getTypeUrl();

        // https://github.com/grpc/grpc-web/issues/399#issuecomment-443248907
        try {
          if (typeUrl.equals(TYPE_URL_PRECONDITION_FAILURE)) {
            PreconditionFailure info = PreconditionFailure.parseFrom(details.getValue());

            if (info.getViolationsCount() > 0) {
              PreconditionFailure.Violation violation = info.getViolations(0);
              String type = violation.getType();

              if (type.equals(PRECONDITION_CAS)) {
                return ProtostellarRequestBehaviour.fail(new CasMismatchException(context));
              }
              else if (type.equals(PRECONDITION_LOCKED)) {
                return RetryOrchestratorProtostellar.shouldRetry(core, request, RetryReason.KV_LOCKED);
              }
            }
          } else if (typeUrl.equals(TYPE_URL_RESOURCE_INFO)) {
            ResourceInfo info = ResourceInfo.parseFrom(details.getValue());

            context.put("resourceName", info.getResourceName());
            context.put("resourceType", info.getResourceType());
          }
        } catch (InvalidProtocolBufferException e) {
          return ProtostellarRequestBehaviour.fail(new DecodingFailureException("Failed to decode GRPC response", e));
        }
      }

      com.couchbase.client.core.deps.io.grpc.Status.Code code = sre.getStatus().getCode();

      switch (code) {
        case CANCELLED:
          return ProtostellarRequestBehaviour.fail(new RequestCanceledException("Request cancelled by server", CancellationReason.SERVER_CANCELLED, new CancellationErrorContext(context)));
        case ABORTED:
        case UNKNOWN:
        case INTERNAL:
          return ProtostellarRequestBehaviour.fail(new InternalServerFailureException(context));
        case OUT_OF_RANGE:
        case INVALID_ARGUMENT:
          return ProtostellarRequestBehaviour.fail(new InvalidArgumentException("Invalid argument provided", t, context));
        case DEADLINE_EXCEEDED:
          return ProtostellarRequestBehaviour.fail(new AmbiguousTimeoutException("The server reported the operation timeout, and state might have been changed", new CancellationErrorContext(context)));
        case NOT_FOUND:
          return ProtostellarRequestBehaviour.fail(new DocumentNotFoundException(context));
        case ALREADY_EXISTS:
          return ProtostellarRequestBehaviour.fail(new DocumentExistsException(context));
        case UNAUTHENTICATED:
        case PERMISSION_DENIED:
          return ProtostellarRequestBehaviour.fail(new AuthenticationFailureException("Server reported that permission to the resource was denied", context, t));
        case UNIMPLEMENTED:
          return ProtostellarRequestBehaviour.fail(new FeatureNotAvailableException(t));
        case UNAVAILABLE:
          return RetryOrchestratorProtostellar.shouldRetry(core, request, RetryReason.ENDPOINT_NOT_AVAILABLE);
        default:
          // There are several codes left to handle, to be fixed under JVMCBC-1188.
          return ProtostellarRequestBehaviour.fail(new UnsupportedOperationException("Unhandled error code " + code));
      }
    } else if (t instanceof RuntimeException) {
      return ProtostellarRequestBehaviour.fail((RuntimeException) t);
    } else {
      return ProtostellarRequestBehaviour.fail(new RuntimeException(t));
    }
  }
}
