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

import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.google.protobuf.Any;
import com.couchbase.client.core.deps.com.google.protobuf.InvalidProtocolBufferException;
import com.couchbase.client.core.deps.com.google.rpc.PreconditionFailure;
import com.couchbase.client.core.deps.com.google.rpc.ResourceInfo;
import com.couchbase.client.core.deps.io.grpc.StatusRuntimeException;
import com.couchbase.client.core.deps.io.grpc.protobuf.StatusProto;
import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.InternalServerFailureException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.ScopeExistsException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.ValueTooLargeException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.error.context.GenericErrorContext;
import com.couchbase.client.core.error.subdoc.DocumentNotJsonException;
import com.couchbase.client.core.error.subdoc.DocumentTooDeepException;
import com.couchbase.client.core.error.subdoc.PathExistsException;
import com.couchbase.client.core.error.subdoc.PathMismatchException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.error.subdoc.ValueInvalidException;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.retry.ProtostellarRequestBehaviour;
import com.couchbase.client.core.retry.RetryOrchestratorProtostellar;
import com.couchbase.client.core.retry.RetryReason;
import reactor.util.annotation.Nullable;

import java.util.concurrent.ExecutionException;

import static com.couchbase.client.core.deps.io.grpc.Status.Code;

@Stability.Internal
public class CoreProtostellarErrorHandlingUtil {
  private CoreProtostellarErrorHandlingUtil() {
  }

  private static final String PRECONDITION_CAS = "CAS";
  private static final String PRECONDITION_LOCKED = "LOCKED";
  private static final String PRECONDITION_PATH_MISMATCH = "PATH_MISMATCH";
  private static final String PRECONDITION_DOC_NOT_JSON = "DOC_NOT_JSON";
  private static final String PRECONDITION_DOC_TOO_DEEP = "DOC_TOO_DEEP";
  private static final String PRECONDITION_WOULD_INVALIDATE_JSON = "WOULD_INVALIDATE_JSON";
  // Not currently used as it's unclear what exception this maps to.
  private static final String PRECONDITION_PATH_VALUE_OUT_OF_RANGE = "PATH_VALUE_OUT_OF_RANGE";
  private static final String PRECONDITION_VALUE_TOO_LARGE = "VALUE_TOO_LARGE";
  private static final String TYPE_URL_PRECONDITION_FAILURE = "type.googleapis.com/google.rpc.PreconditionFailure";
  private static final String TYPE_URL_RESOURCE_INFO = "type.googleapis.com/google.rpc.ResourceInfo";

  private static final String RESOURCE_TYPE_DOCUMENT = "document";
  private static final String RESOURCE_TYPE_SEARCH_INDEX = "searchindex";
  private static final String RESOURCE_TYPE_QUERY_INDEX = "queryindex";
  private static final String RESOURCE_TYPE_ANALYTICS_INDEX = "analyticsindex";
  private static final String RESOURCE_TYPE_BUCKET = "bucket";
  private static final String RESOURCE_TYPE_SCOPE = "scope";
  private static final String RESOURCE_TYPE_COLLECTION = "collection";
  private static final String RESOURCE_TYPE_PATH = "path";

  public static ProtostellarRequestBehaviour convertException(CoreProtostellar core,
                                                              ProtostellarRequest<?> request,
                                                              Throwable t) {
    // Handle wrapped CompletableFuture failures.
    if (t instanceof ExecutionException) {
      return convertException(core, request, t.getCause());
    }

    if (t instanceof StatusRuntimeException) {
      // https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto
      StatusRuntimeException sre = (StatusRuntimeException) t;
      com.couchbase.client.core.deps.com.google.rpc.Status status = StatusProto.fromThrowable(sre);
      return convertStatus(core, request, sre, status);
    } else if (t instanceof RuntimeException) {
      return ProtostellarRequestBehaviour.fail((RuntimeException) t);
    } else {
      return ProtostellarRequestBehaviour.fail(new RuntimeException(t));
    }
  }


  public static ProtostellarRequestBehaviour convertStatus(CoreProtostellar core,
                                                           ProtostellarRequest<?> request,
                                                           @Nullable StatusRuntimeException t,
                                                           com.couchbase.client.core.deps.com.google.rpc.Status status) {
    GenericErrorContext context = request.context();

    com.couchbase.client.core.deps.io.grpc.Status.Code code = Code.UNKNOWN;

    if (t != null) {
      com.couchbase.client.core.deps.io.grpc.Status stat = t.getStatus();
      code = stat.getCode();
    } else {
      if (status.getCode() < Code.values().length) {
        code = Code.values()[status.getCode()];
      }
    }

    context.put("server", status.getMessage());
    context.put("details", status.getDetailsCount());

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
            } else if (type.equals(PRECONDITION_LOCKED)) {
              return RetryOrchestratorProtostellar.shouldRetry(core, request, RetryReason.KV_LOCKED);
            } else if (type.equals(PRECONDITION_PATH_MISMATCH)) {
              return ProtostellarRequestBehaviour.fail(new PathMismatchException(context));
            } else if (type.equals(PRECONDITION_DOC_NOT_JSON)) {
              return ProtostellarRequestBehaviour.fail(new DocumentNotJsonException(context));
            } else if (type.equals(PRECONDITION_DOC_TOO_DEEP)) {
              return ProtostellarRequestBehaviour.fail(new DocumentTooDeepException(context));
            } else if (type.equals(PRECONDITION_WOULD_INVALIDATE_JSON)) {
              return ProtostellarRequestBehaviour.fail(new ValueInvalidException(context));
            } else if (type.equals(PRECONDITION_VALUE_TOO_LARGE)) {
              return ProtostellarRequestBehaviour.fail(new ValueTooLargeException(context));
            }
          }
        } else if (typeUrl.equals(TYPE_URL_RESOURCE_INFO)) {
          ResourceInfo info = ResourceInfo.parseFrom(details.getValue());

          String resourceName = info.getResourceName();
          String resourceType = info.getResourceType();

          if (resourceName != null) {
            context.put("resourceName", info.getResourceName());
          }
          context.put("resourceType", info.getResourceType());

          if (code == Code.NOT_FOUND) {
            if (resourceType.equals(RESOURCE_TYPE_DOCUMENT)) {
              return ProtostellarRequestBehaviour.fail(new DocumentNotFoundException(context));
            } else if (resourceType.equals(RESOURCE_TYPE_QUERY_INDEX)
              || resourceType.equals(RESOURCE_TYPE_SEARCH_INDEX)
              || resourceType.equals(RESOURCE_TYPE_ANALYTICS_INDEX)) {
              return ProtostellarRequestBehaviour.fail(IndexNotFoundException.withMessageAndErrorContext(status.getMessage(), context));
            } else if (resourceType.equals(RESOURCE_TYPE_BUCKET)) {
              return ProtostellarRequestBehaviour.fail(new BucketNotFoundException(info.getResourceName(), context));
            } else if (resourceType.equals(RESOURCE_TYPE_SCOPE)) {
              return ProtostellarRequestBehaviour.fail(new ScopeNotFoundException(info.getResourceName(), context));
            } else if (resourceType.equals(RESOURCE_TYPE_COLLECTION)) {
              return ProtostellarRequestBehaviour.fail(new CollectionNotFoundException(info.getResourceName(), context));
            } else if (resourceType.equals(RESOURCE_TYPE_PATH)) {
              return ProtostellarRequestBehaviour.fail(new PathNotFoundException(null));
            }
          } else if (code == Code.ALREADY_EXISTS) {
            if (resourceType.equals(RESOURCE_TYPE_DOCUMENT)) {
              return ProtostellarRequestBehaviour.fail(new DocumentExistsException(context));
            } else if (resourceType.equals(RESOURCE_TYPE_QUERY_INDEX)
              || resourceType.equals(RESOURCE_TYPE_SEARCH_INDEX)
              || resourceType.equals(RESOURCE_TYPE_ANALYTICS_INDEX)) {
              return ProtostellarRequestBehaviour.fail(new IndexExistsException(status.getMessage(), context));
            } else if (resourceType.equals(RESOURCE_TYPE_BUCKET)) {
              return ProtostellarRequestBehaviour.fail(new BucketExistsException(info.getResourceName(), context));
            } else if (resourceType.equals(RESOURCE_TYPE_SCOPE)) {
              return ProtostellarRequestBehaviour.fail(new ScopeExistsException(info.getResourceName(), context));
            } else if (resourceType.equals(RESOURCE_TYPE_COLLECTION)) {
              return ProtostellarRequestBehaviour.fail(new CollectionExistsException(info.getResourceName(), context));
            } else if (resourceType.equals(RESOURCE_TYPE_PATH)) {
              return ProtostellarRequestBehaviour.fail(new PathExistsException(context));
            }
          }
          // If the code or resourceType are not understood, will intentionally fallback to a CouchbaseException.
        }
      } catch (InvalidProtocolBufferException e) {
        return ProtostellarRequestBehaviour.fail(new DecodingFailureException("Failed to decode GRPC response", e));
      }
    }

    switch (code) {
      case CANCELLED:
        return ProtostellarRequestBehaviour.fail(new RequestCanceledException("Request cancelled by server", CancellationReason.SERVER_CANCELLED, t, new CancellationErrorContext(context)));
      case ABORTED:
      case UNKNOWN:
      case INTERNAL:
        return ProtostellarRequestBehaviour.fail(new InternalServerFailureException(t, context));
      case OUT_OF_RANGE:
      case INVALID_ARGUMENT:
        return ProtostellarRequestBehaviour.fail(new InvalidArgumentException("Invalid argument provided", t, context));
      case DEADLINE_EXCEEDED:
        CancellationErrorContext ec = new CancellationErrorContext(context);
        TimeoutException e;
        if (request.readonly()) {
          e = new UnambiguousTimeoutException("The operation timed out possibly after being sent, and is read-only", ec);
        } else {
          e = new AmbiguousTimeoutException("The operation timed out possibly after being sent, and state could have been changed on the server as a result", ec);
        }
        return ProtostellarRequestBehaviour.fail(e);
      case NOT_FOUND:
        return ProtostellarRequestBehaviour.fail(new DocumentNotFoundException(context));
      case ALREADY_EXISTS:
        return ProtostellarRequestBehaviour.fail(new DocumentExistsException(context));
      case UNAUTHENTICATED:
      case PERMISSION_DENIED:
        return ProtostellarRequestBehaviour.fail(new AuthenticationFailureException("Server reported that permission to the resource was denied", context, t));
      case UNIMPLEMENTED:
        return ProtostellarRequestBehaviour.fail(new FeatureNotAvailableException(status.getMessage(), t));
      case UNAVAILABLE:
        return RetryOrchestratorProtostellar.shouldRetry(core, request, RetryReason.ENDPOINT_NOT_AVAILABLE);
      case OK:
        return ProtostellarRequestBehaviour.success();
      default:
        return ProtostellarRequestBehaviour.fail(new CouchbaseException(status.getMessage(), t, context));
    }
  }
}
