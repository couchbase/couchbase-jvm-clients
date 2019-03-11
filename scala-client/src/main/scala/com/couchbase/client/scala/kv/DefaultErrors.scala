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

package com.couchbase.client.scala.kv

import com.couchbase.client.core.error._
import com.couchbase.client.core.msg.ResponseStatus

private[scala] object DefaultErrors {
  def throwOnBadResult(status: ResponseStatus): RuntimeException = {
    status match {
      case ResponseStatus.TOO_BIG => new RequestTooBigException()
      case ResponseStatus.EXISTS => new DocumentAlreadyExistsException()
      case ResponseStatus.LOCKED => new TemporaryLockFailureException()
      case ResponseStatus.TEMPORARY_FAILURE => new TemporaryFailureException()
      case ResponseStatus.NOT_STORED => new DocumentMutationLostException()
      case ResponseStatus.NOT_FOUND => new DocumentDoesNotExistException()
      case ResponseStatus.SERVER_BUSY => new TemporaryFailureException()
      case ResponseStatus.OUT_OF_MEMORY => new CouchbaseOutOfMemoryException()
      case ResponseStatus.DURABILITY_INVALID_LEVEL => new DurabilityLevelNotAvailableException()
      case ResponseStatus.DURABILITY_IMPOSSIBLE => new DurabilityImpossibleException()
      case ResponseStatus.SYNC_WRITE_AMBIGUOUS => new DurabilityAmbiguous()
      case ResponseStatus.SYNC_WRITE_IN_PROGRESS => new DurableWriteInProgressException()

      // Any other error should not make it to this generic error handling
        // NOT_MY_VBUCKET - handled at a lower level by retrying the request
        // SUBDOC_FAILURE - handled in the subdoc handling code
        // UNSUPPORTED - probably a client-side bug if this is happening
        // NO_ACCESS - should raise an authentication error at a lower layer
        // UNKNOWN - will drop down to generic CouchbaseException case anyway

      case rs => new CouchbaseException("Unknown ResponseStatus: " + rs)
    }
  }

}
