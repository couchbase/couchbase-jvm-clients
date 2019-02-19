package com.couchbase.client.scala.kv

import com.couchbase.client.core.error._
import com.couchbase.client.core.error.subdoc.SubDocumentException
import com.couchbase.client.core.msg.ResponseStatus

object DefaultErrors {
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
      case ResponseStatus.DURABILITY_INVALID_LEVEL => new DurabilityInvalidLevelException()
      case ResponseStatus.DURABILITY_IMPOSSIBLE => new DurabilityImpossibleException()
      case ResponseStatus.SYNC_WRITE_AMBIGUOUS => new DurableWriteInProgressException()
      case ResponseStatus.SYNC_WRITE_IN_PROGRESS => new DurabilityAmbiguous()

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
