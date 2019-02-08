package com.couchbase.client.scala.kv

import com.couchbase.client.core.error._
import com.couchbase.client.core.msg.ResponseStatus

object DefaultErrors {
  def throwOnBadResult(status: ResponseStatus): RuntimeException = {
    status match {
      case ResponseStatus.EXISTS => new DocumentAlreadyExistsException()
      case ResponseStatus.LOCKED | ResponseStatus.TEMPORARY_FAILURE => new TemporaryLockFailureException()
      case ResponseStatus.NOT_FOUND => new DocumentDoesNotExistException()
      case ResponseStatus.SERVER_BUSY => new TemporaryFailureException()
      case ResponseStatus.OUT_OF_MEMORY => new CouchbaseOutOfMemoryException()
      case rs => new CouchbaseException("Unknown ResponseStatus: " + rs)
      // TODO remaining failures
    }
  }

}
