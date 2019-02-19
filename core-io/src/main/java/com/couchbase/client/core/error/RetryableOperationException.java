package com.couchbase.client.core.error;

/**
 * This operation has returned a temporary error, and the application can retry it.
 *
 * Note that in some cases the operation may have succeeded despite the error - for instance, if
 * the server completed the operation but was unable to report that success.
 *
 * For this reason, the application must consider its error handling strategy, possibly on a
 * per-operation basis.  Some simple idempotent operations can simply be retried, others may need
 * a more sophisticated approach, possibly including using getFromReplica to ascertain the current
 * document content.
 */
interface RetryableOperationException {
}
