package com.couchbase.client.scala.transactions.internal

import com.couchbase.client.core.transaction.forwards.{
  CoreTransactionsExtension,
  CoreTransactionsSupportedExtensions
}

object TransactionsSupportedExtensionsUtil {
  val Supported: CoreTransactionsSupportedExtensions = CoreTransactionsSupportedExtensions.from(
    CoreTransactionsExtension.EXT_TRANSACTION_ID,
    CoreTransactionsExtension.EXT_DEFERRED_COMMIT,
    CoreTransactionsExtension.EXT_TIME_OPT_UNSTAGING,
    CoreTransactionsExtension.EXT_BINARY_METADATA,
    CoreTransactionsExtension.EXT_CUSTOM_METADATA_COLLECTION,
    CoreTransactionsExtension.EXT_QUERY,
    CoreTransactionsExtension.EXT_STORE_DURABILITY,
    CoreTransactionsExtension.BF_CBD_3838,
    CoreTransactionsExtension.BF_CBD_3787,
    CoreTransactionsExtension.BF_CBD_3705,
    CoreTransactionsExtension.BF_CBD_3794,
    CoreTransactionsExtension.EXT_REMOVE_COMPLETED,
    CoreTransactionsExtension.EXT_ALL_KV_COMBINATIONS,
    CoreTransactionsExtension.EXT_UNKNOWN_ATR_STATES,
    CoreTransactionsExtension.BF_CBD_3791,
    CoreTransactionsExtension.EXT_THREAD_SAFE,
    CoreTransactionsExtension.EXT_SERIALIZATION,
    CoreTransactionsExtension.EXT_SDK_INTEGRATION,
    CoreTransactionsExtension.EXT_MOBILE_INTEROP,
    CoreTransactionsExtension.EXT_REPLACE_BODY_WITH_XATTR,
    CoreTransactionsExtension.EXT_INSERT_EXISTING,
    CoreTransactionsExtension.EXT_QUERY_CONTEXT,
    CoreTransactionsExtension.EXT_PARALLEL_UNSTAGING,
    CoreTransactionsExtension.EXT_REPLICA_FROM_PREFERRED_GROUP,
    CoreTransactionsExtension.EXT_BINARY_SUPPORT,
    CoreTransactionsExtension.EXT_GET_MULTI

    // Not currently supported:
//  CoreTransactionsExtension.EXT_SINGLE_QUERY
//  CoreTransactionsExtension.EXT_OBSERVABILITY
  )
}
