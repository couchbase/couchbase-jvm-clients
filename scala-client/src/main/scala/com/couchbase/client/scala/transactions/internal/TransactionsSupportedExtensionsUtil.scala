package com.couchbase.client.scala.transactions.internal

import com.couchbase.client.core.transaction.forwards.{CoreTransactionsExtension, CoreTransactionsSupportedExtensions}

object TransactionsSupportedExtensionsUtil {
  val Supported = new CoreTransactionsSupportedExtensions

  Supported.add(CoreTransactionsExtension.EXT_TRANSACTION_ID)
  Supported.add(CoreTransactionsExtension.EXT_DEFERRED_COMMIT)
  Supported.add(CoreTransactionsExtension.EXT_TIME_OPT_UNSTAGING)
  Supported.add(CoreTransactionsExtension.EXT_BINARY_METADATA)
  Supported.add(CoreTransactionsExtension.EXT_CUSTOM_METADATA_COLLECTION)
  Supported.add(CoreTransactionsExtension.EXT_QUERY)
  Supported.add(CoreTransactionsExtension.EXT_STORE_DURABILITY)
  Supported.add(CoreTransactionsExtension.BF_CBD_3838)
  Supported.add(CoreTransactionsExtension.BF_CBD_3787)
  Supported.add(CoreTransactionsExtension.BF_CBD_3705)
  Supported.add(CoreTransactionsExtension.BF_CBD_3794)
  Supported.add(CoreTransactionsExtension.EXT_REMOVE_COMPLETED)
  Supported.add(CoreTransactionsExtension.EXT_ALL_KV_COMBINATIONS)
  Supported.add(CoreTransactionsExtension.EXT_UNKNOWN_ATR_STATES)
  Supported.add(CoreTransactionsExtension.BF_CBD_3791)
  Supported.add(CoreTransactionsExtension.EXT_THREAD_SAFE)
  Supported.add(CoreTransactionsExtension.EXT_SERIALIZATION)
  Supported.add(CoreTransactionsExtension.EXT_SDK_INTEGRATION)
  Supported.add(CoreTransactionsExtension.EXT_MOBILE_INTEROP)
  Supported.add(CoreTransactionsExtension.EXT_REPLACE_BODY_WITH_XATTR)
  Supported.add(CoreTransactionsExtension.EXT_INSERT_EXISTING)
  Supported.add(CoreTransactionsExtension.EXT_QUERY_CONTEXT)

  // Not currently supported:
//  Supported.add(CoreTransactionsExtension.EXT_SINGLE_QUERY)
//  Supported.add(CoreTransactionsExtension.EXT_OBSERVABILITY)
//  Supported.add(CoreTransactionsExtension.EXT_BINARY_SUPPORT)
}
