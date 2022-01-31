package com.couchbase.client.kotlin.annotations

/**
 * Specifies the earliest version of Couchbase Server that supports the annotated feature.
 */
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION,
    AnnotationTarget.TYPE_PARAMETER, AnnotationTarget.VALUE_PARAMETER,
    AnnotationTarget.EXPRESSION)
@Retention(AnnotationRetention.SOURCE)
@MustBeDocumented
internal annotation class SinceCouchbase(val version: String)
