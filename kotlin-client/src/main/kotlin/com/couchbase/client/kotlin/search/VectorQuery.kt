package com.couchbase.client.kotlin.search

import com.couchbase.client.core.api.search.queries.CoreSearchRequest
import com.couchbase.client.core.api.search.vector.CoreVectorQuery
import com.couchbase.client.core.api.search.vector.CoreVectorQueryCombination
import com.couchbase.client.core.api.search.vector.CoreVectorSearch
import com.couchbase.client.core.api.search.vector.CoreVectorSearchOptions
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi


/**
 * A single vector query, or a compound vector query.
 *
 * To create an instance for a single vector, call [SearchSpec.vector].
 *
 * To combine multiple vectors into a compound vector query,
 * call [SearchSpec.anyOf] or [SearchSpec.allOf].
 */
@VolatileCouchbaseApi
public sealed class VectorSearchSpec : SearchSpec()

/**
 * A vector [Full-Text Search query](https://docs.couchbase.com/server/current/fts/fts-supported-queries.html)
 *
 * Create an instance by calling [SearchSpec.vector].
 *
 * Create a compound vector query by calling [SearchSpec.anyOf] or [SearchSpec.allOf].
 *
 * See also: [SearchSpec].
 */
@VolatileCouchbaseApi
public sealed class VectorQuery : VectorSearchSpec() {
    internal abstract val core: CoreVectorQuery
    internal abstract fun withBoost(boost: Double?): VectorQuery

    override val coreRequest: CoreSearchRequest
        get() = CoreSearchRequest(
            null,
            CoreVectorSearch(listOf(core), null)
        )

    /**
     * Returns a new query that decorates this one with the given boost multiplier.
     * Has no effect unless this query is used in a disjunction or conjunction.
     */
    public infix fun boost(boost: Number): VectorQuery = this.withBoost(boost.toDouble())

    /**
     * Returns the JSON representation of this query condition.
     */
    public override fun toString(): String = core.toJson().toString()
}

internal data class InternalVectorQuery(
    val vector: FloatArray,
    val field: String,
    val numCandidates: Int,
    val boost: Double? = null,
) : VectorQuery() {
    override val core = CoreVectorQuery(vector, field, numCandidates, boost)
    override fun withBoost(boost: Double?) = copy(boost = boost)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as InternalVectorQuery

        if (!vector.contentEquals(other.vector)) return false
        if (field != other.field) return false
        if (numCandidates != other.numCandidates) return false
        if (boost != other.boost) return false

        return true
    }

    override fun hashCode(): Int {
        return vector.contentHashCode()
    }
}

internal class CompoundVectorSearchSpec(
    private val children: List<VectorQuery>,
    private val operator: CoreVectorQueryCombination,
) : VectorSearchSpec() {
    override val coreRequest: CoreSearchRequest
        get() = CoreSearchRequest(
            null,
            CoreVectorSearch(
                children.map { it.core },
                CoreVectorSearchOptions(operator)
            )
        )
}



