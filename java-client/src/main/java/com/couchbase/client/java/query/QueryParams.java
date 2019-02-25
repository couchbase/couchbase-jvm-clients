package com.couchbase.client.java.query;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;


public class QueryParams implements Serializable {
	private static final long serialVersionUID = 8888370260267213831L;

	private String serverSideTimeout;
	private ScanConsistency consistency;
	private String scanWait;
	private String clientContextId;
	private Integer maxParallelism;
	private Integer pipelineCap;
	private Integer pipelineBatch;
	private Integer scanCap;
	private boolean disableMetrics;
	private Map<String, Object> rawParams;
	private boolean pretty;
	private boolean readonly;
	private QueryProfile profile;

	private final Map<String, String> credentials;

	/**
	 * If adhoc, the query should never be prepared.
	 */
	private boolean adhoc;

	private QueryParams() {
		adhoc = true;
		disableMetrics = false;
		pretty = true;
		readonly = false;
		credentials = new LinkedHashMap<String, String>();
	}

	/**
	 * Modifies the given N1QL query (as a {@link JsonObject}) to reflect these {@link QueryParams}.
	 * @param queryJson the N1QL query
	 */
	public void injectParams(JsonObject queryJson) {
		if (this.serverSideTimeout != null) {
			queryJson.put("timeout", this.serverSideTimeout);
		}
		if (this.consistency != null) {
			queryJson.put("scan_consistency", this.consistency.value());
		}
		if (this.scanWait != null
				&& (ScanConsistency.REQUEST_PLUS == this.consistency
				|| ScanConsistency.STATEMENT_PLUS == this.consistency)) {
			queryJson.put("scan_wait", this.scanWait);
		}
		if (this.clientContextId != null) {
			queryJson.put("client_context_id", this.clientContextId);
		}
		if (this.maxParallelism != null) {
			queryJson.put("max_parallelism", this.maxParallelism.toString());
		}
		if (this.pipelineCap != null) {
			queryJson.put("pipeline_cap", this.pipelineCap.toString());
		}
		if (this.pipelineBatch != null) {
			queryJson.put("pipeline_batch", this.pipelineBatch.toString());
		}
		if (this.scanCap != null) {
			queryJson.put("scan_cap", this.scanCap.toString());
		}
		if (this.disableMetrics) {
			queryJson.put("metrics", false);
		}

		
		if (!this.credentials.isEmpty()) {
			JsonArray creds = JsonArray.create();
			for (Map.Entry<String, String> c : credentials.entrySet()) {
				if (c.getKey() != null && !c.getKey().isEmpty()) {
					creds.add(JsonObject.create()
							.put("user", c.getKey())
							.put("pass", c.getValue()));
				}
			}
			if (!creds.isEmpty()) {
				queryJson.put("creds", creds);
			}
		}

		if (!this.pretty) {
			queryJson.put("pretty", false);
		}

		if (this.readonly) {
			queryJson.put("readonly", true);
		}

		if (this.profile != null) {
			queryJson.put("profile", this.profile.toString());
		}

		if (this.rawParams != null) {
			for (Map.Entry<String, Object> entry : rawParams.entrySet()) {
				queryJson.put(entry.getKey(), entry.getValue());
			}
		}
	}

	/**
	 * Helper method to convert a duration into the n1ql (golang) format.
	 */
	public static String durationToN1qlFormat(long duration, TimeUnit unit) {
		switch (unit) {
			case NANOSECONDS:
				return duration + "ns";
			case MICROSECONDS:
				return duration + "us";
			case MILLISECONDS:
				return duration + "ms";
			case SECONDS:
				return duration + "s";
			case MINUTES:
				return duration + "m";
			case HOURS:
				return duration + "h";
			case DAYS:
			default:
				return unit.toHours(duration) + "h";
		}
	}

	/**
	 * Start building a {@link QueryParams}, allowing to customize an N1QL request.
	 *
	 * @return a new {@link QueryParams}
	 */
	public static QueryParams build() {
		return new QueryParams();
	}

	/**
	 * Sets a maximum timeout for processing on the server side.
	 *
	 * @param timeout the duration of the timeout.
	 * @param unit the unit of the timeout, from nanoseconds to hours.
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams serverSideTimeout(long timeout, TimeUnit unit) {
		this.serverSideTimeout = durationToN1qlFormat(timeout, unit);
		return this;
	}

	/**
	 * Adds a client context ID to the request, that will be sent back in the response, allowing clients
	 * to meaningfully trace requests/responses when many are exchanged.
	 *
	 * @param clientContextId the client context ID (null to send none)
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams withContextId(String clientContextId) {
		this.clientContextId = clientContextId;
		return this;
	}

	/**
	 * If set to true (false being the default), the metrics object will not be returned from N1QL and
	 * as a result be more efficient. Note that if metrics are disabled you are loosing information
	 * to diagnose problems - so use with care!
	 *
	 * @param disableMetrics true if disabled, false otherwise (false = default).
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams disableMetrics(boolean disableMetrics) {
		this.disableMetrics = disableMetrics;
		return this;
	}

	/**
	 * Sets scan consistency.
	 *
	 * Note that {@link ScanConsistency#NOT_BOUNDED NOT_BOUNDED} will unset the {@link #scanWait} if it was set.
	 *
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams consistency(ScanConsistency consistency) {
		this.consistency = consistency;
		if (consistency == ScanConsistency.NOT_BOUNDED) {
			this.scanWait = null;
		}
		return this;
	}


	/**
	 * If the {@link ScanConsistency#NOT_BOUNDED NOT_BOUNDED scan consistency} has been chosen, does nothing.
	 *
	 * Otherwise, sets the maximum time the client is willing to wait for an index to catch up to the
	 * vector timestamp in the request.
	 *
	 * @param wait the duration.
	 * @param unit the unit for the duration.
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams scanWait(long wait, TimeUnit unit) {
		if (this.consistency == ScanConsistency.NOT_BOUNDED) {
			this.scanWait = null;
		} else {
			this.scanWait = durationToN1qlFormat(wait, unit);
		}
		return this;
	}

	/**
	 * Allows to override the default maximum parallelism for the query execution on the server side.
	 *
	 * @param maxParallelism the maximum parallelism for this query, 0 or negative values disable it.
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams maxParallelism(int maxParallelism) {
		this.maxParallelism = maxParallelism;
		return this;
	}


	/**
	 * Allows to specify if this query is adhoc or not.
	 *
	 * If it is not adhoc (so performed often), the client will try to perform optimizations
	 * transparently based on the server capabilities, like preparing the statement and
	 * then executing a query plan instead of the raw query.
	 *
	 * @param adhoc if the query is adhoc, default is true (plain execution).
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams adhoc(boolean adhoc) {
		this.adhoc = adhoc;
		return this;
	}


	/**
	 * Allows to add a credential username/password pair to this request. If a credential for that
	 * username was previously set by a similar call, it is replaced.
	 *
	 * @param login the username/bucketname to add a credential for.
	 * @param password the associated password.
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams withCredentials(String login, String password) {
		credentials.put(login, password);
		return this;
	}

	/**
	 * If set to false, the server will be instructed to remove extra whitespace from the JSON response
	 * in order to save bytes. In performance-critical environments as well as large responses this is
	 * recommended in order to cut down on network traffic.
	 *
	 * Note that this option is only supported in Couchbase Server 4.5.1 or later.
	 *
	 * @param pretty if set to false, pretty responses are disabled.
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams pretty(boolean pretty) {
		this.pretty = pretty;
		return this;
	}

	/**
	 * If set to true, it will signal the query engine on the server that only non-data modifying requests
	 * are allowed. Note that this rule is enforced on the server and not the SDK side.
	 *
	 * Controls whether a query can change a resulting record set.
	 *
	 * If readonly is true, then the following statements are not allowed:
	 *  - CREATE INDEX
	 *  - DROP INDEX
	 *  - INSERT
	 *  - MERGE
	 *  - UPDATE
	 *  - UPSERT
	 *  - DELETE
	 *
	 * @param readonly true if readonly should be forced, false is the default and will use the server side default.
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams readonly(boolean readonly) {
		this.readonly = readonly;
		return this;
	}

	/**
	 * Advanced: Maximum buffered channel size between the indexer client and the query service for index scans.
	 *
	 * This parameter controls when to use scan backfill. Use 0 or a negative number to disable.
	 *
	 * @param scanCap the scan_cap param, use 0 or negative number to disable.
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams scanCap(int scanCap) {
		this.scanCap = scanCap;
		return this;
	}

	/**
	 * Advanced: Controls the number of items execution operators can batch for Fetch from the KV.
	 *
	 * @param pipelineBatch the pipeline_batch param.
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams pipelineBatch(int pipelineBatch) {
		this.pipelineBatch = pipelineBatch;
		return this;
	}

	/**
	 * Advanced: Maximum number of items each execution operator can buffer between various operators.
	 *
	 * @param pipelineCap the pipeline_cap param.
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams pipelineCap(int pipelineCap) {
		this.pipelineCap = pipelineCap;
		return this;
	}

	/**
	 * Specifies if there should be a profile section returned with the request results.
	 * @see <a href="https://developer.couchbase.com/documentation/server/current/monitoring/monitoring-n1ql-query.html">Monitoring N1QL Queries</a>
	 *
	 * @param profile the profile param {@link QueryProfile}.
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams profile(QueryProfile profile) {
		this.profile = profile;
		return this;
	}

	/**
	 * Allows to specify an arbitrary, raw N1QL param.
	 *
	 * Use with care and only provide options that are supported by the server and are not exposed as part of the
	 * overall stable API in the {@link QueryParams} class.
	 *
	 * @param name the name of the property.
	 * @param value the value of the property, only JSON value types are supported.
	 * @return this {@link QueryParams} for chaining.
	 */
	public QueryParams rawParam(String name, Object value) {
		if (this.rawParams == null) {
			this.rawParams = new HashMap<String, Object>();
		}

		if (!JsonValue.checkType(value)) {
			throw new IllegalArgumentException("Only JSON types are supported.");
		}

		rawParams.put(name, value);
		return this;
	}

	/**
	 * Helper method to check if a custom server side timeout has been applied on the params.
	 *
	 * @return true if it has, false otherwise.
	 */
	public boolean hasServerSideTimeout() {
		return serverSideTimeout != null;
	}

	/**
	 * Helper method to check if a client context ID is set.
	 */
	public String clientContextId() {
		return clientContextId;
	}

	/**
	 * True if this query is adhoc, false otherwise.
	 *
	 * @return true if adhoc false otherwise.
	 */
	public boolean isAdhoc() {
		return adhoc;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		QueryParams that = (QueryParams) o;

		if (disableMetrics != that.disableMetrics) return false;
		if (adhoc != that.adhoc) return false;
		if (pretty != that.pretty) return false;
		if (readonly != that.readonly) return false;
		if (serverSideTimeout != null ? !serverSideTimeout.equals(that.serverSideTimeout) : that.serverSideTimeout != null)
			return false;
		if (consistency != that.consistency) return false;
		if (scanWait != null ? !scanWait.equals(that.scanWait) : that.scanWait != null) return false;
		if (clientContextId != null ? !clientContextId.equals(that.clientContextId) : that.clientContextId != null)
			return false;
		if (maxParallelism != null ? !maxParallelism.equals(that.maxParallelism) : that.maxParallelism != null)
			return false;
		if (pipelineCap != null ? !pipelineCap.equals(that.pipelineCap) : that.pipelineCap != null)
			return false;
		if (pipelineBatch != null ? !pipelineBatch.equals(that.pipelineBatch) : that.pipelineBatch != null)
			return false;
		if (scanCap != null ? !scanCap.equals(that.scanCap) : that.scanCap != null)
			return false;
		if (!credentials.equals(that.credentials)) return false;
		if (profile != that.profile) return false;
		return rawParams != null ? rawParams.equals(that.rawParams) : that.rawParams == null;
	}

	@Override
	public int hashCode() {
		int result = serverSideTimeout != null ? serverSideTimeout.hashCode() : 0;
		result = 31 * result + (consistency != null ? consistency.hashCode() : 0);
		result = 31 * result + (scanWait != null ? scanWait.hashCode() : 0);
		result = 31 * result + (clientContextId != null ? clientContextId.hashCode() : 0);
		result = 31 * result + (maxParallelism != null ? maxParallelism.hashCode() : 0);
		result = 31 * result + (scanCap != null ? scanCap.hashCode() : 0);
		result = 31 * result + (pipelineBatch != null ? pipelineBatch.hashCode() : 0);
		result = 31 * result + (pipelineCap != null ? pipelineCap.hashCode() : 0);
		result = 31 * result + (disableMetrics ? 1 : 0);
		result = 31 * result + credentials.hashCode();
		result = 31 * result + (rawParams != null ? rawParams.hashCode() : 0);
		result = 31 * result + (adhoc ? 1 : 0);
		result = 31 * result + (pretty ? 1 : 0);
		result = 31 * result + (readonly ? 1 : 0);
		result = 31 * result + (profile != null ? profile.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("QueryParams{");
		sb.append("serverSideTimeout='").append(serverSideTimeout).append('\'');
		sb.append(", consistency=").append(consistency);
		sb.append(", scanWait='").append(scanWait).append('\'');
		sb.append(", clientContextId='").append(clientContextId).append('\'');
		sb.append(", maxParallelism=").append(maxParallelism);
		sb.append(", scanCap=").append(scanCap);
		sb.append(", pipelineCap=").append(pipelineCap);
		sb.append(", pipelineBatch=").append(pipelineBatch);
		sb.append(", adhoc=").append(adhoc);
		sb.append(", readonly=").append(readonly);
		sb.append(", pretty=").append(pretty);
		sb.append(", disableMetrics=").append(disableMetrics);
		sb.append(", rawParams=").append(rawParams);
		if (!credentials.isEmpty())
			sb.append(", credentials=").append(credentials.size());
		if (profile != null) {
			sb.append(", profile=").append(profile.toString());
		}
		sb.append('}');
		return sb.toString();
	}
}
