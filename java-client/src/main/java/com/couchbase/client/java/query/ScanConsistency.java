package com.couchbase.client.java.query;

public enum ScanConsistency {

	/**
	 * This is the default (for single-statement requests). No timestamp vector is used
	 * in the index scan.
	 * This is also the fastest mode, because we avoid the cost of obtaining the vector,
	 * and we also avoid any wait for the index to catch up to the vector.
	 */
	NOT_BOUNDED,
	/**
	 * This implements strong consistency per request.
	 * Before processing the request, a current vector is obtained.
	 * The vector is used as a lower bound for the statements in the request.
	 * If there are DML statements in the request, RYOW is also applied within the request.
	 */
	REQUEST_PLUS,
	/**
	 * This implements strong consistency per statement.
	 * Before processing each statement, a current vector is obtained
	 * and used as a lower bound for that statement.
	 */
	STATEMENT_PLUS;

	public String value() {
		return this.name().toLowerCase();
	}
}
