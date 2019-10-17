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

package com.couchbase.client.java.analytics;

/**
 * The possible values for scan consistency in an analytics request.
 *
 * @since 3.0.0
 */
public enum AnalyticsScanConsistency {

	/**
	 * The index will return whatever state it has to the analytics query engine at the time of query.
	 * <p>
	 * This is the default (for single-statement requests). No timestamp vector is used in the index scan. This is also
	 * the fastest mode, because we avoid the cost of obtaining the vector, and we also avoid any wait for the index
	 * to catch up to the vector.
	 */
	NOT_BOUNDED {
		@Override
		public String toString() {
			return "not_bounded";
		}
	},

	/**
	 * The index will wait until all mutations have been processed at the time of request before being processed in the
	 * analytics query engine.
	 * <p>
	 * This implements strong consistency per request. Before processing the request, a current vector is obtained.
	 * The vector is used as a lower bound for the statements in the request.
	 */
	REQUEST_PLUS {
		@Override
		public String toString() {
			return "request_plus";
		}
	}

}
