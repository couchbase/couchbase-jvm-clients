package com.couchbase.client.java.query;

public enum QueryProfile {

	/**
	 * No profiling information is added to the query response.
	 */
	OFF {
		@Override
		public String toString() {
			return "off";
		}
	},

	/**
	 * The query response includes a profile section with stats and details
	 * about various phases of the query plan and execution.
	 * Three phase times will be included in the system:active_requests and
	 * system:completed_requests monitoring keyspaces.
	 */
	PHASES {
		@Override
		public String toString() {
			return "phases";
		}
	},

	/**
	 * Besides the phase times, the profile section of the query response document will
	 * include a full query plan with timing and information about the number of processed
	 * documents at each phase. This information will be included in the system:active_requests
	 * and system:completed_requests keyspaces.
	 */
	TIMINGS {
		@Override
		public String toString() {
			return "timings";
		}
	}
}