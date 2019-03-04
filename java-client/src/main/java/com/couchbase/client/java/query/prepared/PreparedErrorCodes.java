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
package com.couchbase.client.java.query.prepared;

/**
 * Prepared query error codes
 *
 */
public enum PreparedErrorCodes {

	NO_SUCH_PREPARED_STATEMENT(4040, "No such prepared statement"),

	INDEX_NOT_FOUND(5000, "Index not found"),

	ENCODED_PLAN_MISMATCH(4080, "Mismatching prepared encoded plan"),

	ENCODED_PLAN_DECODE_ERROR(4070, "Unable to decode prepared statement");


	private final int code;
	private final String msg;

	PreparedErrorCodes(int code, String msg) {
		this.code = code;
		this.msg = msg;
	}

	public int code() {
		return this.code;
	}

	@Override
	public String toString() {
		return "PreparedErrorCode{" +
						"msg:" + this.msg +
						"code:" + this.code +
						"}";
	}
}
