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

package com.couchbase.client.core.util;

import com.couchbase.client.core.msg.ResponseStatus;

public class ResponseStatusConverter {

	private static final int HTTP_OK = 200;
	private static final int HTTP_CREATED = 201;
	private static final int HTTP_ACCEPTED = 202;
	private static final int HTTP_BAD_REQUEST = 400;
	private static final int HTTP_UNAUTHORIZED = 401;
	private static final int HTTP_NOT_FOUND = 404;
	private static final int HTTP_INTERNAL_ERROR = 500;
	private static final int HTTP_TOO_MANY_REQUESTS = 429;

	public static ResponseStatus fromHttp(final int code) {
		ResponseStatus status;
		switch (code) {
			case HTTP_OK:
			case HTTP_CREATED:
			case HTTP_ACCEPTED:
				status = ResponseStatus.SUCCESS;
				break;
			case HTTP_NOT_FOUND:
				status = ResponseStatus.NOT_FOUND;
				break;
			case HTTP_BAD_REQUEST:
				status = ResponseStatus.INVALID_ARGS;
				break;
			case HTTP_INTERNAL_ERROR:
				status = ResponseStatus.INTERNAL_ERROR;
				break;
			case HTTP_UNAUTHORIZED:
				status = ResponseStatus.NO_ACCESS;
				break;
			case HTTP_TOO_MANY_REQUESTS:
				status = ResponseStatus.TOO_MANY_REQUESTS;
				break;
			default:
				status = ResponseStatus.UNKNOWN;
		}
		return status;
	}

}
