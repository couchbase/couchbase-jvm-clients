package com.couchbase.client.core.util;

import com.couchbase.client.core.msg.ResponseStatus;

public class ResponseStatusConverter {

	public static final int HTTP_OK = 200;
	public static final int HTTP_CREATED = 201;
	public static final int HTTP_ACCEPTED = 202;
	public static final int HTTP_BAD_REQUEST = 400;
	public static final int HTTP_UNAUTHORIZED = 401;
	public static final int HTTP_NOT_FOUND = 404;
	public static final int HTTP_INTERNAL_ERROR = 500;
	public static final int HTTP_TOO_MANY_REQUESTS = 429;

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
