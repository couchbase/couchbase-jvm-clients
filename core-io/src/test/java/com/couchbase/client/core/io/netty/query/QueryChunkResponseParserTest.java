/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty.query;

import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DmlFailureException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.InternalServerFailureException;
import com.couchbase.client.core.error.ParsingFailureException;
import com.couchbase.client.core.error.PlanningFailureException;
import com.couchbase.client.core.error.PreparedStatementFailureException;
import com.couchbase.client.core.error.QuotaLimitedException;
import com.couchbase.client.core.error.RateLimitedException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static com.couchbase.client.test.Util.readResource;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies aspects of query response parsing.
 */
class QueryChunkResponseParserTest {

  /**
   * Since the conversion from a N1QL error into the actual exception type is a quite complex
   * if/else block, this test makes sure that even if changes are made, the input and output
   * are stable and don't break by accident.
   */
  @Test
  void errorHandlingRegressionTests() {
    assertThrows(ParsingFailureException.class, () -> runAndThrow("parsing"));

    assertThrows(PreparedStatementFailureException.class, () -> runAndThrow("prepared_4040"));
    assertThrows(PreparedStatementFailureException.class, () -> runAndThrow("prepared_4050"));
    assertThrows(PreparedStatementFailureException.class, () -> runAndThrow("prepared_4060"));
    assertThrows(PreparedStatementFailureException.class, () -> runAndThrow("prepared_4070"));
    assertThrows(PreparedStatementFailureException.class, () -> runAndThrow("prepared_4080"));
    assertThrows(PreparedStatementFailureException.class, () -> runAndThrow("prepared_4090"));

    assertThrows(IndexExistsException.class, () -> runAndThrow("index_exists_4300"));
    assertThrows(IndexExistsException.class, () -> runAndThrow("index_exists_5000"));

    assertThrows(PlanningFailureException.class, () -> runAndThrow("planning_4000"));
    assertThrows(PlanningFailureException.class, () -> runAndThrow("planning_4321"));

    assertThrows(IndexNotFoundException.class, () -> runAndThrow("index_not_found_12004"));
    assertThrows(IndexNotFoundException.class, () -> runAndThrow("index_not_found_12016"));
    assertThrows(IndexNotFoundException.class, () -> runAndThrow("index_not_found_5000"));

    assertThrows(QuotaLimitedException.class, () -> runAndThrow("quota_limited"));

    assertThrows(InternalServerFailureException.class, () -> runAndThrow("internal_5000"));

    assertThrows(CasMismatchException.class, () -> runAndThrow("cas_mismatch"));
    assertThrows(CasMismatchException.class, () -> runAndThrow("cas_mismatch_reason"));

    assertThrows(DmlFailureException.class, () -> runAndThrow("dml_failure"));

    assertThrows(AuthenticationFailureException.class, () -> runAndThrow("auth_13014"));
    assertThrows(AuthenticationFailureException.class, () -> runAndThrow("auth_10000"));

    assertThrows(IndexFailureException.class, () -> runAndThrow("index_12000"));
    assertThrows(IndexFailureException.class, () -> runAndThrow("index_14000"));

    assertThrows(FeatureNotAvailableException.class, () -> runAndThrow("query_context"));
    assertThrows(FeatureNotAvailableException.class, () -> runAndThrow("preserve_expiry"));

    assertThrows(UnambiguousTimeoutException.class, () -> runAndThrow("streaming"));

    assertThrows(RateLimitedException.class, () -> runAndThrow("rate_limited_1191"));
    assertThrows(RateLimitedException.class, () -> runAndThrow("rate_limited_1192"));
    assertThrows(RateLimitedException.class, () -> runAndThrow("rate_limited_1193"));
    assertThrows(RateLimitedException.class, () -> runAndThrow("rate_limited_1194"));

    assertThrows(CouchbaseException.class, () -> runAndThrow("empty_list"));
    assertThrows(CouchbaseException.class, () -> runAndThrow("unknown"));

    assertThrows(DocumentNotFoundException.class, () -> runAndThrow("kv_notfound"));
    assertThrows(DocumentExistsException.class, () -> runAndThrow("kv_exists"));
  }

  @Test
  void errorContextIncludesReasonAndRetry() {
    DocumentExistsException ex = assertThrows(DocumentExistsException.class, () -> runAndThrow("kv_exists"));

    String message = ex.getMessage();
    assertTrue(message.contains("17012"));
    assertTrue(message.contains("Duplicate Key: k1"));
    assertTrue(message.contains("\"retry\""));
  }

  /**
   * Loads the mocked error response and throws the error which is then caught by the calling methods.
   *
   * @param file the relative filepath without the .json ending and the failure prefix.
   */
  private static void runAndThrow(final String file) {
    String input = readResource("failure_" + file + ".json", QueryChunkResponseParserTest.class);
    // At the moment the query chunk response parser does not care about the http status code and only
    // prints it as part of the error context, so we can pick whatever we want here.
    HttpResponse header = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
    throw QueryChunkResponseParser.errorsToThrowable(input.getBytes(StandardCharsets.UTF_8), header, null);
  }

}