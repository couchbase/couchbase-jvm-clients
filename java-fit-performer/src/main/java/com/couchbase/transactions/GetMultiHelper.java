/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.transactions;

// [skip:<3.8.0]

import com.couchbase.InternalPerformerFailure;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiMode;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiOptions;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiReplicasFromPreferredServerGroupMode;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiReplicasFromPreferredServerGroupOptions;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiReplicasFromPreferredServerGroupResult;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiResult;
import com.couchbase.client.protocol.transactions.CommandGetMulti;
import com.couchbase.twoway.TestFailure;
import com.couchbase.utils.ContentAsUtil;

import java.time.Duration;

import static com.couchbase.twoway.TwoWayTransactionShared.handleContentAsPerformerValidation;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GetMultiHelper {
  public static TransactionGetMultiOptions convertToGetMulti(com.couchbase.client.protocol.transactions.TransactionGetMultiOptions in) {
    var out = TransactionGetMultiOptions.transactionGetMultiOptions();
    if (in.hasMode()) {
      out = out.mode(switch (in.getMode()) {
        case PRIORITISE_LATENCY -> TransactionGetMultiMode.PRIORITISE_LATENCY;
        case DISABLE_READ_SKEW_DETECTION -> TransactionGetMultiMode.DISABLE_READ_SKEW_DETECTION;
        case PRIORITISE_READ_SKEW_DETECTION -> TransactionGetMultiMode.PRIORITISE_READ_SKEW_DETECTION;
        default -> throw new InternalPerformerFailure(new RuntimeException("Unknown mode: " + in.getMode()));
      });
    }
    return out;
  }

  public static TransactionGetMultiReplicasFromPreferredServerGroupOptions convertToGetMultiReplicas(com.couchbase.client.protocol.transactions.TransactionGetMultiOptions in) {
    var out = TransactionGetMultiReplicasFromPreferredServerGroupOptions.transactionGetMultiReplicasFromPreferredServerGroupOptions();
    if (in.hasMode()) {
      out = out.mode(switch (in.getMode()) {
        case PRIORITISE_LATENCY -> TransactionGetMultiReplicasFromPreferredServerGroupMode.PRIORITISE_LATENCY;
        case DISABLE_READ_SKEW_DETECTION -> TransactionGetMultiReplicasFromPreferredServerGroupMode.DISABLE_READ_SKEW_DETECTION;
        case PRIORITISE_READ_SKEW_DETECTION -> TransactionGetMultiReplicasFromPreferredServerGroupMode.PRIORITISE_READ_SKEW_DETECTION;
        default -> throw new InternalPerformerFailure(new RuntimeException("Unknown mode: " + in.getMode()));
      });
    }
    return out;
  }

  public static void handleGetMultiResult(CommandGetMulti request, TransactionGetMultiResult results) {
    if (request.getSpecsCount() != results.size()) {
      throw new TestFailure(new RuntimeException(String.format("Expected %d results but got %d", request.getSpecsCount(), results.size())));
    }

    for (int i = 0; i < request.getSpecsCount(); i++) {
      final int specIndex = i;
      var requestedSpec = request.getSpecs(i);

      if (requestedSpec.getExpectPresent() && !results.exists(i)) {
        throw new TestFailure(new RuntimeException(String.format("Require spec %d to be present but it was not", i)));
      }
      if (!requestedSpec.getExpectPresent() && results.exists(i)) {
        throw new TestFailure(new RuntimeException(String.format("Require spec %d to be missing but it was not", i)));
      }
      if (results.exists(i)) {
        if (requestedSpec.hasContentAsValidation()) {
          var expectedContent = requestedSpec.getContentAsValidation();
          var actualContent = ContentAsUtil.contentType(
                  expectedContent.getContentAs(),
                  () -> results.contentAs(specIndex, byte[].class),
                  () -> results.contentAs(specIndex, String.class),
                  () -> results.contentAs(specIndex, JsonObject.class),
                  () -> results.contentAs(specIndex, JsonArray.class),
                  () -> results.contentAs(specIndex, Boolean.class),
                  () -> results.contentAs(specIndex, Integer.class),
                  () -> results.contentAs(specIndex, Double.class));

          handleContentAsPerformerValidation(actualContent, expectedContent);
        }
      }
    }
  }

  public static void handleGetMultiFromPreferredServerGroupResult(CommandGetMulti request, TransactionGetMultiReplicasFromPreferredServerGroupResult results) {
    if (request.getSpecsCount() != results.size()) {
      throw new TestFailure(new RuntimeException(String.format("Expected %d results but got %d", request.getSpecsCount(), results.size())));
    }

    for (int i = 0; i < request.getSpecsCount(); i++) {
      final int specIndex = i;
      var requestedSpec = request.getSpecs(i);

      if (requestedSpec.getExpectPresent() && !results.exists(i)) {
        throw new TestFailure(new RuntimeException(String.format("Require spec %d to be present but it was not", i)));
      }
      if (!requestedSpec.getExpectPresent() && results.exists(i)) {
        throw new TestFailure(new RuntimeException(String.format("Require spec %d to be missing but it was not", i)));
      }
      if (results.exists(i)) {
        if (requestedSpec.hasContentAsValidation()) {
          var expectedContent = requestedSpec.getContentAsValidation();
          var actualContent = ContentAsUtil.contentType(
                  expectedContent.getContentAs(),
                  () -> results.contentAs(specIndex, byte[].class),
                  () -> results.contentAs(specIndex, String.class),
                  () -> results.contentAs(specIndex, JsonObject.class),
                  () -> results.contentAs(specIndex, JsonArray.class),
                  () -> results.contentAs(specIndex, Boolean.class),
                  () -> results.contentAs(specIndex, Integer.class),
                  () -> results.contentAs(specIndex, Double.class));

          handleContentAsPerformerValidation(actualContent, expectedContent);
        }
      }
    }
  }

}
