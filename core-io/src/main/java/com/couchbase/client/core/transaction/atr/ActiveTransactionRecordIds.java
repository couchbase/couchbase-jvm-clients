/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.transaction.atr;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.node.KeyValueLocator;
import com.couchbase.client.core.util.CbPreconditions;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Stability.Internal
public class ActiveTransactionRecordIds {
    // On MacOS there are actually 64 vBuckets, but creating more ATRs is still fine - still get benefits of reduced
    // ATR contention
    public final static int NUM_VBUCKETS = 1024;

    // Default number of ATRs discussed on TXNJ-122
    public final static int NUM_ATRS_DEFAULT = NUM_VBUCKETS;

    // TXNJ-112: Currently have 20 blocks available, can increase if needed
    public final static int MAX_ATR_BLOCKS = 20;
    public final static int MAX_ATRS = 1024 * MAX_ATR_BLOCKS;


    private ActiveTransactionRecordIds() { }

    // Each vbucket has 1 ATR.  The #136 stuff is used purely to force a document to a particular vbucket
    // Update: TXNJ-112 permits > 1024 ATRs.
    // Guaranteed to be no duplicate ids
    private static List<String> ATR_IDS;

    public static List<String> allAtrs(int numAtrs) {
        // ATRs are arranged in blocks of 1024 (one ATR per vbucket).  So if num_atrs=2048, simply use the first 2048 ATRs
        // (2 blocks worth), and get 2 ATRs for each vbucket.
        return ActiveTransactionRecordIds.ATR_IDS.stream().limit(numAtrs).collect(Collectors.toList());
    }

    static {
        try {
            ArrayList<String> atrIds = new ArrayList<>();

            try (BufferedReader stream = new BufferedReader(
                    new InputStreamReader(
                            ActiveTransactionRecordIds.class.getResourceAsStream("transaction-atr-ids.txt")))) {
                String in;
                while ((in = stream.readLine()) != null) {
                    if (in.startsWith("//") || in.trim().isEmpty()) continue;

                    atrIds.add(in);
                }
            }

            ATR_IDS = Collections.unmodifiableList(atrIds);
        }
        catch (Exception err) {
            throw new RuntimeException("Fatal error loading ATR ids, transactions will not operate", err);
        }
    }

    public static List<String> atrIdsForVbucket(int vbucketId, int numAtrs) {
        CbPreconditions.check(vbucketId >= 0);
        CbPreconditions.check(vbucketId < NUM_VBUCKETS);

        ArrayList<String> out = new ArrayList<>();
        int i = vbucketId;
        while (i < numAtrs) {
            out.add(ATR_IDS.get(i));
            i += NUM_VBUCKETS;
        }

        return out;
    }

    public static String randomAtrIdForVbucket(int vbucketId, int numAtrs) {
        List<String> possibilities = atrIdsForVbucket(vbucketId, numAtrs);
        int index = ThreadLocalRandom.current().nextInt(possibilities.size());
        return possibilities.get(index);
    }

    public static int vbucketForKey(String id, int numPartitions) {
        return KeyValueLocator.partitionForKey(id.getBytes(StandardCharsets.UTF_8), numPartitions);
    }


}
