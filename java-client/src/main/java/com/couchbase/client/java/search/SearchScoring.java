/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.search;

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.queries.CoreSearchScoring;
import org.jspecify.annotations.Nullable;

public abstract class SearchScoring {

  /**
   * Assign all hits a score of zero, for improved performance
   * when scores are not required.
   */
  public static SearchScoring disabled() {
    return DISABLED;
  }

  /**
   * Combine vector and non-vector query results using
   * relative score fusion.
   */
  @SinceCouchbase("8.5")
  public static RelativeScoreFusion relativeScoreFusion() {
    return RelativeScoreFusion.INSTANCE;
  }

  /**
   * Combine vector and non-vector query results using
   * reciprocal rank fusion.
   */
  @SinceCouchbase("8.5")
  public static ReciprocalRankFusion reciprocalRankFusion() {
    return ReciprocalRankFusion.INSTANCE;
  }

  @Stability.Internal
  abstract CoreSearchScoring toCore();

  private static final SearchScoring DISABLED = new SearchScoring() {
    @Override
    public CoreSearchScoring toCore() {
      return CoreSearchScoring.Disabled.INSTANCE;
    }
  };

  public static class RelativeScoreFusion extends SearchScoring {
    private static final RelativeScoreFusion INSTANCE = new RelativeScoreFusion(null);

    private final @Nullable Integer windowSize;

    private RelativeScoreFusion(@Nullable Integer windowSize) {
      this.windowSize = windowSize;
    }

    public RelativeScoreFusion withWindowSize(@Nullable Integer windowSize) {
      return new RelativeScoreFusion(windowSize);
    }

    @Override
    public CoreSearchScoring toCore() {
      return new CoreSearchScoring.RelativeScoreFusion(windowSize);
    }
  }

  public static class ReciprocalRankFusion extends SearchScoring {
    private static final ReciprocalRankFusion INSTANCE = new ReciprocalRankFusion(null, null);

    private final @Nullable Integer windowSize;
    private final @Nullable Integer rankConstant;

    private ReciprocalRankFusion(
      @Nullable Integer windowSize,
      @Nullable Integer rankConstant
    ) {
      this.windowSize = windowSize;
      this.rankConstant = rankConstant;
    }

    public ReciprocalRankFusion withWindowSize(@Nullable Integer windowSize) {
      return new ReciprocalRankFusion(windowSize, rankConstant);
    }

    public ReciprocalRankFusion withRankConstant(@Nullable Integer rankConstant) {
      return new ReciprocalRankFusion(windowSize, rankConstant);
    }

    @Override
    public CoreSearchScoring toCore() {
      return new CoreSearchScoring.ReciprocalRankFusion(windowSize, rankConstant);
    }
  }
}
