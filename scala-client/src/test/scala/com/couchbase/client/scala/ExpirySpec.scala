package com.couchbase.client.scala

import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import java.time.Instant
import java.util.concurrent.TimeUnit

import com.couchbase.client.scala.util.ExpiryUtil
import org.junit.Test

class ExpirySpec {
  @Test
  def expiry = {
    val now = Instant.now
    assert(ExpiryUtil.expiryActual(null, Some(now), now) == now.getEpochSecond)
    assert(ExpiryUtil.expiryActual(5 seconds, None, now) == now.getEpochSecond + 5)
    assert(ExpiryUtil.expiryActual(5 seconds, Some(now), now) == now.getEpochSecond)
    assert(
      ExpiryUtil.expiryActual(31 days, None, now) == now.getEpochSecond + TimeUnit.DAYS
        .toSeconds(31)
    )
  }
}
