package com.couchbase.client.scala

import com.couchbase.client.core.env.WanDevelopmentProfile
import com.couchbase.client.scala.env._
import org.junit.jupiter.api.Test

import scala.concurrent.duration.{Duration, _}
import scala.util.{Success, Try}

class EnvironmentSpec {
  @Test
  def basic(): Unit = {
    val env = ClusterEnvironment.create

    val cluster = Cluster
      .connect("localhost", ClusterOptions(PasswordAuthenticator("Admin", "pass")).environment(env))
      .get

    assert(!env.owned)
    assert(env == cluster.async.env)

    cluster.disconnect()
    env.shutdown()
  }

  @Test
  def badConnstrReturnsErr(): Unit = {
    Cluster.connect("not:a:valid:conn:str", "user", "pass") match {
      case Success(env) => assert(false)
      case _            =>
    }
  }

  @Test
  def connectWithSeedNodes(): Unit = {
    Cluster.connect("node1,node2", "user", "pass") match {
      case Success(cluster) =>
        assert(cluster.async.connectionString.original().equals("couchbase://node1,node2"))
      case _ => assert(false)
    }
  }

  @Test
  def basic_unowned(): Unit = {
    val env     = ClusterEnvironment.create
    val cluster = Cluster.connect("localhost", "Administrator", "password").get
    assert(!env.owned)
    cluster.disconnect()
    env.shutdown()
    assert(env.threadPool.isShutdown)
    assert(env.threadPool.isTerminated)
  }

  @Test
  def basic_owned(): Unit = {
    val cluster = Cluster.connect("localhost", "Administrator", "password").get
    val env     = cluster.async.env
    assert(env.owned)
    cluster.disconnect()
    assert(env.threadPool.isShutdown)
    assert(env.threadPool.isTerminated)
  }

  @Test
  def io_config(): Unit = {
    val env = ClusterEnvironment.builder
      .ioConfig(
        IoConfig()
          .mutationTokensEnabled(true)
          .configPollInterval(Duration("5 seconds"))
          .kvCircuitBreakerConfig(
            CircuitBreakerConfig()
              .enabled(true)
              .errorThresholdPercentage(50)
              .sleepWindow(Duration("10 seconds"))
          )
      )
      .build
      .get
    env.shutdown()
  }

  @Test
  def logging_config(): Unit = {
    val env = ClusterEnvironment.builder
      .loggerConfig(
        LoggerConfig()
          .loggerName("test")
          .fallbackToConsole(true)
          .disableSlf4J(true)
      )
      .build
      .get
    env.shutdown()
  }

  @Test
  def systemPropertiesShouldOverride(): Unit = {
    System.setProperty("com.couchbase.env.timeout.kvTimeout", "10s")
    System.setProperty("com.couchbase.env.timeout.queryTimeout", "15s")

    try {
      val env = ClusterEnvironment.builder
        .timeoutConfig(
          TimeoutConfig()
            .kvTimeout(5.seconds)
        )
        .build
        .get

      assert(env.timeoutConfig.kvTimeout().getSeconds == 10)
      assert(env.timeoutConfig.queryTimeout().getSeconds == 15)
    } finally {
      System.clearProperty("com.couchbase.env.timeout.kvTimeout")
      System.clearProperty("com.couchbase.env.timeout.queryTimeout")
    }
  }

  @Test
  def closeUnownedEnvironment(): Unit = {
    val clusterTry: Try[Cluster] = ClusterEnvironment.builder
      // Customize settings here
      .build
      .flatMap(env =>
        Cluster.connect(
          "1.2.3.4",
          ClusterOptions
            .create("username", "password")
            .environment(env)
        )
      )

    clusterTry match {
      case Success(cluster) =>
        assert(!cluster.env.owned)

        // Shutdown gracefully
        cluster.disconnect()
        cluster.env.shutdown()

        assert(cluster.env.threadPool.isShutdown)
        assert(cluster.env.threadPool.isTerminated)
      case _ => assert(false)
    }
  }

  @Test
  def closeOwnedEnvironment(): Unit = {
    val clusterTry: Try[Cluster] = Cluster.connect("1.2.3.4", "username", "password")

    clusterTry match {
      case Success(cluster) =>
        assert(cluster.env.owned)

        // Shutdown gracefully
        cluster.disconnect()

        assert(cluster.env.threadPool.isShutdown)
        assert(cluster.env.threadPool.isTerminated)

        // App should not shutdown an owned env, but make sure it's harmless
        cluster.env.shutdown()
      case _ => assert(false)
    }
  }

  @Test
  def dnsSrv(): Unit = {
    val env = ClusterEnvironment.builder
      .ioConfig(
        IoConfig()
          .enableDnsSrv(true)
      )
      .build
      .get
    assert(env.coreEnv.ioConfig().dnsSrvEnabled())
    env.shutdown()
  }

  @Test
  def wanDevelopmentProfile(): Unit = {
    val env = ClusterEnvironment.builder
      .applyProfile(ClusterEnvironment.WanDevelopmentProfile)
      .build
      .get

    assert(env.timeoutConfig.kvTimeout().toMillis == WanDevelopmentProfile.KV_TIMEOUT.toMillis)
    assert(
      env.timeoutConfig.connectTimeout().toMillis == WanDevelopmentProfile.CONNECT_TIMEOUT.toMillis
    )
    assert(
      env.timeoutConfig.viewTimeout().toMillis == WanDevelopmentProfile.SERVICE_TIMEOUT.toMillis
    )
  }

  @Test
  def nonExistentProfile(): Unit = {
    val env = ClusterEnvironment.builder
      .applyProfile("does-not-exist")
      .build

    assert(env.isFailure)
  }

}
