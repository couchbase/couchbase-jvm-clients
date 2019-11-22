package com.couchbase.client.scala

import com.couchbase.client.scala.env._
import org.junit.jupiter.api.Test

import scala.concurrent.duration.Duration
import scala.util.Success

class EnvironmentSpec {
  @Test
  def basic() {
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
  def badConnstrReturnsErr() {
    Cluster.connect("not:a:valid:conn:str", "", "") match {
      case Success(env) => assert(false)
      case _            =>
    }
  }

  @Test
  def connectWithSeedNodes() {
    Cluster.connect("node1,node2", "user", "pass") match {
      case Success(cluster) =>
        assert(cluster.async.seedNodes.size == 2)
        assert(cluster.async.seedNodes.contains(SeedNode("node1")))
        assert(cluster.async.seedNodes.contains(SeedNode("node2")))
      case _ => assert(false)
    }
  }

  @Test
  def basic_unowned() {
    val env     = ClusterEnvironment.create
    val cluster = Cluster.connect("localhost", "Administrator", "password").get
    assert(!env.owned)
    cluster.disconnect()
    env.shutdown()
    assert(env.threadPool.isShutdown)
    assert(env.threadPool.isTerminated)
  }

  @Test
  def basic_owned() {
    val cluster = Cluster.connect("localhost", "Administrator", "password").get
    val env     = cluster.async.env
    assert(env.owned)
    cluster.disconnect()
    assert(env.threadPool.isShutdown)
    assert(env.threadPool.isTerminated)
  }

  @Test
  def io_env() {
    val env = ClusterEnvironment.builder
      .ioEnvironment(
        IoEnvironment()
          .managerEventLoopGroup(null)
          .analyticsEventLoopGroup(null)
      )
      .build
      .get
    env.shutdown()
  }

  @Test
  def io_config() {
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
  def logging_config() {
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
}
