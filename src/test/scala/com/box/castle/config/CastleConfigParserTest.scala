package com.box.castle.config

import java.util.concurrent.TimeUnit

import com.box.castle.core.config._
import org.joda.time.Duration
import com.box.castle.config
import com.box.kafka.Broker
import org.specs2.mutable.Specification

import scala.concurrent.duration.FiniteDuration

/**
 * Created by dgrenader on 5/24/17.
 */
class CastleConfigParserTest extends Specification {

  "CastleConfigParser" should {
    "load config with custom values" in {
      val castleConfig = config.parse("""
############################ CASTLE CONFIGURATION ############################

castle:
  # ----------------
  # --- REQUIRED ---

  namespace: my_test-namespace

  brokers:
    - broker-address1000:9092
    - broker-address2000:9092
    - broker-address3000:9092
    - broker-address3000:9092

  #
  committers:
    - loggingCommitter

  # ----------------
  # --- OPTIONAL ---

  cacheSizeInBytes: 333222111000111999
  bufferSizeInBytes: 999999
  gracefulShutdownTimeoutMs: 7289293
  brokerTimeoutMs: 435094

zookeeper:
  # ----------------
  # --- REQUIRED ---

  connect: zk-address1000:2181,zk-address2000:2181,zk-address3000:2181

  # ----------------
  # --- OPTIONAL ---

  root: mockRoot
  sessionTimeoutMs: 283392
  connectionTimeoutMs: 83923
  initialConnectTimeoutMs: 172933

leader:
  # ----------------
  # --- REQUIRED ---

  # Leader does not have any required configuration

  # ----------------
  # --- OPTIONAL ---

  kafkaTopicsPollIntervalMs: 999998
  availableWorkersPollIntervalMs: 888887
  leadershipAcquisitionTimeoutMs: 7777776

loggingCommitter:
  factoryClassName: abc.defg.xyz.LoggingCommitterFactory
  topics:
    - login
    - api
    - auth
  topicsRegex: .*[ABC]*
  customConfig: loggingCommitterCustomConfig
  heartbeatCadenceMs: 111333
  numThreads: 71
  initialOffset: latest
  offsetOutOfRangePolicy: useLatestOffset
  parallelismFactor: 99
  parallelismFactorByTopic:
    login: 35
    api: 11
  corruptMessagePolicy: fail
  useKafkaMetadataManager: false
  batchSizeManager:
    targetBatchSizePercent: 0.35
    samplingIntervalMs: 32592
    maxWaitTimeMs: 98283
    discountFactor: 0.876
    fullBufferThresholdCount: 34285
    emptyBufferThresholdCount: 222333
    samplingSlots: 894


loggingCommitterCustomConfig:
  someKey: someValue
  otherKey: otherValue
                                      """)

      castleConfig.namespace must_== "my_test-namespace"
      castleConfig.brokers must_== Set(
        Broker(0, "broker-address1000", 9092),
        Broker(0, "broker-address2000", 9092),
        Broker(0, "broker-address3000", 9092),
        Broker(0, "broker-address3000", 9092))

      castleConfig.brokerTimeout must_== FiniteDuration(435094, TimeUnit.MILLISECONDS)
      castleConfig.bufferSizeInBytes must_== 999999
      castleConfig.cacheSizeInBytes must_== 333222111000111999L
      castleConfig.gracefulShutdownTimeout must_== FiniteDuration(7289293, TimeUnit.MILLISECONDS)

      // -------------------------------------------------------------------
      // Leader
      castleConfig.leaderConfig must_==
        LeaderConfig(
          kafkaTopicsPollInterval=FiniteDuration(999998, TimeUnit.MILLISECONDS),
          availableWorkersPollInterval=FiniteDuration(888887, TimeUnit.MILLISECONDS),
          leadershipAcquisitionTimeout=FiniteDuration(7777776, TimeUnit.MILLISECONDS))

      // -------------------------------------------------------------------
      // Committer
      val committerConfig = castleConfig.committerConfigs.head
      committerConfig.id must_== "loggingCommitter"
      committerConfig.factoryClassName must_== "abc.defg.xyz.LoggingCommitterFactory"

      committerConfig.customConfig must havePairs("someKey" -> "someValue", "otherKey" -> "otherValue")
      committerConfig.customConfig.size must_== 2

      committerConfig.topicsRegex.get.toString() must_== ".*[ABC]*"
      committerConfig.topicsSet must_== None

      committerConfig.heartbeatCadence must_== Some(new Duration(111333))
      committerConfig.numThreads must_== 71
      committerConfig.numThreads must_!= CommitterConfig.DefaultNumThreads

      committerConfig.initialOffset must_== InitialOffset.latest
      committerConfig.initialOffset must_!= CommitterConfig.DefaultInitialOffset

      committerConfig.offsetOutOfRangePolicy must_== OffsetOutOfRangePolicy.useLatestOffset
      committerConfig.offsetOutOfRangePolicy must_!= CommitterConfig.DefaultOffsetOutOfRangePolicy

      committerConfig.parallelismFactor must_== 99
      committerConfig.parallelismFactor must_!= CommitterConfig.DefaultParallelismFactor

      committerConfig.parallelismFactorByTopic must havePairs("login" -> 35, "api" -> 11)
      committerConfig.parallelismFactorByTopic.size must_== 2
      CommitterConfig.DefaultParallelismFactorByTopic.size must_== 0

      committerConfig.corruptMessagePolicy must_== CorruptMessagePolicy.fail
      committerConfig.corruptMessagePolicy must_!= CommitterConfig.DefaultCorruptMessagePolicy

      committerConfig.useKafkaMetadataManager must_== false
      committerConfig.useKafkaMetadataManager must_!= CommitterConfig.DefaultUseKafkaMetadataManager

      val batchSizeMangerConfig = committerConfig.batchSizeManagerConfig.get

      batchSizeMangerConfig.samplingSlots must_== 894
      batchSizeMangerConfig.samplingSlots must_!= BatchSizeManagerConfig.DefaultSamplingSlots

      batchSizeMangerConfig.samplingInterval must_== new Duration(32592)
      batchSizeMangerConfig.samplingInterval must_!= BatchSizeManagerConfig.DefaultSamplingInterval

      batchSizeMangerConfig.maxWaitTime must_== new Duration(98283)
      batchSizeMangerConfig.maxWaitTime must_!= BatchSizeManagerConfig.DefaultMaxWaitTime

      batchSizeMangerConfig.discountFactor must_== 0.876
      batchSizeMangerConfig.discountFactor must_!= BatchSizeManagerConfig.DefaultDiscountFactor

      batchSizeMangerConfig.fullBufferThresholdCount must_== 34285
      batchSizeMangerConfig.fullBufferThresholdCount must_!= BatchSizeManagerConfig.DefaultFullBufferThresholdCount

      batchSizeMangerConfig.emptyBufferThresholdCount must_== 222333
      batchSizeMangerConfig.emptyBufferThresholdCount must_!= BatchSizeManagerConfig.DefaultEmptyBufferThresholdCount

      batchSizeMangerConfig.targetBatchSizePercent must_== 0.35
      batchSizeMangerConfig.targetBatchSizePercent must_!= BatchSizeManagerConfig.DefaultTargetBatchSizePercent

      // -------------------------------------------------------------------
      // Zookeeper
      val zkConfig = castleConfig.castleZookeeperConfig

      zkConfig.connect must_== "zk-address1000:2181,zk-address2000:2181,zk-address3000:2181"

      zkConfig.root must_== "mockRoot"
      zkConfig.root must_!= CastleZookeeperConfig.DefaultRoot

      zkConfig.sessionTimeout must_== FiniteDuration(283392, TimeUnit.MILLISECONDS)
      zkConfig.sessionTimeout must_!= CastleZookeeperConfig.DefaultSessionTimeout

      zkConfig.connectionTimeout must_== FiniteDuration(83923, TimeUnit.MILLISECONDS)
      zkConfig.connectionTimeout must_!= CastleZookeeperConfig.DefaultConnectionTimeout

      zkConfig.initialConnectTimeout must_== FiniteDuration(172933, TimeUnit.MILLISECONDS)
      zkConfig.initialConnectTimeout must_!= CastleZookeeperConfig.DefaultInitialConnectTimeout

      zkConfig.retryPolicy must_!= CastleZookeeperConfig.DefaultRetryPolicy

    }

    "check secondary settings" in {
      val castleConfig = config.parse( """
############################ CASTLE CONFIGURATION ############################

castle:
  # ----------------
  # --- REQUIRED ---

  namespace: my_test-namespace

  brokers:
    - broker-address1000:9092
    - broker-address2000:9092
    - broker-address3000:9092
    - broker-address3000:9092

  #
  committers:
    - loggingCommitter

zookeeper:
  # ----------------
  # --- REQUIRED ---

  # https://github.com/Box-Castle/core/wiki/Configuration:-ZooKeeper#zookeeperconnect
  connect: zk-address1000:2181,zk-address2000:2181,zk-address3000:2181

loggingCommitter:
  factoryClassName: abc.defg.xyz.LoggingCommitterFactory
  topics:
    - login
    - api
    - auth
  heartbeatCadenceMs: 0
  batchSizeManager:
    targetBatchSizePercent: 0

""")

      castleConfig.namespace must_== "my_test-namespace"
      castleConfig.brokers must_== Set(
        Broker(0, "broker-address1000", 9092),
        Broker(0, "broker-address2000", 9092),
        Broker(0, "broker-address3000", 9092),
        Broker(0, "broker-address3000", 9092))

      // Committer
      val committerConfig = castleConfig.committerConfigs.head
      committerConfig.id must_== "loggingCommitter"
      committerConfig.factoryClassName must_== "abc.defg.xyz.LoggingCommitterFactory"

      committerConfig.topicsSet.get must contain("login", "api", "auth")

      committerConfig.heartbeatCadence must_== None

      committerConfig.batchSizeManagerConfig must_== None
    }
  }
}
