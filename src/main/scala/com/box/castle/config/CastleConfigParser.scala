package com.box.castle.config

import java.util

import com.box.castle.core.config.BatchSizeManagerConfig._
import com.box.castle.core.config._
import com.box.castle.router.RouterConfig
import com.box.kafka.Broker
import org.apache.curator.retry.ExponentialBackoffRetry
import org.joda.time.Duration

import scala.concurrent.duration.FiniteDuration

import scala.collection.JavaConverters._
import CastleConfigParser._

import Transformers._

class CastleConfigParser(config: java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, Object]]) {

  /**
   * Utility method to grab an entire section of config file as a Map[String, Object]
   * @param section
   * @return
   */
  private[this] def getSection(section: String): java.util.LinkedHashMap[String, Object] =
    if (config.containsKey(section)) {
      val result = config.get(section)
      if (result == null) {
        new util.LinkedHashMap[String, Object]()
      }
      else {
        result
      }
    }
    else {
      new util.LinkedHashMap[String, Object]()
    }

  /**
   * Utility method to get the value of a config key
   * @param section The config section
   * @param key The config key
   * @param transformer A transformation function to apply to the value
   */
  private[this] def get[T](section: String, key: String, transformer: Object => T): Option[T] = {
    val sectionMap = getSection(section)
    if (sectionMap.isEmpty)
      None
    else
    if (sectionMap.containsKey(key)) {
      val value = sectionMap.get(key)
      if (value == null)
        None
      else
        try { Some(transformer(value)) }
        catch {
          case ex: ClassCastException =>
            throw new IllegalArgumentException(s"The the value '$value' for key '$key' in section '$section' has an unexpected type", ex)
          case ex: NumberFormatException =>
            throw new IllegalArgumentException(s"The the value '$value' for key '$key' in section '$section' cannot be formatted as a number", ex)

        }
    }
    else
      None
  }

  /**
   * Utility method to read a required config key. If the config key does not exist,
   * an IllegalArgumentException will be thrown
   * @param section The config section
   * @param key The config key
   * @param transformer A transformation function to apply to the value
   * @throws IllegalArgumentException
   */
  private[this] def getRequired[T](section: String, key: String, transformer: Object => T): T =
    get(section, key, transformer) match {
      case Some(value) => value
      case None        => throw new IllegalArgumentException(
        s"Failed to get a value for the required key: '$key' from the '$section' section")
    }

  /**
   * Utility method to read an optional config key and return the value if it exists, otherwise
   * return a default value
   * @param section The config section
   * @param key The config key
   * @param transformer Transformation function to apply on the value
   * @param default The default value to return if the config key is not defined
   */
  private[this] def getOptional[T](section: String, key: String, transformer: Object => T, default: T): T =
    get(section, key, transformer).getOrElse(default)

  /**
   * Utility method to read a config key and return its value as String
   * @param section The config section
   * @param key The config key
   */
  private[this] def getString(section: String, key: String): Option[String] =
    get(section, key, asString)

  /**
   * Utility method to read a required config key and return its value as String.
   * If the config key is absent, an IllegalArgumentException is raised.
   * @param section The config section
   * @param key The config key
   * @throws IllegalArgumentException
   */
  private[this] def getRequiredString(section: String, key: String): String =
    getRequired(section, key, asString)

  /**
   * Utility method to read an optional config key and return its value as String.
   * If the config key is absent, the specified default value will be returned.
   * @param section The config section
   * @param key The config key
   * @param default The default value to return
   */
  private[this] def getOptionalString(section: String, key: String, default: String): String =
    getString(section, key).getOrElse(default)

  /**
   * Utility method to read an optional key and return its Value or defaultValue if not found
   */
  private[this] def getFromMap[T](cfg: java.util.LinkedHashMap[String, Object],
                                  key: String,
                                  transformer: Object => T, defaultValue: T): T = {
    if (cfg.containsKey(key)) {
      val value = cfg.get(key)
      transformer(value)
    }
    else {
      defaultValue
    }
  }

  private[this] val castleNamespace: String =
    getRequiredString(CastleConfigSectionName, "namespace")

  private[this] val brokers: Set[Broker] =
    getRequired(CastleConfigSectionName, "brokers", asArrayList[String]).map { portHost =>
      val split = portHost.split(":")
      Broker(0, split(0), split(1).toInt)
    }.toSet

  private[this] val committerConfigs: Seq[CommitterConfig] = {
    import CommitterConfig._

    for (
      committerId <- getRequired(CastleConfigSectionName, "committers", asArrayList[String]).map(_.trim)
    ) yield {
      val factoryClassName = getRequiredString(committerId, "factoryClassName")

      val customCommitterConfig =
        getString(committerId, "customConfig") match {
          case Some(customConfigSectionName) => getSection(customConfigSectionName).asScala.map {
            case (k, v) => (k, v.asInstanceOf[String])
          }.toMap
          case None                          => Map[String, String]()
        }

      val (topicsRegexRaw, topicsSet) = getString(committerId, "topicsRegex") match {
        case Some(topicsRegex) => (Some(topicsRegex.trim), None)
        case None => (None, Some(getRequired(committerId, "topics", asArrayList[String]).map(_.trim).toSet))
      }

      val heartbeatCadence: Option[Duration] = get(committerId, "heartbeatCadenceMs", asDurationMs).flatMap(hb =>
        if (hb.getMillis > 0) Some(hb) else None)

      val numThreads: Integer =
        getOptional(committerId, "numThreads", asInteger, DefaultNumThreads)

      val initialOffset = InitialOffset.withName(
        getOptionalString(committerId, "initialOffset", DefaultInitialOffset.toString)
      )

      val parallelismFactor: Integer = getOptional(committerId, "parallelismFactor", asInteger, DefaultParallelismFactor)
      val parallelismFactorByTopic = getOptional[Map[String, Int]](committerId, "parallelismFactorByTopic",
          { x: Object =>
            x.asInstanceOf[java.util.LinkedHashMap[String, Integer]].asScala.toMap.map { case (k, v) => k -> v.toInt }
          }, Map.empty[String, Int])

      val offsetOutOfRangePolicy =
        try {
          OffsetOutOfRangePolicy.withName(
            getOptionalString(committerId, "offsetOutOfRangePolicy", DefaultOffsetOutOfRangePolicy.toString)
          )
        } catch {
          case e: NoSuchElementException =>
            val validPolicies = OffsetOutOfRangePolicy.values.mkString(", ")
            throw new IllegalArgumentException(s"Invalid value for offsetOutOfRangePolicy. Must be one of these: $validPolicies")
        }

      val corruptMessagePolicy =
        try {
          CorruptMessagePolicy.withName(
            getOptionalString(committerId, "corruptMessagePolicy", DefaultCorruptMessagePolicy.toString)
          )
        } catch {
          case e: NoSuchElementException =>
            val validPolicies = CorruptMessagePolicy.values.mkString(", ")
            throw new IllegalArgumentException(s"Invalid value for corruptMessagePolicy. Must be one of these: $validPolicies")
        }

      val useKafkaMetadataManager: Boolean =
        getOptional(committerId, "useKafkaMetadataManager", asBoolean, DefaultUseKafkaMetadataManager)

      def createBatchSizeManagerConfig(batchSizeManagerConfigObject: Object): BatchSizeManagerConfig = {
        val c = batchSizeManagerConfigObject.asInstanceOf[java.util.LinkedHashMap[String, Object]]

        new BatchSizeManagerConfig(
          samplingSlots = getFromMap(c, "samplingSlots", asInteger, DefaultSamplingSlots),
          samplingInterval = getFromMap(c, "samplingIntervalMs", asDurationMs, DefaultSamplingInterval),
          maxWaitTime = getFromMap(c, "maxWaitTimeMs", asDurationMs, DefaultMaxWaitTime),
          discountFactor = getFromMap(c, "discountFactor", asDouble, DefaultDiscountFactor),
          fullBufferThresholdCount = getFromMap(c, "fullBufferThresholdCount", asInteger, DefaultFullBufferThresholdCount),
          emptyBufferThresholdCount = getFromMap(c, "emptyBufferThresholdCount", asInteger, DefaultEmptyBufferThresholdCount),
          targetBatchSizePercent = getFromMap(c, "targetBatchSizePercent", asDouble, DefaultTargetBatchSizePercent))
      }
      val batchSizeManagerConfig: Option[BatchSizeManagerConfig] =
        get(committerId, "batchSizeManager", createBatchSizeManagerConfig).flatMap(
          config => if (config.targetBatchSizePercent > 0) Some(config) else None
        )

      CommitterConfig(
        committerId,
        factoryClassName,
        customCommitterConfig,
        topicsRegexRaw,
        topicsSet,
        heartbeatCadence,
        numThreads,
        initialOffset,
        offsetOutOfRangePolicy,
        parallelismFactor,
        parallelismFactorByTopic,
        corruptMessagePolicy,
        useKafkaMetadataManager,
        batchSizeManagerConfig)
    }
  }
  private[this] val leaderConfig: LeaderConfig = {
    import LeaderConfig._

    if (getSection(LeaderConfigSectionName).isEmpty) {
      LeaderConfig()
    } else {
      LeaderConfig(
        getOptional(LeaderConfigSectionName, "kafkaTopicsPollIntervalMs", asFiniteDurationMs, DefaultKafkaPollInterval),
        getOptional(LeaderConfigSectionName, "availableWorkersPollIntervalMs", asFiniteDurationMs, DefaultAvailableWorkersPollInterval),
        getOptional(LeaderConfigSectionName, "leadershipAcquisitionTimeoutMs", asFiniteDurationMs, DefaultLeadershipAcquisitionTimeout))
    }
  }

  private[this] val routerConfig: RouterConfig = {
    import RouterConfig._
    if (getSection(RouterConfigSectionName).isEmpty) {
      RouterConfig()
    } else {
      RouterConfig(
        getOptional(RouterConfigSectionName, "maxWaitTimeMs", asFiniteDurationMs, DefaultMaxWait),
        getOptional(RouterConfigSectionName, "minBytes", asInteger, DefaultMinBytes)
      )
    }
  }

  private[this] val castleZookeeperConfig: CastleZookeeperConfig = {
    import CastleZookeeperConfig._

    val retryPolicyType = getOptionalString(CastleZkConfigSectionName, "retryPolicy", DefaultRetryPolicyType)
    require(retryPolicyType == DefaultRetryPolicyType, s"Only '$DefaultRetryPolicyType' is supported as a retry policy at this time")

    val exponentialBackoffBaseSleepTime: FiniteDuration =
      getOptional(CastleZkConfigSectionName, "exponentialBackoffBaseSleepTimeMs", asFiniteDurationMs, DefaultExponentialBackoffBaseSleepTime)

    val exponentialBackoffMaxRetries: Integer =
      getOptional(CastleZkConfigSectionName, "exponentialBackoffMaxRetries", asInteger, DefaultExponentialBackoffMaxRetries)

    val exponentialBackoffMaxSleepTime: FiniteDuration =
      getOptional(CastleZkConfigSectionName, "exponentialBackoffMaxSleepTimeMs", asFiniteDurationMs, DefaultExponentialBackoffMaxSleepTime)

    val retryPolicy = new ExponentialBackoffRetry(
      exponentialBackoffBaseSleepTime.toMillis.toInt,
      exponentialBackoffMaxRetries,
      exponentialBackoffMaxSleepTime.toMillis.toInt)

    CastleZookeeperConfig(
      getRequiredString(CastleZkConfigSectionName, "connect"),
      getOptionalString(CastleZkConfigSectionName, "root", DefaultRoot),
      getOptional(CastleZkConfigSectionName, "sessionTimeoutMs", asFiniteDurationMs, DefaultSessionTimeout),
      getOptional(CastleZkConfigSectionName, "connectionTimeoutMs", asFiniteDurationMs, DefaultConnectionTimeout),
      getOptional(CastleZkConfigSectionName, "initialConnectTimeoutMs", asFiniteDurationMs, DefaultInitialConnectTimeout),
      retryPolicy
    )
  }

  private[this] val brokerTimeout: FiniteDuration =
    getOptional(CastleConfigSectionName, "brokerTimeoutMs", asFiniteDurationMs, CastleConfig.DefaultBrokerTimeout)

  private[this] val bufferSizeInBytes: Integer =
    getOptional(CastleConfigSectionName, "bufferSizeInBytes", asInteger, CastleConfig.DefaultBufferSizeInBytes)

  private[this] val cacheSizeInBytes: Long =
    getOptional(CastleConfigSectionName, "cacheSizeInBytes", asLong, CastleConfig.DefaultCacheSizeInBytes)

  private[this] val gracefulShutdownTimeout: FiniteDuration =
    getOptional(CastleConfigSectionName, "gracefulShutdownTimeoutMs", asFiniteDurationMs, CastleConfig.DefaultGracefulShutdownTimeout)

  val castleConfig = CastleConfig(castleNamespace,
    brokers,
    leaderConfig,
    committerConfigs,
    castleZookeeperConfig,
    routerConfig,
    brokerTimeout,
    bufferSizeInBytes,
    cacheSizeInBytes,
    gracefulShutdownTimeout)
}

object CastleConfigParser {
  val CastleGroup = "castle"

  val CastleConfigSectionName = "castle"
  val LeaderConfigSectionName = "leader"
  val RouterConfigSectionName = "router"
  val CastleZkConfigSectionName = "zookeeper"

  def apply(config: java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, Object]]): CastleConfigParser =
    new CastleConfigParser(config)
}