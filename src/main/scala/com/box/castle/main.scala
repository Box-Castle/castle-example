package com.box.castle

object main {
  def main(args: Array[String]): Unit = {
    System.out.println("hello world")
    val document =
      """
############################ CASTLE CONFIGURATION ############################

castle:
  # ----------------
  # --- REQUIRED ---

  # https://github.com/Box-Castle/core/wiki/Configuration:-Core#castlenamespace
  namespace: castle

  # https://github.com/Box-Castle/core/wiki/Configuration:-Core#castlebrokers
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

  # https://github.com/Box-Castle/core/wiki/Configuration:-Core#castlecachesizeinbytes
  # cacheSizeInBytes: 1073741824

  # https://github.com/Box-Castle/core/wiki/Configuration:-Core#castlebuffersizeinbytes
  # bufferSizeInBytes: 4194176

  # https://github.com/Box-Castle/core/wiki/Configuration:-Core#castlegracefulshutdowntimeout
  # gracefulShutdownTimeoutSeconds: 10

  # https://github.com/Box-Castle/core/wiki/Configuration:-Core#castlebrokertimeout
  # brokerTimeoutSeconds: 60

zookeeper:
  # ----------------
  # --- REQUIRED ---

  # https://github.com/Box-Castle/core/wiki/Configuration:-ZooKeeper#zookeeperconnect
  connect: zk-address1000:2181,zk-address2000:2181,zk-address3000:2181

  # ----------------
  # --- OPTIONAL ---

  # https://github.com/Box-Castle/core/wiki/Configuration:-ZooKeeper#zookeeperroot
  root: castle

  # https://github.com/Box-Castle/core/wiki/Configuration:-ZooKeeper#zookeepersessiontimeout
  sessionTimeoutSeconds: 60

  # https://github.com/Box-Castle/core/wiki/Configuration:-ZooKeeper#zookeeperconnectiontimeout
  connectionTimeoutSeconds: 15

  # https://github.com/Box-Castle/core/wiki/Configuration:-ZooKeeper#zookeeperinitialconnecttimeout
  initialConnectTimeout: 30

leader:
  # ----------------
  # --- REQUIRED ---

  # Leader does not have any required configuration

  # ----------------
  # --- OPTIONAL ---

  # https://github.com/Box-Castle/core/wiki/Configuration:-Leader#leaderkafkatopicspollinterval
  kafkaTopicsPollIntervalSeconds: 10

  # https://github.com/Box-Castle/core/wiki/Configuration:-Leader#leaderavailableworkerspollinterval
  availableWorkersPollIntervalSeconds: 5

  # https://github.com/Box-Castle/core/wiki/Configuration:-Leader#leaderleadershipacquisitiontimeout
  leadershipAcquisitionTimeoutSeconds: 60

loggingCommitter:
  factoryClassName: abc.defg
  topics:
    - login
    - api
    - auth

      """
  System.out.println(config.parse(document))
  }
}
