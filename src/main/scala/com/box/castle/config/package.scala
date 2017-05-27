package com.box.castle

import com.box.castle.core.config.CastleConfig
import org.yaml.snakeyaml.Yaml

package object config {

  def parse(config: java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, Object]]): CastleConfig =
    CastleConfigParser(config).castleConfig

  def parse(yamlConfig: String): CastleConfig = {
    val yaml = new Yaml()
    val list = yaml.load(yamlConfig)
      .asInstanceOf[java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, Object]]]
    this.parse(list)
  }

}
