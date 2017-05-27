name := "castle"

version := "1.0"

organization := "com.box"

licenses += ("Apache 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

scalaVersion := "2.10.4"

mainClass in Compile := Some("com.box.castle.main")

libraryDependencies ++= Seq(
  "com.box"                 %% "castle-core"            % "1.0.3",
  "org.yaml"                %  "snakeyaml"              % "1.18",
  "org.slf4s"               %% "slf4s-api"              % "1.7.13",
  "joda-time"               %  "joda-time"              % "2.9.4",
  "org.specs2"              %% "specs2"                 % "2.4.2"   % "test"
)

licenses += ("Castle License", url("https://github.com/Box-Castle/castle/blob/master/LICENSE"))