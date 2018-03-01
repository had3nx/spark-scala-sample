name := "Dewa_apps"

version := "0.1"

scalaVersion := "2.11.8"




libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" % Provided

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0" % Provided

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0" % Provided

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0" % Provided

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"







assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("org", "codehaus", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "overview.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}








