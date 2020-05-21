name := "akka-streams-s3-upload"

scalaVersion in ThisBuild := "2.13.2"

val akkaVersion = "2.6.4"
val alpakkaVersion = "2.0.0"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-protobuf" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-s3" % alpakkaVersion
)
