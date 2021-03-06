import sbt._

object Dependencies {
  val akkaStream   = "com.typesafe.akka" %% "akka-stream"   % "2.4.2"
  val sprayJson    = "io.spray"          %% "spray-json"    % "1.3.2"
  val playJson     = "com.typesafe.play" %% "play-json"     % "2.5.1"
  val liftJson     = "net.liftweb"       %% "lift-json"     % "2.6.3"
  val circeCore    = "io.circe"          %% "circe-core"    % "0.3.0"
  val circeGeneric = "io.circe"          %% "circe-generic" % "0.3.0"
  val circeJawn    = "io.circe"          %% "circe-jawn"    % "0.3.0"
  val scalaTest    = "org.scalatest"     %% "scalatest"     % "2.2.6"

  val connectorDeps = Seq(
    akkaStream,
    sprayJson % Provided, playJson % Provided, liftJson % Provided, circeCore % Provided,
    scalaTest % Test, circeGeneric % Test, circeJawn % Test)

  val examplesDeps = Seq(sprayJson)
}
