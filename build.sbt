
name := "pg-batch-executor"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.2.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

assemblyMergeStrategy in assembly := {
  x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}