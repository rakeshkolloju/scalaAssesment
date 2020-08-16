import sbtassembly.PathList

conflictManager:=
  ConflictManager.latestRevision

resolvers ++= Seq(
  "Jet Maven Releases" at "https://artifacts.notjet.net/maven-release-local/",
  "Jet Maven Snapshots" at "https://artifacts.notjet.net/maven-snapshot-local/",
  "Walmart repo" at "http://repository.walmart.com/content/groups/public/"
)

scalaVersion := "2.11.12"
val sparkVersion = "2.4.5"

libraryDependencies ++= Seq("org.scalaj" %% "scalaj-http" % "1.1.4" % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.hadoop" % "hadoop-azure-datalake" % "3.2.1",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3.2.6.5.3004-13",
  "org.apache.hadoop" % "hadoop-azure" % "2.7.3.2.6.5.3004-13"
  )

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8"
assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

coverageEnabled := true
coverageHighlighting := true





