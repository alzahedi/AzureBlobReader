import Dependencies._

ThisBuild / scalaVersion := "2.13.12"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "azure-blob-reader",
    libraryDependencies ++= Seq(
      sparkCore,
      sparkSQL,
      hadoopAzure,
      azureIdentity,
      azureStorageBlob,
      munit % Test
    )
  )

// // Enable sbt-assembly for packaging a fat JAR
// enablePlugins(AssemblyPlugin)

// // Merge strategy for assembling JAR
// assembly / assemblyMergeStrategy := {
//   case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//   case _                             => MergeStrategy.first
// }
