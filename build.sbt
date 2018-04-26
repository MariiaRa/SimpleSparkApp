name := "SparkApp"

version := "0.1"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "sbt-plugin-releases on bintray" at "https://dl.bintray.com/sbt/sbt-plugin-releases/"
)

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % Test,
  "org.apache.hadoop" % "hadoop-hdfs" % "2.5.2")

enablePlugins(sbtdocker.DockerPlugin, JavaAppPackaging)

dockerfile in docker := {

  val jarFile: File = sbt.Keys.`package`.in(Compile, packageBin).value

  new Dockerfile {
    from("sparkcontainer:latest")
    add(jarFile, "/mainDir/app.jar")
    expose(4040)
    entryPoint("/mainDir/spark/startSpark.sh", "apples", "2001", "Egypt", "/storage/fao_data_crops_data.csv")
  }
}

dockerUpdateLatest := true
packageName in Docker := packageName.value
version in Docker := version.value


imageNames in docker := Seq(
  // Sets the latest tag
  ImageName(s"orteganav/sparkapp:latest")
)