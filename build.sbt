name := "csvw-check"

organization := "io.github.gss-cogs"
version := "0.0.3"
maintainer := "csvcubed@gsscogs.uk"

scalaVersion := "2.13.4"
scalacOptions ++= Seq("-deprecation", "-feature")
autoCompilerPlugins := true

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)
enablePlugins(UniversalPlugin)

dockerBaseImage := "openjdk:11"
dockerEntrypoint := Seq("bash")
dockerEnvVars := Map("PATH" -> "$PATH:/opt/docker/bin")
Docker / packageName := "csvwcheck"

libraryDependencies += "io.cucumber" %% "cucumber-scala" % "8.14.1" % Test
libraryDependencies += "io.cucumber" % "cucumber-junit" % "7.11.1" % Test
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test
libraryDependencies += "com.github.pathikrit" %% "better-files" % "3.9.2" % Test

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "io.spray" %% "spray-json" % "1.3.6"
libraryDependencies += "org.apache.jena" % "jena-arq" % "4.4.0"
libraryDependencies += "joda-time" % "joda-time" % "2.12.2"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.21"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.2"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.14.2"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.14.2"
libraryDependencies += "com.softwaremill.sttp.client3" %% "core" % "3.8.15"
libraryDependencies += "com.ibm.icu" % "icu4j" % "72.1"
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.10.0"
libraryDependencies += "com.chuusai" %% "shapeless" % "2.3.10"

publishTo := Some("GitHub Maven package repo for GSS-Cogs" at "https://maven.pkg.github.com/gss-cogs/csvw-check")
publishMavenStyle := true
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "gss-cogs",
  sys.env.getOrElse("GITHUB_TOKEN", "")
)