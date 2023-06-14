package csvwcheck

import better.files.File

object TestPaths {
  val resourcesPath: File = File(System.getProperty("user.dir"))
    ./("src")
    ./("test")
    ./("resources")

  val fixturesPath: File = resourcesPath
    ./("features")
    ./("fixtures")
}
