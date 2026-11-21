pluginManagement { includeBuild("build-logic") }

plugins { id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0" }

rootProject.name = "system-design-practise"

// Every topics/<topic>/topic.yaml becomes project :<topic>; every
// topics/<topic>/services/<service>/build.gradle.kts becomes :<topic>:<service>.
val topicSlug = Regex("^[a-z][a-z0-9-]{1,30}$")

rootDir
    .resolve("topics")
    .listFiles()
    .orEmpty()
    .filter { it.resolve("topic.yaml").isFile }
    .sortedBy { it.name }
    .forEach { topicDir ->
      require(topicSlug.matches(topicDir.name)) { "invalid topic folder ${topicDir.name}" }
      val declared = Regex("""^name:\s*([a-z][a-z0-9-]{1,30})\s*$""", RegexOption.MULTILINE)
          .find(topicDir.resolve("topic.yaml").readText())?.groupValues?.get(1)
      require(declared == topicDir.name) { "topics/${topicDir.name}/topic.yaml must declare 'name: ${topicDir.name}'" }
      include(topicDir.name)
      project(":${topicDir.name}").projectDir = topicDir
      topicDir
          .resolve("services")
          .listFiles()
          .orEmpty()
          .filter { it.resolve("build.gradle.kts").isFile }
          .sortedBy { it.name }
          .forEach { serviceDir ->
            val path = ":${topicDir.name}:${serviceDir.name}"
            include(path)
            project(path).projectDir = serviceDir
          }
    }
