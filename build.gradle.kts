// Per-topic aggregate tasks used by the topic's Jenkins job, e.g.
// ./gradlew :url-shortener:topicTest
val topicTasks = mapOf("topicTest" to "test", "topicIntegrationTest" to "integrationTest", "topicAssemble" to "assemble")

rootProject.file("topics").listFiles().orEmpty().forEach { dir ->
  findProject(":${dir.name}")?.let { topic ->
    topicTasks.forEach { (aggregate, task) ->
      topic.tasks.register(aggregate) {
        group = "topic"
        description = "Runs '$task' for every service of topic ${topic.name}."
        dependsOn(topic.subprojects.map { "${it.path}:$task" })
      }
    }
  }
}
