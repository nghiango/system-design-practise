// Convention for every topic service: Java 21, Spring Boot, Spotless,
// unit tests + Testcontainers integration tests, actuator metrics and tracing.
plugins {
  java
  id("org.springframework.boot")
  id("io.spring.dependency-management")
  id("com.diffplug.spotless")
}

val logstashEncoderVersion = "8.0"
val testcontainersVersion = "1.21.3"

repositories { mavenCentral() }

java { toolchain { languageVersion.set(JavaLanguageVersion.of(21)) } }

tasks.withType<JavaCompile>().configureEach {
  options.encoding = "UTF-8"
  options.release.set(21)
}

spotless {
  java {
    googleJavaFormat("1.24.0")
    targetExclude("**/build/**", "**/generated-sources/**")
  }
}

val integrationTest: SourceSet =
    sourceSets.create("integrationTest") {
      compileClasspath += sourceSets.main.get().output
      runtimeClasspath += sourceSets.main.get().output
    }

configurations["integrationTestImplementation"].extendsFrom(configurations["testImplementation"])

configurations["integrationTestRuntimeOnly"].extendsFrom(configurations["testRuntimeOnly"])

dependencies {
  "implementation"("org.springframework.boot:spring-boot-starter-actuator")
  "implementation"("io.micrometer:micrometer-registry-prometheus")
  "implementation"("io.micrometer:micrometer-tracing-bridge-brave")
  "implementation"("io.zipkin.reporter2:zipkin-reporter-brave")
  "implementation"("io.zipkin.reporter2:zipkin-sender-urlconnection")
  "implementation"("net.logstash.logback:logstash-logback-encoder:$logstashEncoderVersion")

  "testImplementation"("org.springframework.boot:spring-boot-starter-test")
  "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")

  "integrationTestImplementation"("org.springframework.boot:spring-boot-testcontainers")
  "integrationTestImplementation"("org.testcontainers:junit-jupiter:$testcontainersVersion")
}

val integrationTestTask =
    tasks.register<Test>("integrationTest") {
      description = "Runs integration tests against isolated Testcontainers infrastructure."
      group = "verification"
      testClassesDirs = integrationTest.output.classesDirs
      classpath = integrationTest.runtimeClasspath
      shouldRunAfter(tasks.test)
    }

tasks.check { dependsOn(integrationTestTask) }

tasks.withType<Test>().configureEach {
  useJUnitPlatform()
  maxHeapSize = "1g"
  testLogging { events("passed", "failed", "skipped") }
}
