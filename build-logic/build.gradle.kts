plugins { `kotlin-dsl` }

dependencies {
  implementation(libs.spring.boot.gradle.plugin)
  implementation(libs.spring.dependency.management.plugin)
  implementation(libs.spotless.gradle.plugin)

  testImplementation(gradleTestKit())
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testImplementation(libs.assertj.core)
  testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test { useJUnitPlatform() }
