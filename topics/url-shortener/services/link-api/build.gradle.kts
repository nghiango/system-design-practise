plugins { id("sd.spring-service") }

dependencies {
  implementation("org.springframework.boot:spring-boot-starter-web")
  implementation("org.springframework.boot:spring-boot-starter-jdbc")
  implementation("org.springframework.boot:spring-boot-starter-flyway")
  implementation("org.flywaydb:flyway-core")
  implementation("org.flywaydb:flyway-database-postgresql")
  implementation("org.springframework.boot:spring-boot-starter-kafka")
  runtimeOnly("org.postgresql:postgresql")

  integrationTestImplementation("org.testcontainers:postgresql:1.21.3")
}
