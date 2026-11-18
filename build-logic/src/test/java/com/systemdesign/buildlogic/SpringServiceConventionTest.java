package com.systemdesign.buildlogic;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SpringServiceConventionTest {

  @Test
  void appliesJava21SpringBootSpotlessAndIntegrationTest(@TempDir Path dir) throws IOException {
    Files.writeString(dir.resolve("settings.gradle.kts"), "rootProject.name = \"sample\"");
    Files.writeString(
        dir.resolve("build.gradle.kts"),
        """
        plugins { id("sd.spring-service") }
        tasks.register("printRelease") {
          doLast { println("release=" + tasks.compileJava.get().options.release.get()) }
        }
        """);

    var result =
        GradleRunner.create()
            .withProjectDir(dir.toFile())
            .withPluginClasspath()
            .withArguments("tasks", "--all", "printRelease")
            .build();

    assertThat(result.getOutput())
        .contains("integrationTest", "spotlessCheck", "bootJar", "release=21");
  }
}
