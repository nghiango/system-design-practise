# Build context is the service directory; run `./gradlew :notification:notify-api:bootJar` first.
FROM eclipse-temurin:21-jre-jammy
RUN mkdir -p /diagnostics && chown 10001:10001 /diagnostics
WORKDIR /app
COPY build/libs/*.jar app.jar
EXPOSE 8080
ENV JAVA_TOOL_OPTIONS="-XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/diagnostics/heapdump.hprof -XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0"
USER 10001:10001
ENTRYPOINT ["java", "-jar", "app.jar"]
