FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace

COPY gradlew gradlew
COPY gradlew.bat gradlew.bat
COPY gradle gradle
COPY build.gradle build.gradle
COPY settings.gradle settings.gradle
COPY gradle.properties gradle.properties
COPY gradle.lockfile gradle.lockfile
COPY src src

RUN chmod +x ./gradlew \
    && ./gradlew --no-daemon bootJar

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN mkdir -p /var/lib/dblog/state && chown -R 1000:1000 /var/lib/dblog /app
ENV JAVA_TOOL_OPTIONS="-XX:+UseG1GC -XX:InitialRAMPercentage=25.0 -XX:MaxRAMPercentage=65.0 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/var/lib/dblog/state/heapdump.hprof -XX:+ExitOnOutOfMemoryError -Xlog:gc*:stdout:time,uptime,level,tags -XX:StartFlightRecording=filename=/var/lib/dblog/state/runtime.jfr,settings=profile,maxage=30m,maxsize=256m,dumponexit=true"
COPY --from=build --chown=1000:1000 /workspace/build/libs/*.jar /app/dblog-impl.jar
USER 1000:1000
ENTRYPOINT ["java", "-jar", "/app/dblog-impl.jar"]
