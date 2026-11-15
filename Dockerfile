# ---- build ----
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN chmod +x mvnw && ./mvnw -B -q dependency:go-offline
COPY src src
RUN ./mvnw -B -q package -DskipTests

# ---- run ----
# One image, two modes. With no configuration it runs embedded: the topology in-process, no
# broker, sized for a 512 MB host. Set RISKSTREAM_MODE=kafka and KAFKA_BOOTSTRAP_SERVERS to
# run it as a Kafka Streams application.
FROM eclipse-temurin:21-jre
RUN groupadd --system riskstream && useradd --system --gid riskstream --no-create-home riskstream
WORKDIR /app
COPY --from=build /workspace/target/riskstream-*.jar app.jar
USER riskstream
EXPOSE 8080
ENV JAVA_TOOL_OPTIONS="-Xmx256m -XX:MaxMetaspaceSize=128m -XX:+UseSerialGC -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
