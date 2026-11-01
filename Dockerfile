# Stage 1: Build the Spring Boot application
FROM eclipse-temurin:25-jdk AS build

WORKDIR /workspace

COPY . .

RUN chmod +x ./gradlew

# Build the executable JAR.
# Tests are executed separately by GitHub Actions.
RUN ./gradlew --no-daemon clean bootJar -x test

# Copy the executable JAR to a predictable location
RUN set -eux; \
    JAR="$(find build/libs -maxdepth 1 -type f -name '*.jar' ! -name '*-plain.jar' | head -n 1)"; \
    test -n "$JAR"; \
    cp "$JAR" /workspace/app.jar


# Stage 2: Run the application
FROM eclipse-temurin:25-jre

WORKDIR /app

# Create a non-root application user
RUN useradd --system --uid 10001 appuser

# Create uploads directory and grant ownership
RUN mkdir -p /app/uploads \
    && chown -R appuser:appuser /app

# Copy the executable JAR
COPY --from=build --chown=appuser:appuser /workspace/app.jar /app/app.jar

# Run as non-root user
USER 10001

EXPOSE 8080

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/app.jar"]