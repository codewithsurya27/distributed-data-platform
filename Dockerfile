# The executable Spring Boot JAR is built on the host or CI runner.
# This image only packages and runs that JAR.
FROM eclipse-temurin:25-jre

WORKDIR /app

# Create a non-root application user and uploads directory.
RUN useradd --system --uid 10001 appuser \
    && mkdir -p /app/uploads /tmp/build \
    && chown -R appuser:appuser /app

# Copy the Gradle build output temporarily.
COPY build/libs/ /tmp/build/

# Select only the executable Spring Boot JAR.
# Exclude Gradle's optional *-plain.jar.
RUN set -eux; \
    JAR="$(find /tmp/build -maxdepth 1 -type f -name '*.jar' ! -name '*-plain.jar' | head -n 1)"; \
    test -n "$JAR"; \
    cp "$JAR" /app/app.jar; \
    rm -rf /tmp/build; \
    chown appuser:appuser /app/app.jar

# Run the application as a non-root user.
USER 10001

EXPOSE 8080

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/app.jar"]