
package com.surya.DistributedDataPlatform.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Node;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component("kafka")
public class KafkaHealthIndicator implements HealthIndicator {

    private final String bootstrapServers;

    public KafkaHealthIndicator(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public Health health() {
        AdminClient adminClient = null;

        try {
            adminClient = AdminClient.create(
                    Map.of(
                            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                            bootstrapServers
                    )
            );

            List<Node> nodes = adminClient
                    .describeCluster()
                    .nodes()
                    .get(3, TimeUnit.SECONDS)
                    .stream()
                    .toList();

            return Health.up()
                    .withDetail("bootstrapServers", bootstrapServers)
                    .withDetail("brokerCount", nodes.size())
                    .withDetail("brokers", nodes.stream()
                            .map(Node::toString)
                            .toList())
                    .build();

        } catch (Exception exception) {
            String errorMessage = exception.getMessage() != null
                    ? exception.getMessage()
                    : exception.getClass().getSimpleName();

            return Health.down()
                    .withDetail("bootstrapServers", bootstrapServers)
                    .withDetail("error", errorMessage)
                    .build();

        } finally {
            if (adminClient != null) {
                adminClient.close(Duration.ZERO);
            }
        }
    }
}
