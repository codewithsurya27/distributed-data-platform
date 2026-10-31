
package com.surya.DistributedDataPlatform.messaging.outbox.service;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class OutboxRecoveryServiceTest {

    @Test
    void recoversAbandonedOutboxEvents() {

        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);

        when(jdbcTemplate.update(anyString(), eq(60)))
                .thenReturn(2);

        OutboxRecoveryService service =
                new OutboxRecoveryService(jdbcTemplate, 60);

        service.recoverAbandonedEvents();

        verify(jdbcTemplate).update(anyString(), eq(60));
    }

    @Test
    void handlesCaseWhereNoEventsNeedRecovery() {

        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);

        when(jdbcTemplate.update(anyString(), eq(60)))
                .thenReturn(0);

        OutboxRecoveryService service =
                new OutboxRecoveryService(jdbcTemplate, 60);

        service.recoverAbandonedEvents();

        verify(jdbcTemplate).update(anyString(), eq(60));
    }
}
