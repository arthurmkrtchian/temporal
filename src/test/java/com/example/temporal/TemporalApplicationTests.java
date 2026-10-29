package com.example.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.testing.TestWorkflowEnvironment;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * БАЗОВЫЙ ДЫМОВОЙ ТЕСТ SPRING BOOT КОНТЕКСТА.
 *
 * Что мы здесь проверяем:
 * Успешно ли стартует наше приложение со всеми настройками из application.yml
 * и создаёт ли Spring автоматический пульт связи `WorkflowClient`.
 */
@SpringBootTest
class TemporalApplicationTests {

    @Autowired(required = false)
    private TestWorkflowEnvironment testWorkflowEnvironment;

    @Autowired(required = false)
    private WorkflowClient workflowClient;

    @Test
    void contextLoads() {
        // Ожидаем, что бин WorkflowClient успешно создан и не равен null
        assertThat(workflowClient).isNotNull();
    }
}
