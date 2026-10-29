package com.example.temporal.showcase.activity;

import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * РЕЕСТР ТОКЕНОВ ОЖИДАЮЩИХ ОПЕРАЦИЙ ЗАКАЗА (TaskToken Registry).
 *
 * Зачем нужен этот класс в системе заказов:
 * На Шаге 6 заказ передаётся во внешнюю партнёрскую систему (например, на таможенное оформление или внешнему логисту),
 * которая подтвердит выполнение позже через HTTP-колбэк (Async Activity Completion).
 *
 * Чтобы не блокировать поток воркера на часы или дни в ожидании ответа партнёра:
 * 1. Операция Activity получает у Temporal уникальный токен задачи ("номер накладной") — `byte[] taskToken`,
 *    сохраняет его в этот реестр по `workflowId` заказа и вызывает `doNotCompleteOnReturn()`, освобождая поток.
 * 2. Когда внешняя система вызывает `POST /api/showcase/{workflowId}/complete-async`, контроллер достаёт
 *    сохранённый `taskToken` из этого реестра и передаёт его в `ActivityCompletionClient.complete(...)`,
 *    после чего заказ продолжает движение к следующему шагу.
 *
 * (В продакшене `taskToken` обычно передаётся во внешнюю систему в теле запроса/сообщения или хранится в БД).
 */
@Component
public class AsyncTokenRegistry {

    /**
     * Таблица соответствия: "Имя процесса (workflowId)" -> "Секретный номерок задачи (taskToken)".
     */
    private final ConcurrentMap<String, byte[]> tokensByWorkflowId = new ConcurrentHashMap<>();

    /**
     * Положить номерок в коробочку (вызывается внутри ShowcaseActivityImpl.waitForExternalCallback).
     */
    public void saveToken(String workflowId, byte[] taskToken) {
        tokensByWorkflowId.put(workflowId, taskToken);
    }

    /**
     * Забрать номерок из коробочки, чтобы закрыть задачу (вызывается в ShowcaseController.completeAsyncActivity).
     * После взятия номерок удаляется из таблицы, так как использовать его можно только один раз.
     */
    public Optional<byte[]> consumeToken(String workflowId) {
        return Optional.ofNullable(tokensByWorkflowId.remove(workflowId));
    }

    /**
     * Посмотреть список всех процессов, которые прямо сейчас ждут внешнего подтверждения по номерку.
     */
    public Set<String> getWaitingWorkflowIds() {
        return Set.copyOf(tokensByWorkflowId.keySet());
    }
}
