package com.example.temporal.showcase.activity;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * КОНТРАКТ ВНЕШНИХ ОПЕРАЦИЙ РАСШИРЕННОГО ЗАКАЗА (Activity Interface).
 *
 * Роль в системе заказов:
 * Каждый метод в этом интерфейсе — это отдельная внешняя операция, выполняемая в рамках обработки заказа:
 * обращение в платёжный шлюз, параллельный опрос служб доставки, прохождение контрольных точек маршрута
 * или выполнение шагов (и возвратов) распределённой транзакции Saga.
 */
@ActivityInterface
public interface ShowcaseActivity {

    /**
     * 1. БЫСТРАЯ ПЕРВИЧНАЯ ПРОВЕРКА ЗАКАЗА (для Шага 1 — Local Activity).
     * Выполняется в памяти воркера за ~1 мс без постановки задачи в общую очередь на сервере.
     * Возвращает строку "LocalCheckOK:<scenarioName>".
     */
    @ActivityMethod
    String fastLocalCheck(String scenarioName);

    /**
     * 2. ВЫЗОВ ПЛАТЁЖНОГО ШЛЮЗА С СИМУЛЯЦИЕЙ СБОЕВ (для Шага 3 — Демонстрация RetryOptions и NonRetryable ошибок).
     *
     * @param operationName      Название операции для логов (например, "PaymentGatewayCheck").
     * @param failuresToSimulate Сколько первых попыток должны упасть с временной ошибкой:
     *                           - Если передать `2`, то попытки #1 и #2 выбросят временную ошибку шлюза,
     *                             а попытка #3 вернёт "Успешно выполнено с попытки #3".
     *                           - Если передать `-1`, метод сразу выбросит фатальную бизнес-ошибку (NonRetryable),
     *                             которую сервер Temporal НЕ будет повторять.
     */
    @ActivityMethod
    String callFlakyExternalService(String operationName, int failuresToSimulate);

    /**
     * 3. ЗАПРОС РАСЧЁТА В СЛУЖБУ ДОСТАВКИ (для Шага 4 — Параллельный тендер Promise.anyOf и CancellationScope).
     *
     * Имитирует ответ службы доставки через `simulatedDelayMs` миллисекунд.
     * Каждые 200 мс отправляет на сервер пульс (`ctx.heartbeat`), чтобы немедленно прервать работу,
     * если другая служба доставки ответила быстрее и запрос был отменён.
     */
    @ActivityMethod
    String requestProviderQuote(String providerName, int simulatedDelayMs);

    /**
     * 4. ПРОХОЖДЕНИЕ КОНТРОЛЬНЫХ ТОЧЕК МАРШРУТА ДОСТАВКИ (для Шага 5 — Heartbeats & Checkpoints внутри Child Workflow).
     *
     * Выполняет `totalSteps` этапов доставки, делая паузу `stepSleepMs` на каждом этапе.
     * На каждом этапе отправляет серверу номер пройденной контрольной точки (`ctx.heartbeat(step)`).
     * При перезапуске воркера читает последнюю сохранённую точку из `ctx.getHeartbeatDetails`
     * и продолжает маршрут с неё, а не с 1-го этапа.
     */
    @ActivityMethod
    String runHeavyTaskWithHeartbeat(String taskName, int totalSteps, int stepSleepMs);

    /**
     * 5. ПЕРЕДАЧА ЗАКАЗА ВО ВНЕШНЮЮ ПАРТНЁРСКУЮ СИСТЕМУ (для Шага 6 — Async Activity Completion по TaskToken).
     *
     * Получает уникальный токен задачи (`TaskToken`), сохраняет его в `AsyncTokenRegistry` и переводит шаг заказа
     * в режим ожидания внешнего HTTP-колбэка (`doNotCompleteOnReturn`), освобождая поток воркера.
     */
    @ActivityMethod
    String waitForExternalCallback(String workflowId);

    // =========================================================================
    // 6. ОПЕРАЦИИ И КОМПЕНСАЦИИ (ВОЗВРАТЫ) ДЛЯ ТРАНЗАКЦИИ ЗАКАЗА SAGA (Шаг 7)
    // =========================================================================

    /** Ступень 1 (Прямое действие): Зарезервировать товар на складе под заказ. */
    @ActivityMethod
    String sagaReserveInventory(String workflowId);

    /** Ступень 1 (Компенсация): Снять резерв и вернуть товар на склад при откате заказа. */
    @ActivityMethod
    void sagaCompensateInventory(String workflowId);

    /** Ступень 2 (Прямое действие): Списать оплату заказа с карты покупателя. */
    @ActivityMethod
    String sagaChargePayment(String workflowId, double amount);

    /** Ступень 2 (Компенсация): Оформить возврат списанных средств покупателю при откате заказа. */
    @ActivityMethod
    void sagaRefundPayment(String workflowId, double amount);

    /**
     * Ступень 3 (Прямое действие): Назначить курьера для выезда по заказу.
     * Если `simulateFailure = true`, выбрасывает ошибку отсутствия курьеров, запуская обратную цепочку возвратов Saga.
     */
    @ActivityMethod
    String sagaDispatchCourier(String workflowId, boolean simulateFailure);

    /** Ступень 3 (Компенсация): Отменить заявку на курьера. */
    @ActivityMethod
    void sagaCancelCourier(String workflowId);
}
