package com.example.temporal.showcase.activity;

import com.example.temporal.showcase.workflow.ShowcaseWorkflow;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.client.ActivityCanceledException;
import io.temporal.failure.ApplicationFailure;
import io.temporal.spring.boot.ActivityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Base64;
import java.util.Optional;

/**
 * РЕАЛИЗАЦИЯ ВНЕШНИХ ОПЕРАЦИЙ ЗАКАЗА (ShowcaseActivityImpl).
 *
 * Роль в системе заказов:
 * Здесь находится код, который обращается во внешние системы (платёжный шлюз, склад, службы доставки),
 * выполняет блокирующие ожидания (`Thread.sleep`), обрабатывает сетевые сбои и отправляет отметки прогресса (Heartbeat).
 *
 * Контекст выполнения операции (`Activity.getExecutionContext()`):
 * Внутри любого метода Activity доступен контекст `ActivityExecutionContext`, который позволяет:
 * 1) Узнать номер текущей попытки выполнения: `ctx.getInfo().getAttempt()` (1-я, 2-я, 3-я...).
 * 2) Отправить сигнал активности и сохранить номер пройденной контрольной точки маршрута: `ctx.heartbeat(step)`.
 * 3) Получить токен задачи (`ctx.getTaskToken()`) для асинхронного завершения внешней партнёрской системой.
 */
@Component
@ActivityImpl(taskQueues = ShowcaseWorkflow.TASK_QUEUE)
public class ShowcaseActivityImpl implements ShowcaseActivity {

    private static final Logger log = LoggerFactory.getLogger(ShowcaseActivityImpl.class);

    private final AsyncTokenRegistry tokenRegistry;

    public ShowcaseActivityImpl(AsyncTokenRegistry tokenRegistry) {
        this.tokenRegistry = tokenRegistry;
    }

    /**
     * ШАГ 1: Быстрая первичная валидация параметров заказа (Local Activity).
     *
     * Выполняется в том же потоке и памяти, где работает Workflow Worker, без пересылки задачи через очередь сервера.
     */
    @Override
    public String fastLocalCheck(String scenarioName) {
        log.info("⚡ [LOCAL ACTIVITY] Мгновенная локальная валидация сценария '{}' в памяти воркера (без очереди Matching Service)", scenarioName);
        return "LocalCheckOK:" + scenarioName;
    }

    /**
     * ШАГ 3: Обращение в платёжный шлюз с демонстрацией автоповторов (Retries) и фатальных ошибок (NonRetryable).
     *
     * Порядок работы (например, при `failuresToSimulate = 2`):
     * 1. Получаем номер текущей попытки: `int attempt = ctx.getInfo().getAttempt()` (начинается с 1).
     * 2. Если `failuresToSimulate < 0` (например, `-1`), симулируем НЕПОВТОРЯЕМУЮ бизнес-ошибку
     *    (`ApplicationFailure.newNonRetryableFailure`) — например, неверный формат реквизитов.
     *    Сервер Temporal видит флаг `NonRetryable` и немедленно прекращает повторные попытки.
     * 3. Если `attempt <= failuresToSimulate` (попытки #1 и #2 при `failuresToSimulate = 2`):
     *    Выбрасываем обычную ошибку временного сбоя шлюза (`ApplicationFailure.newFailure`).
     *    Сервер Temporal перехватывает её и автоматически назначает повтор по правилам `RetryOptions` (через 1с, затем 1.5с).
     * 4. На попытке #3 (`attempt = 3 > 2`) операция проходит успешно и возвращает результат в Workflow.
     */
    @Override
    public String callFlakyExternalService(String operationName, int failuresToSimulate) {
        ActivityExecutionContext ctx = Activity.getExecutionContext();
        int attempt = ctx.getInfo().getAttempt();

        // Сценарий А: Неповторяемая (фатальная) бизнес-ошибка
        if (failuresToSimulate < 0) {
            log.error("🚫 [NON-RETRYABLE ERROR] Операция '{}' вернула фатальную бизнес-ошибку на попытке #{}. Ретраи немедленно остановлены!",
                    operationName, attempt);
            throw ApplicationFailure.newNonRetryableFailure(
                    "Фатальная ошибка валидации (повторы бессмысленны)",
                    "FatalBusinessError"
            );
        }

        log.info("🌐 [ACTIVITY RETRY DEMO] Вызов '{}' -> Попытка #{} (нужно пережить сбоев: {})",
                operationName, attempt, failuresToSimulate);
        sleepQuietly(600);

        // Сценарий Б: Временная ошибка сети/шлюза (пока номер попытки меньше или равен числу заказанных сбоев)
        if (attempt <= failuresToSimulate) {
            log.warn("💥 [ACTIVITY FAILED] '{}' упала на попытке #{}! Сервер Temporal запланирует повтор по экспоненциальному backoff...",
                    operationName, attempt);
            throw ApplicationFailure.newFailure(
                    "Временный сбой внешнего шлюза на попытке #" + attempt,
                    "TransientGatewayTimeout"
            );
        }

        // Сценарий В: Все запланированные сбои позади — попытка успешна!
        String result = "Успешно выполнено с попытки #" + attempt;
        log.info("✅ [ACTIVITY SUCCESS] '{}' прошла успешно на попытке #{}!", operationName, attempt);
        return result;
    }

    /**
     * ШАГ 4: Запрос расчёта доставки у одной из 3 курьерских служб (FastExpress 700мс vs CityPost 1800мс vs SlowCargo 3000мс).
     *
     * Как работает отмена проигравших запросов в Temporal:
     * Когда в Workflow побеждает самая быстрая служба ("FastExpress" за 700 мс), Workflow вызывает `raceScope.cancel()`.
     * Узнать о том, что запрос отменён, выполняющаяся Activity может во время вызова `ctx.heartbeat(...)`.
     * Поэтому ожидание разбито на интервалы по 200 мс с отправкой `ctx.heartbeat(elapsed)`.
     * Как только тендер завершён и `raceScope` отменён, вызов `ctx.heartbeat` у двух медленных служб
     * выбрасывает `ActivityCanceledException`, и они сразу прекращают работу.
     */
    @Override
    public String requestProviderQuote(String providerName, int simulatedDelayMs) {
        ActivityExecutionContext ctx = Activity.getExecutionContext();
        log.info("🏁 [RACE / PARALLEL] Запрос к провайдеру '{}' (ожидаемое время ответа ~{} мс)...",
                providerName, simulatedDelayMs);

        int stepMs = 200;
        int elapsed = 0;
        try {
            while (elapsed < simulatedDelayMs) {
                Thread.sleep(Math.min(stepMs, simulatedDelayMs - elapsed));
                elapsed += stepMs;
                // Отправляем пульс каждые 200 мс: если в Workflow уже вызвали raceScope.cancel(),
                // эта строчка мгновенно выбросит ActivityCanceledException
                ctx.heartbeat(elapsed);
            }
        } catch (ActivityCanceledException e) {
            // Ожидаемое поведение для проигравших служб доставки (CityPost и SlowCargo):
            log.info("🛑 [RACE CANCELED] Провайдер '{}' отменён на {} мс, так как другой провайдер ответил быстрее!",
                    providerName, elapsed);
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("🛑 [RACE INTERRUPTED] Провайдер '{}' прерван.", providerName);
            throw Activity.wrap(e);
        }

        // Ожидаемое поведение для победителя (FastExpress):
        String msg = providerName + " (ответил за " + simulatedDelayMs + " мс)";
        log.info("🏆 [RACE FINISHED] Провайдер '{}' вернул ответ!", msg);
        return msg;
    }

    /**
     * ШАГ 5: Прохождение маршрута доставки с отметками пульса (Heartbeat) и контрольными точками (Checkpoint).
     *
     * Как это работает в процессе доставки:
     * Маршрут доставки состоит из 4 контрольных участков (шаги 1..4).
     * После прохождения каждого участка вызывается `ctx.heartbeat(step)` — сигнал серверу:
     * "Доставка активна, пройдена контрольная точка №2".
     *
     * Что происходит при сбое воркера посреди маршрута:
     * 1. В начале метода вызывается `ctx.getHeartbeatDetails(Integer.class)` — проверка последней сохранённой точки.
     * 2. Если предыдущая попытка успела пройти шаг 2, новая попытка продолжит маршрут сразу с шага 2, не начиная сначала.
     */
    @Override
    public String runHeavyTaskWithHeartbeat(String taskName, int totalSteps, int stepSleepMs) {
        ActivityExecutionContext ctx = Activity.getExecutionContext();

        // Шаг А: Проверяем, есть ли сохранённая контрольная точка (чекпоинт) от предыдущей попытки
        Optional<Integer> savedStep = ctx.getHeartbeatDetails(Integer.class);
        int startStep = savedStep.orElse(1);

        if (startStep > 1) {
            log.info("🔄 [HEARTBEAT RESUME] Восстановление задачи '{}' с сохранённого чекпоинта: шаг {}/{}",
                    taskName, startStep, totalSteps);
        } else {
            log.info("🚀 [HEARTBEAT START] Старт долгой задачи '{}' ({} шагов по {} мс)",
                    taskName, totalSteps, stepSleepMs);
        }

        // Шаг Б: Проходим участки маршрута, на каждом шаге отправляя пульс и сохраняя номер текущей точки
        for (int step = startStep; step <= totalSteps; step++) {
            sleepQuietly(stepSleepMs);
            int percent = (step * 100) / totalSteps;
            String bar = buildProgressBar(step, totalSteps);

            // Передаём серверу сигнал активности и сохраняем номер текущего этапа `step`
            ctx.heartbeat(step);

            log.info("💓 [HEARTBEAT] {} {} {}% (шаг {}/{}, пульс записан)",
                    taskName, bar, percent, step, totalSteps);
        }

        log.info("✅ [HEARTBEAT COMPLETE] Долгая задача '{}' завершена на 100%!", taskName);
        return "Completed " + totalSteps + " steps with heartbeats";
    }

    /**
     * ШАГ 6: Ожидание подтверждения от внешней партнёрской системы по токену накладной (TaskToken + doNotCompleteOnReturn).
     *
     * Порядок работы:
     * 1. Получаем уникальный токен текущей операции: `byte[] taskToken = ctx.getTaskToken()`.
     * 2. Сохраняем его в реестр `tokenRegistry.saveToken(workflowId, taskToken)`.
     * 3. Вызываем `ctx.doNotCompleteOnReturn()` — это указывает Temporal НЕ завершать Activity при выходе из Java-метода.
     *    Поток воркера сразу освобождается, а шаг заказа остаётся ожидать внешнего вызова `POST /api/showcase/{workflowId}/complete-async`.
     */
    @Override
    public String waitForExternalCallback(String workflowId) {
        ActivityExecutionContext ctx = Activity.getExecutionContext();
        // 1. Получаем уникальный токен текущей задачи
        byte[] taskToken = ctx.getTaskToken();
        // 2. Сохраняем его в реестр, чтобы REST-контроллер мог завершить задачу по workflowId
        tokenRegistry.saveToken(workflowId, taskToken);

        String tokenPreview = Base64.getEncoder().encodeToString(taskToken);
        if (tokenPreview.length() > 28) {
            tokenPreview = tokenPreview.substring(0, 28) + "...";
        }

        log.info("⏸️ [ASYNC COMPLETION] Activity переведена в ожидание внешнего колбэка! WorkflowId='{}', TaskToken='{}'",
                workflowId, tokenPreview);
        log.info("👉 Для завершения этой Activity вызовите: POST http://localhost:8081/api/showcase/{}/complete-async?result=ApprovedByExternalSystem",
                workflowId);

        // 3. Не завершать Activity при выходе из метода — ждать внешнего подтверждения по TaskToken
        ctx.doNotCompleteOnReturn();
        return "IGNORED_UNTIL_EXTERNAL_CALLBACK";
    }

    // =========================================================================
    // ШАГ 7: ПРЯМЫЕ ДЕЙСТВИЯ И КОМПЕНСАЦИИ (ОТМЕНЫ) ДЛЯ ПАТТЕРНА SAGA
    // =========================================================================

    @Override
    public String sagaReserveInventory(String workflowId) {
        log.info("🟢 [SAGA STEP 1/3] Резервирование товара на складе для '{}'...", workflowId);
        sleepQuietly(500);
        log.info("📦 [SAGA STEP 1/3 OK] Склад зарезервирован (компенсация зарегистрирована).");
        return "INV-RESERVED";
    }

    @Override
    public void sagaCompensateInventory(String workflowId) {
        log.warn("⏪ [SAGA COMPENSATION] Возврат товара на склад (снятие резерва) для '{}'!", workflowId);
        sleepQuietly(400);
        log.info("✔️ [SAGA COMPENSATION OK] Резерв склада для '{}' успешно снят.", workflowId);
    }

    @Override
    public String sagaChargePayment(String workflowId, double amount) {
        log.info("🟢 [SAGA STEP 2/3] Списание оплаты ${} для '{}'...", amount, workflowId);
        sleepQuietly(500);
        log.info("💳 [SAGA STEP 2/3 OK] Сумма ${} списана (компенсация возврата средств зарегистрирована).", amount);
        return "PAY-CHARGED-" + amount;
    }

    @Override
    public void sagaRefundPayment(String workflowId, double amount) {
        log.warn("⏪ [SAGA COMPENSATION] Оформление возврата средств ${} клиенту для '{}'!", amount, workflowId);
        sleepQuietly(400);
        log.info("✔️ [SAGA COMPENSATION OK] Деньги (${}) для '{}' возвращены клиенту.", amount, workflowId);
    }

    /**
     * Финальный 3-й шаг Saga — вызов курьера.
     * Что ожидаем:
     * - Если `simulateFailure = false`: курьер назначается, и вся покупка завершается успехом!
     * - Если `simulateFailure = true`: мы специально бросаем неисправимую ошибку `CourierUnavailableException`,
     *   чтобы Workflow тут же перешёл к блоку `catch` и запустил возврат денег и разблокировку склада!
     */
    @Override
    public String sagaDispatchCourier(String workflowId, boolean simulateFailure) {
        log.info("🟢 [SAGA STEP 3/3] Назначение курьера для '{}' (simulateFailure={})...", workflowId, simulateFailure);
        sleepQuietly(500);

        if (simulateFailure) {
            log.error("💥 [SAGA STEP 3/3 FAILED] Сбой службы доставки для '{}'! Все свободные курьеры заняты. Запускаем откат SAGA!", workflowId);
            throw ApplicationFailure.newNonRetryableFailure(
                    "Нет доступных курьеров в регионе — требуется откат транзакции Saga",
                    "CourierUnavailableException"
            );
        }

        log.info("🚚 [SAGA STEP 3/3 OK] Курьер назначен для '{}'. Вся транзакция Saga успешна!", workflowId);
        return "COURIER-ASSIGNED";
    }

    @Override
    public void sagaCancelCourier(String workflowId) {
        log.warn("⏪ [SAGA COMPENSATION] Отмена заявки на курьера для '{}'!", workflowId);
        sleepQuietly(300);
    }

    /** Рисует наглядную текстовую полоску прогресса, например `[██████░░░░]`. */
    private String buildProgressBar(int current, int total) {
        int width = 10;
        int filled = (current * width) / total;
        return "[" + "█".repeat(Math.max(0, filled)) + "░".repeat(Math.max(0, width - filled)) + "]";
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Activity.wrap(e);
        }
    }
}
