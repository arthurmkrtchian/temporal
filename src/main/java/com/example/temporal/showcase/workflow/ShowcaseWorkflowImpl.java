package com.example.temporal.showcase.workflow;

import com.example.temporal.showcase.activity.ShowcaseActivity;
import com.example.temporal.showcase.dto.ShowcaseRequest;
import com.example.temporal.showcase.dto.ShowcaseState;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.common.RetryOptions;
import io.temporal.failure.TemporalFailure;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.*;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * ГЛАВНЫЙ КООРДИНАТОР РАСШИРЕННОГО ЗАКАЗА (ShowcaseWorkflowImpl).
 *
 * Роль в системе заказов:
 * Метод `runShowcase(...)` проводит заказ через все 8 этапов жизненного цикла (Шаги 0..7):
 * от первичной проверки и ожидания подтверждения до оплаты с ретраями, тендера служб доставки,
 * отслеживания маршрута отгрузки и финальной распределённой транзакции Saga.
 *
 * ПОЧЕМУ ПРОЦЕСС ЗАКАЗА НЕ БОИТСЯ ПЕРЕЗАГРУЗОК СЕРВЕРА (Replay):
 * Внутри этого класса хранятся поля состояния заказа (`currentBudget`, `approvedBySignal`, `timelineLog`).
 * Если посреди Шага 4 сервер приложения перезагрузится:
 * 1. Temporal заново создаст объект заказа и вызовет `runShowcase` с первой строчки (воспроизведение по журналу — Replay).
 * 2. На всех шагах (Activities и таймерах), которые УЖЕ были завершены до сбоя, Temporal НЕ выполняет операции заново,
 *    а мгновенно подставляет сохранённые результаты из базы данных (Event History).
 * 3. Специальный логгер `Workflow.getLogger(...)` во время Replay не дублирует старые сообщения в консоль,
 *    а начинает писать логи только на новых шагах.
 */
@WorkflowImpl(taskQueues = ShowcaseWorkflow.TASK_QUEUE)
public class ShowcaseWorkflowImpl implements ShowcaseWorkflow {

    /**
     * Логгер Temporal с поддержкой Replay: не дублирует записи при восстановлении состояния заказа из журнала.
     */
    private static final Logger log = Workflow.getLogger(ShowcaseWorkflowImpl.class);

    // =========================================================================
    // СОСТОЯНИЕ ЗАКАЗА (Сохраняется между шагами благодаря Event History)
    // =========================================================================
    private String workflowId = "";
    private String scenarioName = "";
    private String currentPhase = "CREATED";
    private double currentBudget = 0.0;
    private boolean approvedBySignal = false;
    private String signalApprover = "NONE";
    private String raceWinnerProvider = "NONE";
    private String childWorkflowResult = "NONE";
    private String asyncCallbackResult = "SKIPPED";

    // Пошаговый журнал всех пройденных этапов заказа
    private final List<String> timelineLog = new ArrayList<>();
    // Журнал выполненных возвратов (компенсаций) при откате заказа по паттерну Saga
    private final List<String> sagaCompensations = new ArrayList<>();

    // =========================================================================
    // КЛИЕНТЫ ВНЕШНИХ ОПЕРАЦИЙ ЗАКАЗА (Activity Stubs) И ИХ РЕГЛАМЕНТЫ (ActivityOptions)
    // =========================================================================

    /**
     * РЕЖИМ №1: Локальная операция в памяти воркера (Local Activity Stub + LocalActivityOptions).
     *
     * Зачем используется в заказе:
     * Для мгновенной первичной валидации параметров заказа прямо в памяти текущего воркера (за ~1 мс),
     * без постановки задачи в общую очередь сервера Temporal. Подходит только для быстрых операций (таймаут 5 сек).
     */
    private final ShowcaseActivity localActivities = Workflow.newLocalActivityStub(
            ShowcaseActivity.class,
            LocalActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(5))
                    .build()
    );

    /**
     * РЕЖИМ №2: Стандартные внешние операции заказа с автоповторами (ActivityOptions + RetryOptions).
     *
     * Зачем используется в заказе:
     * Для обращений во внешние сервисы (платёжный шлюз банка, складской учёт, курьерские службы).
     *
     * Регламент выполнения (ActivityOptions):
     * - StartToCloseTimeout (30 сек): максимальное время на одну попытку выполнения операции.
     * - HeartbeatTimeout (5 сек): для долгих операций исполнитель обязан присылать сигнал прогресса минимум раз в 5 секунд.
     * - RetryOptions (Правила автоматических повторов при сбоях):
     *   1) После 1-го сбоя ждём 1 секунду (`InitialInterval = 1s`) и повторяем запрос.
     *   2) С каждым новым сбоем увеличиваем паузу в 1.5 раза (`BackoffCoefficient = 1.5`: 1с -> 1.5с -> 2.25с...).
     *   3) Максимальная пауза между попытками — 5 секунд (`MaximumInterval = 5s`).
     *   4) Всего до 5 попыток (`MaximumAttempts = 5`). Ошибка вернётся в Workflow только если все 5 попыток упали.
     */
    private final ShowcaseActivity standardActivities = Workflow.newActivityStub(
            ShowcaseActivity.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setHeartbeatTimeout(Duration.ofSeconds(5))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setBackoffCoefficient(1.5)
                            .setMaximumInterval(Duration.ofSeconds(5))
                            .setMaximumAttempts(5)
                            .build())
                    .build()
    );

    /**
     * РЕЖИМ №3: Операция с долгим ожиданием внешней партнёрской системы по TaskToken (ActivityOptions).
     *
     * Здесь задан увеличенный таймаут `StartToCloseTimeout = 10 минут`, чтобы внешняя система (или оператор через `curl`)
     * успела прислать HTTP-подтверждение по сохранённому `TaskToken`.
     */
    private final ShowcaseActivity asyncCallbackActivities = Workflow.newActivityStub(
            ShowcaseActivity.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofMinutes(10))
                    .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                    .build()
    );

    /**
     * =========================================================================
     * ГЛАВНЫЙ МАРШРУТ СЦЕНАРИЯ (Выполняется шаг за шагом сверху вниз)
     * =========================================================================
     */
    @Override
    public ShowcaseState runShowcase(ShowcaseRequest request) {
        // Запоминаем наш уникальный номер (например, "showcase-a1b2c3") и стартовые настройки
        this.workflowId = Workflow.getInfo().getWorkflowId();
        this.scenarioName = request.scenarioName();
        this.currentBudget = request.initialBudget();

        log.info("================================================================================");
        log.info("🎬 [WORKFLOW START] Запущен '{}' (сценарий: '{}', бюджет: ${})",
                workflowId, scenarioName, currentBudget);
        log.info("================================================================================");
        recordStep("START: Workflow запущен (бюджет $" + currentBudget + ")");

        // -------------------------------------------------------------------------
        // ШАГ 0: ВЕРСИОНИРОВАНИЕ КОДА (Workflow.getVersion)
        // -------------------------------------------------------------------------
        // Зачем это нужно:
        // Представь, что у тебя запущено 1000 заказов, которые длятся месяц. На 15-й день ты решил поменять
        // код сценария. Чтобы старые незаконченные заказы не сломались при "пересказе" (Replay),
        // `Workflow.getVersion` ставит в дневник пометку: "Этот заказ шёл по версии №1".
        // Ожидаем: в лог шагов запишется v=1.
        int version = Workflow.getVersion("showcase-flow-v1", Workflow.DEFAULT_VERSION, 1);
        recordStep("VERSIONING: Проверена версия кода workflow (v=" + version + ")");

        // -------------------------------------------------------------------------
        // ШАГ 1: БЫСТРАЯ ЛОКАЛЬНАЯ ПРОВЕРКА (Local Activity)
        // -------------------------------------------------------------------------
        // Что за чем: вызываем `fastLocalCheck` через пульт `localActivities`.
        // Что ожидаем: метод выполнится мгновенно прямо в памяти нашего воркера, без постановки в общую очередь,
        // и вернёт строку "LocalCheckOK:<имя_сценария>".
        currentPhase = "STEP_1_LOCAL_ACTIVITY";
        String localRes = localActivities.fastLocalCheck(scenarioName);
        recordStep("LOCAL_ACTIVITY: " + localRes);

        // -------------------------------------------------------------------------
        // ШАГ 2: НЕУБИВАЕМЫЙ БУДИЛЬНИК И ОЖИДАНИЕ СИГНАЛА (Workflow.await / Workflow.sleep)
        // -------------------------------------------------------------------------
        // Что здесь происходит:
        // Если включён режим `waitForSignal = true`, наш сценарий говорит серверу Temporal:
        // "Разбуди меня через N секунд ИЛИ раньше, если кто-нибудь пришлёт сигнал approveSignal (сделает approvedBySignal = true)!"
        //
        // Пока идут эти секунды:
        // 1) Сценарий "спит" на сервере. Никакой поток Java на нашем компьютере не заблокирован!
        // 2) В это окно времени вы можете прислать @UpdateMethod (поменять бюджет) или @SignalMethod (подтвердить досрочно).
        currentPhase = "STEP_2_WAITING_SIGNAL_OR_TIMER";
        if (request.waitForSignal()) {
            log.info("⏳ [AWAIT + TIMER] Ждём сигнал 'approveSignal' до {} сек... (Сейчас можно вызвать @UpdateMethod или @SignalMethod!)",
                    request.timerSeconds());

            // Ждём: кто сработает первым — условие `() -> approvedBySignal` (вернёт true) или таймер (вернёт false)?
            boolean receivedInTime = Workflow.await(
                    Duration.ofSeconds(Math.max(1, request.timerSeconds())),
                    () -> approvedBySignal
            );

            if (receivedInTime) {
                // Ожидаемый результат №1: вы успели прислать сигнал! Таймер тут же отменяется, идём дальше досрочно.
                log.info("🔔 [SIGNAL RECEIVED] Ожидание прервано досрочно! Подтверждено пользователем: '{}'", signalApprover);
                recordStep("SIGNAL: Досрочно подтверждено сигналом от '" + signalApprover + "'");
            } else {
                // Ожидаемый результат №2: никто не прислал сигнал за отведённое время. Будильник прозвенел, идём дальше сами!
                log.info("⏰ [TIMER EXPIRED] Сигнал не пришёл за {} сек — сработал серверный таймер, продолжаем автоматически!",
                        request.timerSeconds());
                recordStep("TIMER: Таймаут ожидания сигнала (" + request.timerSeconds() + " сек) истёк, авто-продолжение");
            }
        } else if (request.timerSeconds() > 0) {
            // Если сигнала ждать не нужно, просто ставим обычный серверный будильник на N секунд
            log.info("⏰ [DURABLE TIMER] Серверный сон Workflow.sleep({} сек)...", request.timerSeconds());
            Workflow.sleep(Duration.ofSeconds(request.timerSeconds()));
            recordStep("TIMER: Серверный таймер " + request.timerSeconds() + " сек отработал");
        }

        // -------------------------------------------------------------------------
        // ШАГ 3: АВТОМАТИЧЕСКИЕ ПОВТОРЫ ПРИ СБОЯХ (RetryOptions)
        // -------------------------------------------------------------------------
        // Что за чем:
        // Мы вызываем внешний сервис `callFlakyExternalService` ОДИН РАЗ в коде Workflow!
        // Но передаём ему параметр `flakyFailuresBeforeSuccess` (по умолчанию = 2).
        //
        // Что мы ожидаем увидеть за кулисами:
        // - Попытка #1 в Activity упадёт с ошибкой -> сервер Temporal подождёт 1 сек и сам вызовет её снова.
        // - Попытка #2 в Activity снова упадёт -> сервер подождёт 1.5 сек и вызовет в 3-й раз.
        // - Попытка #3 пройдёт успешно! И наш Workflow получит готовый ответ "Успешно выполнено с попытки #3",
        //   даже не заметив, что по дороге были две аварии!
        // (А если передать -1, Activity бросит NonRetryable-ошибку, и сценарий мгновенно остановится без повторов).
        currentPhase = "STEP_3_FLAKY_ACTIVITY_RETRIES";
        String retryRes = standardActivities.callFlakyExternalService(
                "PaymentGatewayCheck",
                request.flakyFailuresBeforeSuccess()
        );
        recordStep("RETRIES: " + retryRes);

        // -------------------------------------------------------------------------
        // ШАГ 4: ПАРАЛЛЕЛЬНАЯ ГОНКА 3 КУРЬЕРСКИХ СЛУЖБ И ОТМЕНА ПРОИГРАВШИХ (Async + Promise.anyOf + CancellationScope)
        // -------------------------------------------------------------------------
        // Подробное описание смотри внутри метода `runParallelAndRaceStep()` ниже.
        // Коротко что ожидаем: победит самый быстрый провайдер "FastExpress (700 мс)", а два медленных будут отменены.
        currentPhase = "STEP_4_PARALLEL_AND_RACE";
        runParallelAndRaceStep();

        // -------------------------------------------------------------------------
        // ШАГ 5: ЗАПУСК ДОЧЕРНЕГО СЦЕНАРИЯ (Child Workflow) С ПРОГРЕСС-БАРОМ ПУЛЬСА (Heartbeat)
        // -------------------------------------------------------------------------
        // Что за чем:
        // 1. Создаём пульт управления дочерним сценарием `DeliveryChildWorkflow`.
        // 2. Указываем правило `PARENT_CLOSE_POLICY_TERMINATE`: если вдруг родительский сценарий убьют,
        //    ребёнок тоже должен автоматически закрыться, чтобы не остаться беспризорным.
        // 3. Вызываем `child.executeSubProcess(workflowId, 4)` и ждём, пока ребёнок пройдёт все 4 шага конвейера.
        currentPhase = "STEP_5_CHILD_WORKFLOW_AND_HEARTBEAT";
        DeliveryChildWorkflow child = Workflow.newChildWorkflowStub(
                DeliveryChildWorkflow.class,
                ChildWorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId + "-child-delivery")
                        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_TERMINATE)
                        .build()
        );
        this.childWorkflowResult = child.executeSubProcess(workflowId, 4);
        recordStep("CHILD_WORKFLOW: " + childWorkflowResult);

        // -------------------------------------------------------------------------
        // ШАГ 6 (Опциональный): ОЖИДАНИЕ ВНЕШНЕГО ПОДТВЕРЖДЕНИЯ ПО "НОМЕРКУ" (Async Activity Completion)
        // -------------------------------------------------------------------------
        // Когда включается: если в запросе передано `waitForAsyncCallback = true` (например, в демо `/demos/async-callback`).
        // Что происходит:
        // Activity выдаёт секретный номерок `TaskToken`, сохраняет его в `AsyncTokenRegistry` и ставит сценарий на паузу.
        // Что мы ожидаем: сценарий будет терпеливо ждать на этом шаге, пока вы не вызовете REST-запрос `/complete-async`,
        // после чего переданная вами строка запишется в `asyncCallbackResult`, и сценарий пойдёт к финальному Шагу 7!
        if (request.waitForAsyncCallback()) {
            currentPhase = "STEP_6_WAITING_EXTERNAL_ASYNC_CALLBACK";
            this.asyncCallbackResult = asyncCallbackActivities.waitForExternalCallback(workflowId);
            log.info("🎉 [ASYNC CALLBACK COMPLETED] Получен внешний ответ по TaskToken: '{}'", asyncCallbackResult);
            recordStep("ASYNC_COMPLETION: Завершено внешним колбэком -> " + asyncCallbackResult);
        }

        // -------------------------------------------------------------------------
        // ШАГ 7: РАСПРЕДЕЛЁННАЯ ТРАНЗАКЦИЯ ПО ПАТТЕРНУ SAGA ("Умная кнопка Ctrl+Z")
        // -------------------------------------------------------------------------
        // Подробное описание смотри внутри метода `runSagaStep(...)` ниже.
        // Что ожидаем:
        // - Если `triggerSagaRollback = false` -> склад, оплата и курьер проходят успешно, статус "COMPLETED_SUCCESSFULLY".
        // - Если `triggerSagaRollback = true` -> курьер падает с ошибкой, и Saga автоматически в обратном порядке
        //   возвращает деньги и освобождает склад, переводя статус в "ROLLED_BACK_BY_SAGA".
        currentPhase = "STEP_7_SAGA_TRANSACTION";
        runSagaStep(request.triggerSagaRollback());

        log.info("================================================================================");
        log.info("🏁 [WORKFLOW FINISHED] '{}' завершён в статусе '{}'! Итоговый бюджет: ${}",
                workflowId, currentPhase, currentBudget);
        log.info("================================================================================");
        return getState();
    }

    /**
     * ДЕТАЛИ ШАГА 4: Параллельный тендер 3 курьерских служб и отмена проигравших запросов.
     *
     * Мы одновременно запрашиваем расчёт доставки в 3 курьерских службах:
     * 1) "FastExpress" — отвечает за 700 миллисекунд.
     * 2) "CityPost"    — отвечает за 1800 миллисекунд.
     * 3) "SlowCargo"   — отвечает за 3000 миллисекунд.
     *
     * Порядок выполнения:
     * Шаг А. Помещаем все 3 асинхронных вызова (`Async.function`) внутрь одной группы отмены (`CancellationScope`).
     *        Благодаря `Async.function` запросы отправляются параллельно, а не последовательно.
     * Шаг Б. Через `Promise.anyOf(providerPromises).get()` дожидаемся первой ответившей службы доставки.
     * Шаг В. Как только через 700 мс "FastExpress" возвращает результат, фиксируем её победителем тендера.
     * Шаг Г. Вызываем `raceScope.cancel(...)`, чтобы немедленно отменить оставшиеся запросы к "CityPost" и "SlowCargo".
     */
    private void runParallelAndRaceStep() {
        log.info("⚡ [PARALLEL + RACE] Запускаем гонку 3 провайдеров доставки (Promise.anyOf + CancellationScope)...");

        final List<Promise<String>> providerPromises = new ArrayList<>();

        // Упаковываем запуск 3 параллельных задач в общую коробку CancellationScope
        CancellationScope raceScope = Workflow.newCancellationScope(() -> {
            providerPromises.add(Async.function(standardActivities::requestProviderQuote, "FastExpress", 700));
            providerPromises.add(Async.function(standardActivities::requestProviderQuote, "CityPost", 1800));
            providerPromises.add(Async.function(standardActivities::requestProviderQuote, "SlowCargo", 3000));
        });

        // Нажимаем "Старт!" — все 3 задачи полетели выполняться одновременно
        raceScope.run();

        // Ждём самую первую задачу, которая успеет вернуть результат (ожидаем FastExpress за ~700 мс)
        Promise<String> winner = Promise.anyOf(providerPromises);
        this.raceWinnerProvider = winner.get();
        log.info("🥇 [RACE WINNER] Победил провайдер: '{}'! Отменяем остальных провайдеров через CancellationScope.cancel()...",
                raceWinnerProvider);

        // Даём команду отбоя всем остальным задачам внутри коробки raceScope
        raceScope.cancel("Другой провайдер ответил быстрее");
        recordStep("RACE_ANY_OF: Победил " + raceWinnerProvider + " (остальные отменены)");
    }

    /**
     * ДЕТАЛИ ШАГА 7: Как устроен паттерн SAGA (Распределённая транзакция заказа с автоматическим откатом).
     *
     * Оформление заказа включает 3 шага в разных системах:
     * 1) Зарезервировать товар на складе.
     * 2) Списать деньги с карты покупателя.
     * 3) Назначить курьера для выезда.
     *
     * Правило Saga:
     * Сразу после каждого успешного шага мы регистрируем в `saga.addCompensation(...)` компенсирующее действие
     * на случай, если на последующих шагах произойдёт сбой.
     * Настройка `setParallelCompensation(false)` гарантирует выполнение возвратов строго в обратном порядке (LIFO).
     *
     * Что происходит при отказе курьерской службы на 3-м шаге (`simulateFailure = true`):
     * 1. Перехватывается исключение `TemporalFailure`.
     * 2. Вызывается `saga.compensate()`.
     * 3. ПЕРВЫМ выполняется возврат денег покупателю (компенсация Шага 2).
     * 4. ВТОРЫМ выполняется снятие резерва товара на складе (компенсация Шага 1).
     */
    private void runSagaStep(boolean simulateFailure) {
        Saga saga = new Saga(new Saga.Options.Builder()
                .setParallelCompensation(false) // Компенсируем строго в обратном порядке (Сначала Шаг 2 -> Потом Шаг 1)
                .build());

        try {
            // --- Поступенька 1: Бронируем склад ---
            standardActivities.sagaReserveInventory(workflowId);
            // Сразу записываем в блокнот Saga: "Если дальше случится беда — вызови sagaCompensateInventory!"
            saga.addCompensation(() -> {
                standardActivities.sagaCompensateInventory(workflowId);
                sagaCompensations.add("COMPENSATED: Inventory reservation released");
            });
            recordStep("SAGA_1: Склад зарезервирован");

            // --- Поступенька 2: Списываем деньги ---
            standardActivities.sagaChargePayment(workflowId, currentBudget);
            // Сразу записываем поверх первой записи: "Если дальше случится беда — сначала верни деньги (sagaRefundPayment)!"
            saga.addCompensation(() -> {
                standardActivities.sagaRefundPayment(workflowId, currentBudget);
                sagaCompensations.add("COMPENSATED: Refunded $" + currentBudget);
            });
            recordStep("SAGA_2: Списана оплата $" + currentBudget);

            // --- Поступенька 3: Назначаем курьера (здесь может произойти учебная авария!) ---
            standardActivities.sagaDispatchCourier(workflowId, simulateFailure);
            saga.addCompensation(() -> {
                standardActivities.sagaCancelCourier(workflowId);
                sagaCompensations.add("COMPENSATED: Courier canceled");
            });
            recordStep("SAGA_3: Курьер успешно назначен");

            // Если все 3 ступеньки прошли без ошибок — ставим победный статус!
            currentPhase = "COMPLETED_SUCCESSFULLY";
        } catch (TemporalFailure e) {
            // Мы попали сюда, потому что на 3-м шаге курьер выбросил ошибку!
            currentPhase = "COMPENSATING_SAGA";
            log.warn("⚠️ [SAGA ROLLBACK] Ошибка на шаге Saga ({}). Запускаем обратную цепочку компенсаций...",
                    e.getMessage());
            recordStep("SAGA_ERROR: Сбой на 3-м шаге! Запуск saga.compensate() в обратном порядке...");

            // Почему мы оборачиваем saga.compensate() в newDetachedCancellationScope?
            // Даже если сам Workflow был принудительно отменён снаружи (cancel), "отвязанная коробка" (Detached Scope)
            // гарантирует, что возврат денег и освобождение склада всё равно обязательно доведутся до конца!
            CancellationScope detached = Workflow.newDetachedCancellationScope(saga::compensate);
            detached.run();

            currentPhase = "ROLLED_BACK_BY_SAGA";
            recordStep("SAGA_ROLLED_BACK: Все предыдущие шаги успешно откачены (" + sagaCompensations.size() + " компенсации)");
        }
    }

    /**
     * ОБРАБОТЧИК ВХОДЯЩЕГО СИГНАЛА ПОДТВЕРЖДЕНИЯ ЗАКАЗА (@SignalMethod).
     * Вызывается при запросе POST /api/showcase/{id}/signal-approve.
     * Переключает флаг `approvedBySignal = true`, благодаря чему `Workflow.await(...)` на Шаге 2
     * досрочно завершает ожидание и переводит заказ к следующему шагу.
     */
    @Override
    public void approveSignal(String approverName) {
        log.info("📨 [SIGNAL HANDLER] Получен сигнал approveSignal от '{}'", approverName);
        this.approvedBySignal = true;
        this.signalApprover = (approverName == null || approverName.isBlank()) ? "AnonymousOperator" : approverName;
    }

    /**
     * ОБРАБОТЧИК СИНХРОННОГО ОБНОВЛЕНИЯ БЮДЖЕТА ЗАКАЗА (@UpdateMethod).
     * Выполняется после успешной проверки в `validateBudgetUpdate`: изменяет текущий бюджет заказа на `delta`,
     * фиксирует запись в журнале шагов и возвращает обновлённую сумму.
     */
    @Override
    public double adjustBudgetUpdate(double delta) {
        double old = this.currentBudget;
        this.currentBudget += delta;
        log.info("🔄 [UPDATE HANDLER] Бюджет синхронно изменён на лету: ${} -> ${} (delta: {})",
                old, this.currentBudget, delta);
        recordStep("UPDATE: Бюджет изменён с $" + old + " до $" + this.currentBudget);
        return this.currentBudget;
    }

    /**
     * ВАЛИДАТОР ОБНОВЛЕНИЯ БЮДЖЕТА ЗАКАЗА (@UpdateValidatorMethod).
     * Проверяет, не станет ли бюджет заказа отрицательным после прибавления `delta`.
     * Если `currentBudget + delta < 0`, выбрасывает `IllegalArgumentException` и отклоняет изменение до записи в историю.
     */
    @Override
    public void validateBudgetUpdate(double delta) {
        if (this.currentBudget + delta < 0) {
            throw new IllegalArgumentException(
                    "Отказ валидатора @UpdateValidatorMethod: итоговый бюджет не может быть отрицательным (текущий: "
                            + this.currentBudget + ", изменение: " + delta + ")"
            );
        }
    }

    /**
     * ОБРАБОТЧИК ЗАПРОСА СОСТОЯНИЯ (@QueryMethod).
     * Собирает все текущие поля в неизменяемый объект `ShowcaseState` и отдаёт наружу.
     */
    @Override
    public ShowcaseState getState() {
        return new ShowcaseState(
                workflowId,
                scenarioName,
                currentPhase,
                currentBudget,
                approvedBySignal,
                signalApprover,
                raceWinnerProvider,
                childWorkflowResult,
                asyncCallbackResult,
                new ArrayList<>(timelineLog),
                new ArrayList<>(sagaCompensations)
        );
    }

    private void recordStep(String entry) {
        timelineLog.add(entry);
    }
}
