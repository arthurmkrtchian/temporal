package com.example.temporal.showcase.controller;

import com.example.temporal.showcase.activity.AsyncTokenRegistry;
import com.example.temporal.showcase.dto.ShowcaseRequest;
import com.example.temporal.showcase.dto.ShowcaseState;
import com.example.temporal.showcase.workflow.ShowcaseWorkflow;
import io.temporal.api.enums.v1.ScheduleOverlapPolicy;
import io.temporal.client.*;
import io.temporal.client.schedules.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * REST-КОНТРОЛЛЕР УПРАВЛЕНИЯ РАСШИРЕННЫМИ ЗАКАЗАМИ (`/api/showcase`).
 *
 * Через эти HTTP-эндпоинты клиент или оператор взаимодействует с процессами заказов в Temporal:
 * - Запустить полный цикл обработки заказа в фоне или синхронно (`/full-run`);
 * - Проверить текущее состояние и лог заказа (@QueryMethod — `/state`);
 * - Отправить сигнал подтверждения заказа (@SignalMethod — `/signal-approve`);
 * - Изменить бюджет заказа на ходу с проверкой валидатором (@UpdateMethod — `/update-budget`);
 * - Подтвердить завершение внешней партнёрской операции по `TaskToken` (`/complete-async`);
 * - Настроить расписание регулярного автозапуска заказов (`ScheduleClient` — `/schedules/{id}`).
 */
@RestController
@RequestMapping("/api/showcase")
public class ShowcaseController {

    private final WorkflowClient workflowClient;
    private final AsyncTokenRegistry tokenRegistry;

    public ShowcaseController(WorkflowClient workflowClient, AsyncTokenRegistry tokenRegistry) {
        this.workflowClient = workflowClient;
        this.tokenRegistry = tokenRegistry;
    }

    /**
     * КНОПКА 1: Полный запуск всех возможностей песочницы (`POST /api/showcase/full-run`).
     *
     * Что происходит по шагам:
     * 1. Придумываем уникальное имя запуска, например `"showcase-a1b2c3"`.
     * 2. Если вы передали `?sync=true` — контроллер будет ждать ~12 секунд, пока все шаги пройдут до конца,
     *    и сразу вернёт финальный отчёт `ShowcaseState`.
     * 3. Но по умолчанию (`sync=false`) контроллер запускает сценарий В ФОНЕ через `WorkflowClient.start(...)`!
     *
     * Почему запуск в фоне удобнее всего?
     * Потому что сценарий сразу дойдёт до Шага 2 и встанет на 8-секундную паузу ожидания сигнала.
     * А контроллер мгновенно вернёт вам JSON с готовыми командами `curl` (шпаргалкой), чтобы вы прямо во время
     * этой паузы успели поменять бюджет (`update-budget`), прислать сигнал (`signal-approve`) и проверить статус (`state`)!
     */
    @PostMapping("/full-run")
    public ResponseEntity<?> startFullShowcase(
            @RequestParam(defaultValue = "false") boolean sync,
            @RequestBody(required = false) ShowcaseRequest customRequest
    ) {
        // Если тело запроса не передали, берём стандартные настройки "Полного успешного прогона"
        ShowcaseRequest request = (customRequest != null) ? customRequest : ShowcaseRequest.fullSuccessDefault();
        String workflowId = "showcase-" + shortId();

        ShowcaseWorkflow workflow = newWorkflowStub(workflowId);

        // Вариант А: Синхронный запуск (ждём окончания всех 7 шагов прямо в HTTP-запросе)
        if (sync) {
            ShowcaseState finalState = workflow.runShowcase(request);
            return ResponseEntity.ok(finalState);
        }

        // Вариант Б (По умолчанию): Асинхронный старт в фоне (возвращает управление мгновенно!)
        WorkflowClient.start(workflow::runShowcase, request);

        // Собираем удобную шпаргалку с готовыми командами для этого конкретного workflowId
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("workflowId", workflowId);
        response.put("status", "RUNNING_ASYNC");
        response.put("timerWindowSeconds", request.timerSeconds());
        response.put("interactiveCommands", Map.of(
                "1_queryState", "curl http://localhost:8081/api/showcase/" + workflowId + "/state",
                "2_updateBudgetPlus250", "curl -X POST 'http://localhost:8081/api/showcase/" + workflowId + "/update-budget?delta=250'",
                "3_updateBudgetInvalidNegative", "curl -X POST 'http://localhost:8081/api/showcase/" + workflowId + "/update-budget?delta=-9999'",
                "4_sendApproveSignal", "curl -X POST 'http://localhost:8081/api/showcase/" + workflowId + "/signal-approve?approver=TeamLead'"
        ));
        return ResponseEntity.accepted().body(response);
    }

    /**
     * КНОПКА 2: Учебная авария с автоматическим откатом по паттерну SAGA (`POST /api/showcase/demos/saga-rollback`).
     *
     * Что происходит:
     * Запускает сценарий с включённым флажком `triggerSagaRollback = true`.
     *
     * Что мы ожидаем увидеть в ответе:
     * 1. В поле `timelineLog` будет видно, что склад забронирован (`SAGA_1`) и $500 списаны (`SAGA_2`),
     *    но на курьере произошёл сбой (`SAGA_ERROR`).
     * 2. В поле `sagaCompensations` появятся две записи строго в ОБРАТНОМ порядке (LIFO):
     *    - Сначала `"COMPENSATED: Refunded $500.0"` (вернули деньги);
     *    - Затем `"COMPENSATED: Inventory reservation released"` (сняли бронь склада).
     * 3. Итоговый статус `currentPhase` будет равен `"ROLLED_BACK_BY_SAGA"`.
     */
    @PostMapping("/demos/saga-rollback")
    public ResponseEntity<ShowcaseState> demoSagaRollback() {
        String workflowId = "showcase-saga-" + shortId();
        ShowcaseWorkflow workflow = newWorkflowStub(workflowId);
        ShowcaseState state = workflow.runShowcase(ShowcaseRequest.sagaRollbackDefault());
        return ResponseEntity.ok(state);
    }

    /**
     * КНОПКА 3: Проверка живучести при сбоях и повторах (`POST /api/showcase/demos/retries?failures=2`).
     *
     * Что можно проверить:
     * - Передайте `?failures=2` (или `3`): сервис упадёт 2 раза, но благодаря `RetryOptions` с 3-й попытки
     *   выполнится успешно, и весь сценарий дойдёт до `"COMPLETED_SUCCESSFULLY"`!
     * - Передайте `?failures=-1`: сервис выбросит НЕПОВТОРЯЕМУЮ ошибку (`NonRetryable`). Сервер Temporal
     *   поймёт, что пробовать снова бессмысленно, сразу остановит сценарий, а мы вернём HTTP 400 с описанием ошибки.
     */
    @PostMapping("/demos/retries")
    public ResponseEntity<?> demoRetries(@RequestParam(defaultValue = "2") int failures) {
        String workflowId = "showcase-retries-" + shortId();
        ShowcaseWorkflow workflow = newWorkflowStub(workflowId);
        try {
            ShowcaseState state = workflow.runShowcase(ShowcaseRequest.retriesDefault(failures));
            return ResponseEntity.ok(state);
        } catch (WorkflowException e) {
            // Сюда мы попадаем, если передали failures=-1 (NonRetryable ошибка) или превысили лимит из 5 попыток
            return ResponseEntity.badRequest().body(Map.of(
                    "workflowId", workflowId,
                    "status", "WORKFLOW_FAILED_NON_RETRYABLE",
                    "error", e.getCause() != null ? e.getCause().getMessage() : e.getMessage()
            ));
        }
    }

    /**
     * ЭНДПОИНТ 4: Запуск заказа с ожиданием подтверждения от внешней системы по `TaskToken` (`POST /api/showcase/demos/async-callback`).
     *
     * Порядок работы:
     * 1. Заказ запускается в фоне, проходит Шаги 0..5 и останавливается на Шаге 6 (`STEP_6_WAITING_EXTERNAL_ASYNC_CALLBACK`).
     * 2. Токен задачи (`TaskToken`) сохраняется в `AsyncTokenRegistry`.
     * 3. Для завершения ожидающего шага вызовите команду из поля `completeCommand` в ответе (`POST /complete-async`).
     */
    @PostMapping("/demos/async-callback")
    public ResponseEntity<Map<String, Object>> demoAsyncCallback() {
        String workflowId = "showcase-async-" + shortId();
        ShowcaseRequest request = new ShowcaseRequest(
                "ASYNC_CALLBACK_DEMO",
                1,
                0,
                false,
                true,  // Включаем Шаг 6: ожидание внешнего завершения по TaskToken!
                false,
                300.0
        );

        ShowcaseWorkflow workflow = newWorkflowStub(workflowId);
        WorkflowClient.start(workflow::runShowcase, request);

        return ResponseEntity.accepted().body(Map.of(
                "workflowId", workflowId,
                "message", "Workflow запущен и через несколько секунд остановится на шаге STEP_6_WAITING_EXTERNAL_ASYNC_CALLBACK",
                "completeCommand", "curl -X POST 'http://localhost:8081/api/showcase/" + workflowId + "/complete-async?result=ApprovedByExternalSystem'",
                "queryStateCommand", "curl http://localhost:8081/api/showcase/" + workflowId + "/state"
        ));
    }

    /**
     * ЭНДПОИНТ 5: Проверить текущее состояние заказа (@QueryMethod — `GET /api/showcase/{workflowId}/state`).
     *
     * Мгновенно возвращает карточку `ShowcaseState` со всеми деталями заказа, не вмешиваясь в его выполнение.
     */
    @GetMapping("/{workflowId}/state")
    public ResponseEntity<ShowcaseState> queryState(@PathVariable String workflowId) {
        ShowcaseWorkflow workflow = workflowClient.newWorkflowStub(ShowcaseWorkflow.class, workflowId);
        return ResponseEntity.ok(workflow.getState());
    }

    /**
     * ЭНДПОИНТ 6: Отправить сигнал подтверждения заказа (@SignalMethod — `POST /api/showcase/{workflowId}/signal-approve`).
     *
     * Отправляет асинхронный сигнал `approveSignal(approver)` в живой заказ. Если заказ ожидал подтверждения на Шаге 2,
     * он сразу продолжит выполнение и сохранит имя подтвердившего в поле `signalApprover`.
     */
    @PostMapping("/{workflowId}/signal-approve")
    public ResponseEntity<Map<String, String>> sendApproveSignal(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "ConsoleOperator") String approver
    ) {
        ShowcaseWorkflow workflow = workflowClient.newWorkflowStub(ShowcaseWorkflow.class, workflowId);
        workflow.approveSignal(approver);
        return ResponseEntity.ok(Map.of(
                "workflowId", workflowId,
                "action", "SIGNAL_SENT",
                "approver", approver
        ));
    }

    /**
     * ЭНДПОИНТ 7: Изменить бюджет заказа на ходу с проверкой валидатором (@UpdateMethod — `POST /api/showcase/{workflowId}/update-budget?delta=250`).
     *
     * Ожидаемое поведение:
     * - При `delta = 250` (и текущем бюджете 1000): валидатор `@UpdateValidatorMethod` пропускает запрос,
     *   бюджет заказа становится `1250.0`, возвращается HTTP 200 OK с новой суммой.
     * - При `delta = -9999`: валидатор отклоняет уход бюджета в минус, выбрасывается `WorkflowUpdateException`,
     *   и контроллер возвращает HTTP 400 Bad Request с причиной отказа.
     */
    @PostMapping("/{workflowId}/update-budget")
    public ResponseEntity<?> updateBudget(
            @PathVariable String workflowId,
            @RequestParam double delta
    ) {
        ShowcaseWorkflow workflow = workflowClient.newWorkflowStub(ShowcaseWorkflow.class, workflowId);
        try {
            double updatedBudget = workflow.adjustBudgetUpdate(delta);
            return ResponseEntity.ok(Map.of(
                    "workflowId", workflowId,
                    "action", "UPDATE_APPLIED",
                    "delta", delta,
                    "newBudget", updatedBudget
            ));
        } catch (WorkflowUpdateException e) {
            return ResponseEntity.badRequest().body(Map.of(
                    "workflowId", workflowId,
                    "action", "UPDATE_REJECTED_BY_VALIDATOR",
                    "reason", e.getCause() != null ? e.getCause().getMessage() : e.getMessage()
            ));
        }
    }

    /**
     * ЭНДПОИНТ 8: Завершить ожидающую внешнюю операцию заказа по `TaskToken` (`POST /api/showcase/{workflowId}/complete-async`).
     *
     * Порядок работы:
     * 1. Извлекаем сохранённый `byte[] taskToken` для данного заказа из `tokenRegistry.consumeToken(workflowId)`.
     * 2. Вызываем `completionClient.complete(token, result)` через `ActivityCompletionClient`.
     * 3. Заказ мгновенно выходит из ожидания на Шаге 6, переходит к Шагу 7 и завершается.
     */
    @PostMapping("/{workflowId}/complete-async")
    public ResponseEntity<?> completeAsyncActivity(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "ManualCallbackOK") String result
    ) {
        Optional<byte[]> tokenOpt = tokenRegistry.consumeToken(workflowId);
        if (tokenOpt.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Нет ожидающего TaskToken для workflowId=" + workflowId,
                    "waitingWorkflows", tokenRegistry.getWaitingWorkflowIds()
            ));
        }

        // Создаём клиент для внешнего завершения Activity и передаём ему билетик (taskToken) + ответ
        ActivityCompletionClient completionClient = workflowClient.newActivityCompletionClient();
        completionClient.complete(tokenOpt.get(), result);

        return ResponseEntity.ok(Map.of(
                "workflowId", workflowId,
                "action", "ASYNC_ACTIVITY_COMPLETED_BY_TOKEN",
                "resultPassedToWorkflow", result
        ));
    }

    /**
     * КНОПКА 9: Экстренная отмена запущенного сценария (`POST /api/showcase/{workflowId}/cancel`).
     * Отправляет запрос на отмену (`untyped.cancel()`), переводя сценарий в статус `Canceled`.
     */
    @PostMapping("/{workflowId}/cancel")
    public ResponseEntity<Map<String, String>> cancelWorkflow(@PathVariable String workflowId) {
        WorkflowStub untyped = workflowClient.newUntypedWorkflowStub(workflowId);
        untyped.cancel();
        return ResponseEntity.ok(Map.of(
                "workflowId", workflowId,
                "action", "CANCELLATION_REQUESTED"
        ));
    }

    /**
     * КНОПКА 10: Завести серверное расписание автозапуска (`POST /api/showcase/schedules/{scheduleId}?intervalSeconds=30`).
     *
     * Простая аналогия:
     * Мы просим сервер Temporal: "Каждые 30 секунд сам нажимай кнопку старта `ShowcaseWorkflow`!".
     * Даже если мы закроем терминал, сервер Temporal по часам будет создавать новые запуски.
     *
     * Что означает `SCHEDULE_OVERLAP_POLICY_SKIP`:
     * "Если прошлый запуск почему-то ещё не закончился, а уже подошло время нового — пропусти новый запуск,
     *  чтобы они не толкались локтями друг с другом!"
     */
    @PostMapping("/schedules/{scheduleId}")
    public ResponseEntity<Map<String, Object>> createSchedule(
            @PathVariable String scheduleId,
            @RequestParam(defaultValue = "30") int intervalSeconds
    ) {
        ScheduleClient scheduleClient = ScheduleClient.newInstance(workflowClient.getWorkflowServiceStubs(),
                ScheduleClientOptions.newBuilder().setNamespace(workflowClient.getOptions().getNamespace()).build());

        // 1. Описываем ЧТО именно нужно запускать по будильнику (наш ShowcaseWorkflow)
        ScheduleActionStartWorkflow action = ScheduleActionStartWorkflow.newBuilder()
                .setWorkflowType(ShowcaseWorkflow.class)
                .setArguments(new ShowcaseRequest("SCHEDULED_RUN", 1, 1, false, false, false, 100.0))
                .setOptions(WorkflowOptions.newBuilder()
                        .setWorkflowId("scheduled-" + scheduleId)
                        .setTaskQueue(ShowcaseWorkflow.TASK_QUEUE)
                        .build())
                .build();

        // 2. Описываем КОГДА запускать (каждые intervalSeconds секунд) и правило защиты от наложений (SKIP)
        Schedule schedule = Schedule.newBuilder()
                .setAction(action)
                .setSpec(ScheduleSpec.newBuilder()
                        .setIntervals(List.of(new ScheduleIntervalSpec(Duration.ofSeconds(intervalSeconds))))
                        .build())
                .setPolicy(SchedulePolicy.newBuilder()
                        .setOverlap(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP)
                        .build())
                .build();

        scheduleClient.createSchedule(scheduleId, schedule, ScheduleOptions.newBuilder().build());

        return ResponseEntity.ok(Map.of(
                "scheduleId", scheduleId,
                "intervalSeconds", intervalSeconds,
                "overlapPolicy", "SKIP",
                "deleteCommand", "curl -X DELETE http://localhost:8081/api/showcase/schedules/" + scheduleId
        ));
    }

    /**
     * КНОПКА 11: Выключить и удалить расписание (`DELETE /api/showcase/schedules/{scheduleId}`).
     */
    @DeleteMapping("/schedules/{scheduleId}")
    public ResponseEntity<Map<String, String>> deleteSchedule(@PathVariable String scheduleId) {
        ScheduleClient scheduleClient = ScheduleClient.newInstance(workflowClient.getWorkflowServiceStubs(),
                ScheduleClientOptions.newBuilder().setNamespace(workflowClient.getOptions().getNamespace()).build());
        ScheduleHandle handle = scheduleClient.getHandle(scheduleId);
        handle.delete();
        return ResponseEntity.ok(Map.of(
                "scheduleId", scheduleId,
                "status", "SCHEDULE_DELETED"
        ));
    }

    /**
     * Вспомогательный метод: создаёт пульт управления новым Workflow, прикрепляя к нему очередь `ShowcaseTaskQueue`
     * и памятную записку `Memo` (видна в веб-интерфейсе Temporal UI).
     */
    private ShowcaseWorkflow newWorkflowStub(String workflowId) {
        return workflowClient.newWorkflowStub(
                ShowcaseWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId(workflowId)
                        .setTaskQueue(ShowcaseWorkflow.TASK_QUEUE)
                        .setMemo(Collections.singletonMap("description", "Temporal Kitchen-Sink Showcase"))
                        .build()
        );
    }

    private String shortId() {
        return UUID.randomUUID().toString().substring(0, 6);
    }
}
