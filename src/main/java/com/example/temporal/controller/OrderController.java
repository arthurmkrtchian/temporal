package com.example.temporal.controller;

import com.example.temporal.dto.OrderRequest;
import com.example.temporal.dto.OrderResponse;
import com.example.temporal.workflow.OrderWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * ВХОДНЫЕ ВОРОТА ДЛЯ ПОЛЬЗОВАТЕЛЯ (REST Controller базового примера заказов).
 *
 * Простая аналогия:
 * Это "Касса приёма заказов". Покупатель присылает сюда HTTP-запрос, а кассир (этот контроллер)
 * берёт телефон (WorkflowClient) и звонит на сервер Temporal: "Эй, запусти сценарий для заказа №101!".
 *
 * Заметь: сам контроллер НЕ списывает деньги и НЕ шлёт письма. Он только передаёт заявку в Temporal!
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    /**
     * Главный пульт связи с сервером Temporal (на порту 7233).
     * Spring Boot создаёт и настраивает его для нас автоматически.
     */
    private final WorkflowClient workflowClient;

    public OrderController(WorkflowClient workflowClient) {
        this.workflowClient = workflowClient;
    }

    /**
     * ВАРИАНТ 1: СИНХРОННЫЙ ЗАПУСК ЗАКАЗА (Ждём у кассы, пока всё не приготовится).
     *
     * Что происходит по шагам:
     * 1. Мы создаём "пульт управления" (WorkflowStub) и задаём процессу уникальное имя: "order-" + orderId.
     *    (Если прямо сейчас уже выполняется заказ с таким же именем, Temporal не даст случайно запустить дубликат!).
     * 2. Мы вызываем `workflow.processOrder(request)` как обычный Java-метод.
     * 3. На самом деле запрос улетает на сервер Temporal, проходит через очередь `OrderTaskQueue`,
     *    выполняет Шаг 1 (оплата) и Шаг 2 (письмо) на Работнике (Worker).
     * 4. Только когда весь сценарий дошёл до конца, мы возвращаем покупателю готовый JSON-ответ (HTTP 200 OK).
     */
    @PostMapping("/sync")
    public ResponseEntity<OrderResponse> processOrderSync(@RequestBody OrderRequest request) {
        // Настраиваем паспорт будущего процесса: его уникальное имя (WorkflowId) и очередь задач (TaskQueue)
        OrderWorkflow workflow = workflowClient.newWorkflowStub(
                OrderWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId("order-" + request.orderId())
                        .setTaskQueue(OrderWorkflow.TASK_QUEUE)
                        .build()
        );

        // Запускаем и БЛОКИРУЕМСЯ (ждём), пока сценарий полностью не выполнится от начала до конца
        OrderResponse response = workflow.processOrder(request);
        return ResponseEntity.ok(response);
    }

    /**
     * ВАРИАНТ 2: АСИНХРОННЫЙ ЗАПУСК ЗАКАЗА (Отдали заказ, получили номерок и сразу пошли по делам).
     *
     * Чем отличается от /sync:
     * Вместо прямого вызова `workflow.processOrder(request)` мы используем `WorkflowClient.start(...)`.
     *
     * Что мы ожидаем:
     * 1. Как только сервер Temporal записал в блокнот "Заказ принят в работу" (WorkflowExecutionStarted),
     *    контроллер МГНОВЕННО (за пару миллисекунд) возвращает клиенту HTTP 202 Accepted с номерком `workflowId`.
     * 2. А сам заказ тем временем спокойно выполняется в фоновом режиме (хоть 5 секунд, хоть 5 дней!).
     */
    @PostMapping("/async")
    public ResponseEntity<Map<String, String>> processOrderAsync(@RequestBody OrderRequest request) {
        OrderWorkflow workflow = workflowClient.newWorkflowStub(
                OrderWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId("order-" + request.orderId())
                        .setTaskQueue(OrderWorkflow.TASK_QUEUE)
                        .build()
        );

        // Запускаем сценарий в фоновом режиме, НЕ дожидаясь его окончания!
        WorkflowClient.start(workflow::processOrder, request);

        return ResponseEntity.accepted().body(Map.of(
                "orderId", request.orderId(),
                "workflowId", "order-" + request.orderId(),
                "message", "Order workflow started asynchronously"
        ));
    }

    /**
     * ПРОВЕРКА СТАТУСА ЗАКАЗА (@QueryMethod).
     *
     * Что происходит:
     * По номеру заказа мы находим уже запущенный (или даже завершённый!) процесс на сервере Temporal
     * и вызываем его метод `getStatus()`.
     *
     * Что мы ожидаем:
     * Нам мгновенно вернётся текущая фаза заказа ("PROCESSING_PAYMENT", "SENDING_CONFIRMATION" или "COMPLETED").
     */
    @GetMapping("/{orderId}/status")
    public ResponseEntity<Map<String, String>> getOrderStatus(@PathVariable String orderId) {
        String workflowId = "order-" + orderId;
        // Подключаемся к уже существующему процессу по его уникальному имени workflowId
        OrderWorkflow workflow = workflowClient.newWorkflowStub(OrderWorkflow.class, workflowId);
        String status = workflow.getStatus();

        return ResponseEntity.ok(Map.of(
                "orderId", orderId,
                "workflowId", workflowId,
                "status", status
        ));
    }
}
