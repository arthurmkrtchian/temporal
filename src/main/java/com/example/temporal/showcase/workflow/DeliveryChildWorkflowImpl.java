package com.example.temporal.showcase.workflow;

import com.example.temporal.showcase.activity.ShowcaseActivity;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;

import java.time.Duration;

/**
 * РЕАЛИЗАЦИЯ ДОЧЕРНЕГО СЦЕНАРИЯ (Child Workflow Implementation).
 *
 * Когда запускается этот класс:
 * Главный сценарий (ShowcaseWorkflowImpl) на Шаге 5 создаёт ChildWorkflowStub и вызывает executeSubProcess(...).
 *
 * Что делает этот дочерний сценарий по шагам:
 * 1. Просыпается под своим собственным именем: "<имя_родителя>-child-delivery".
 * 2. Ждёт 1 секунду через серверный таймер Workflow.sleep(1s).
 * 3. Запускает долгую задачу `runHeavyTaskWithHeartbeat(...)`, которая каждые 600 мс рисует в консоли
 *    прогресс-бар `[█████░░░░░]` и отправляет серверу "пульс" (Heartbeat) с номером пройденного шага.
 * 4. Возвращает отчёт родителю, после чего родитель продолжает свой Шаг 6.
 */
@WorkflowImpl(taskQueues = ShowcaseWorkflow.TASK_QUEUE)
public class DeliveryChildWorkflowImpl implements DeliveryChildWorkflow {

    private static final Logger log = Workflow.getLogger(DeliveryChildWorkflowImpl.class);

    /**
     * Настраиваем пульт вызова Activity специально для долгих задач с пульсом (Heartbeat):
     * - StartToCloseTimeout (60 сек): вся задача целиком может идти до минуты.
     * - HeartbeatTimeout (5 сек): ВНИМАНИЕ! Это значит, что пока задача работает, она ОБЯЗАНА хотя бы раз
     *   в 5 секунд кричать серверу "Я жива!" (вызывать ctx.heartbeat). Если воркер зависнет или сгорит и
     *   не подаст пульс за 5 секунд, сервер Temporal поймёт, что воркер умер, не будет ждать все 60 секунд,
     *   а сразу передаст задачу другому воркеру с последнего сохранённого шага!
     */
    private final ShowcaseActivity activities = Workflow.newActivityStub(
            ShowcaseActivity.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(60))
                    .setHeartbeatTimeout(Duration.ofSeconds(5))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setMaximumAttempts(3)
                            .build())
                    .build()
    );

    @Override
    public String executeSubProcess(String parentWorkflowId, int heartbeatSteps) {
        // Узнаём своё собственное имя (например, "showcase-a1b2c3-child-delivery")
        String childId = Workflow.getInfo().getWorkflowId();
        log.info("👶 [CHILD WORKFLOW START] Дочерний воркфлоу '{}' запущен по команде родителя '{}'", childId, parentWorkflowId);

        // Шаг 1 внутри ребёнка: небольшая пауза по серверному будильнику (1 секунда)
        Workflow.sleep(Duration.ofSeconds(1));

        // Шаг 2 внутри ребёнка: вызываем тяжёлую задачу из 4 этапов по 600 мс с отправкой пульса на каждом этапе.
        // Ожидаем: в консоли 4 раза напечатается прогресс-бар от 25% до 100%, а метод вернёт строку об успехе.
        String heartbeatSummary = activities.runHeavyTaskWithHeartbeat(
                "Доставка-Конвейер(" + childId + ")",
                heartbeatSteps,
                600
        );

        log.info("👶 [CHILD WORKFLOW END] Дочерний воркфлоу '{}' успешно выполнил работу и возвращает ответ родителю: {}",
                childId, heartbeatSummary);
        return "ChildWorkflow[" + childId + "] -> " + heartbeatSummary;
    }
}
