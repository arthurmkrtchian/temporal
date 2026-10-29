package com.example.temporal.showcase.workflow;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * КОНТРАКТ ДОЧЕРНЕГО ПРОЦЕССА ОТГРУЗКИ И ДОСТАВКИ ЗАКАЗА (Child Workflow Interface).
 *
 * Роль в системе заказов:
 * На Шаге 5 главный процесс заказа (ShowcaseWorkflow) запускает физическую отгрузку и доставку товара по маршруту.
 * Вместо того чтобы смешивать все детальные шаги логистики с основным процессом заказа, мы выделяем доставку
 * в отдельный подпроцесс — Дочерний Воркфлоу (DeliveryChildWorkflow).
 *
 * Зачем выделять доставку в Child Workflow:
 * 1) У дочернего процесса свой собственный журнал событий (Event History), поэтому детальные этапы маршрута
 *    не перегружают историю главного заказа.
 * 2) Дочерним процессом удобно управлять через ParentClosePolicy (например, автоматически остановить доставку,
 *    если главный заказ был принудительно отменён).
 */
@WorkflowInterface
public interface DeliveryChildWorkflow {

    /**
     * Главный метод дочернего сценария (@WorkflowMethod).
     *
     * @param parentWorkflowId Имя родительского воркфлоу, который нас вызвал (для наглядности в логах).
     * @param heartbeatSteps   Из скольких частей состоит долгая задача конвейера (например, 4 шага).
     * @return Строка-отчёт для родителя о том, что конвейер доставки успешно прошёл все шаги.
     */
    @WorkflowMethod
    String executeSubProcess(String parentWorkflowId, int heartbeatSteps);
}
