package com.example.temporal.showcase.dto;

import java.util.List;

/**
 * КАРТОЧКА ТЕКУЩЕГО СОСТОЯНИЯ ЗАКАЗА (DTO).
 *
 * Возвращается при вызове `GET /api/showcase/{workflowId}/state` (@QueryMethod) или по завершении обработки заказа.
 * Содержит полную сводку по всем этапам жизненного цикла заказа:
 *
 * @param workflowId          Уникальный идентификатор процесса заказа в Temporal (например, "showcase-a1b2c3").
 * @param scenarioName        Название запущенного сценария заказа.
 * @param currentPhase        Текущий этап обработки заказа (например, "STEP_2_WAITING_SIGNAL_OR_TIMER" или "COMPLETED_SUCCESSFULLY").
 * @param currentBudget       Текущий бюджет заказа в долларах (с учётом изменений через @UpdateMethod).
 * @param approvedBySignal    Подтверждён ли заказ сигналом оператора (@SignalMethod)? (true / false).
 * @param signalApprover      Имя оператора, подтвердившего заказ (или "NONE", если сигнал не отправлялся).
 * @param raceWinnerProvider  Победитель тендера из 3 курьерских служб на Шаге 4 (например, "FastExpress (ответил за 700 мс)").
 * @param childWorkflowResult Отчёт дочернего процесса доставки (DeliveryChildWorkflow) на Шаге 5.
 * @param asyncCallbackResult Ответ внешней партнёрской системы по TaskToken на Шаге 6 (или "SKIPPED", если шаг отключён).
 * @param timelineLog         Пошаговый журнал заказа: список всех пройденных этапов в хронологическом порядке.
 * @param sagaCompensations   Список выполненных возвратов (компенсаций) из Шага 7:
 *                            пустой при успешной доставке, либо содержит записи о возврате оплаты и снятии брони склада при откате Saga.
 */
public record ShowcaseState(
        String workflowId,
        String scenarioName,
        String currentPhase,
        double currentBudget,
        boolean approvedBySignal,
        String signalApprover,
        String raceWinnerProvider,
        String childWorkflowResult,
        String asyncCallbackResult,
        List<String> timelineLog,
        List<String> sagaCompensations
) {
}
