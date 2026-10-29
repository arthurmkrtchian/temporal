package com.example.temporal.workflow;

import com.example.temporal.dto.OrderRequest;
import com.example.temporal.dto.OrderResponse;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * КОНТРАКТ БАЗОВОГО ПРОЦЕССА ЗАКАЗА (Workflow Interface).
 *
 * Роль в системе заказов:
 * Workflow — это "Менеджер-координатор конкретного заказа", который ведёт заказ по чек-листу от начала до конца.
 * Этот интерфейс объявляет точки взаимодействия с координатором заказа:
 * 1) Запустить обработку заказа — метод с аннотацией @WorkflowMethod.
 * 2) Узнать текущий этап обработки заказа — метод с аннотацией @QueryMethod.
 *
 * Правило Temporal:
 * Любой интерфейс процесса обязан иметь аннотацию @WorkflowInterface и РОВНО ОДИН метод @WorkflowMethod.
 */
@WorkflowInterface
public interface OrderWorkflow {

    /**
     * Имя очереди заявок (Task Queue), через которую диспетчерская Temporal
     * передаёт задачи по заказам нашему серверу-исполнителю (Worker).
     */
    String TASK_QUEUE = "OrderTaskQueue";

    /**
     * СТАРТ ОБРАБОТКИ ЗАКАЗА (@WorkflowMethod).
     *
     * Что происходит при вызове:
     * Сервер Temporal регистрирует новый заказ в базе данных, создаёт для него журнал событий (Event History)
     * и поручает свободному воркеру (Worker) выполнить шаги метода processOrder(...) сверху вниз.
     *
     * Результат:
     * Когда все шаги заказа (оплата и отправка уведомления) завершатся, метод вернёт итоговый OrderResponse,
     * а сам процесс заказа перейдёт в статус COMPLETED.
     */
    @WorkflowMethod
    OrderResponse processOrder(OrderRequest request);

    /**
     * ЗАПРОС ТЕКУЩЕГО СТАТУСА ЗАКАЗА (@QueryMethod).
     *
     * Как работает:
     * Пока заказ обрабатывается (или даже после его завершения), клиент может запросить текущий этап заказа.
     * @QueryMethod работает только на чтение: он возвращает значение переменной состояния, не изменяя ход заказа
     * и не добавляя новых записей в журнал Event History.
     */
    @QueryMethod
    String getStatus();
}
