package com.example.temporal.workflow;

import com.example.temporal.activity.OrderActivity;
import com.example.temporal.dto.OrderRequest;
import com.example.temporal.dto.OrderResponse;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;

import java.time.Duration;

/**
 * РЕАЛИЗАЦИЯ БАЗОВОГО ПРОЦЕССА ОБРАБОТКИ ЗАКАЗА (Workflow Implementation).
 *
 * Роль в системе заказов:
 * Этот класс — "Менеджер-координатор конкретного заказа". Он ведёт заказ по шагам:
 * "Шаг 1: поручить списать оплату в банке. Прошло успешно? Тогда Шаг 2: поручить отправить чек на почту покупателю".
 *
 * Почему заказ не потеряется и не спишет деньги дважды при перезагрузке сервера:
 * Temporal сохраняет результат каждого выполненного шага в журнал событий (Event History) в базе данных.
 * Если сервер перезагрузится посреди работы, Temporal восстановит состояние заказа по журналу:
 * он увидит, что оплата уже проведена, НЕ будет списывать деньги повторно и сразу перейдёт к отправке письма.
 *
 * Важная деталь:
 * В отличие от синглтон-бинов Spring (@Service / @Component), для каждого нового заказа создаётся
 * отдельный экземпляр OrderWorkflowImpl, поэтому поле `status` хранит состояние только этого конкретного заказа.
 */
@WorkflowImpl(taskQueues = OrderWorkflow.TASK_QUEUE)
public class OrderWorkflowImpl implements OrderWorkflow {

    /**
     * Текущий этап обработки заказа:
     * "INITIALIZING" -> "PROCESSING_PAYMENT" -> "SENDING_CONFIRMATION" -> "COMPLETED".
     * При вызове getStatus() (@QueryMethod) возвращается текущее значение этого поля.
     */
    private String status = "INITIALIZING";

    /**
     * КЛИЕНТ ДЛЯ ВЫЗОВА ВНЕШНИХ ОПЕРАЦИЙ ЗАКАЗА (ActivityStub) И ЕГО РЕГЛАМЕНТ (ActivityOptions).
     *
     * Мы НЕ вызываем new OrderActivityImpl() напрямую, иначе Temporal не сможет зафиксировать шаг в журнале Event History.
     * Вместо этого мы создаём прокси-клиент (ActivityStub) и задаём регламент выполнения операций (ActivityOptions):
     * 1) StartToCloseTimeout (30 сек) — максимальное время на одну попытку обращения в банк или почтовый сервис.
     * 2) RetryOptions (Правила автоматических повторов при временных сбоях сети или банка):
     *    - InitialInterval (1 сек): после первого сбоя подождать 1 секунду перед повтором.
     *    - BackoffCoefficient (2.0): с каждым новым сбоем удваивать паузу (1 сек -> 2 сек -> 4 сек -> 8 сек...).
     *    - MaximumInterval (10 сек): ограничивать паузу между попытками максимум 10 секундами.
     *    - MaximumAttempts (5): сделать максимум 5 попыток, прежде чем признать операцию неуспешной.
     */
    private final OrderActivity activities = Workflow.newActivityStub(
            OrderActivity.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(
                            RetryOptions.newBuilder()
                                    .setInitialInterval(Duration.ofSeconds(1))
                                    .setMaximumInterval(Duration.ofSeconds(10))
                                    .setBackoffCoefficient(2.0)
                                    .setMaximumAttempts(5)
                                    .build()
                    )
                    .build()
    );

    /**
     * ПОШАГОВЫЙ СЦЕНАРИЙ ОБРАБОТКИ ЗАКАЗА.
     * Читается сверху вниз, как обычная сказка: что за чем идёт и чего мы ждём.
     */
    @Override
    public OrderResponse processOrder(OrderRequest request) {
        // -------------------------------------------------------------------------
        // ШАГ 1: Списание оплаты через внешний сервис (Activity №1)
        // -------------------------------------------------------------------------
        // Меняем табличку статуса на "Списываем оплату"
        status = "PROCESSING_PAYMENT";

        // Что сейчас произойдёт за кулисами:
        // 1. Наш Workflow говорит серверу Temporal: "Запланируй задачу processPayment в очереди OrderTaskQueue!"
        // 2. Сервер записывает это в БД и передаёт задачу в OrderActivityImpl.
        // 3. Мы ждём здесь ответа (true = деньги списаны, false = отказ банка).
        boolean paymentSuccess = activities.processPayment(request.orderId(), request.amount());

        // Проверяем: а вдруг банк отказал?
        if (!paymentSuccess) {
            // Ожидаемое поведение при отказе: меняем статус на ошибку и сразу выходим (письмо об успехе не шлём!).
            status = "PAYMENT_FAILED";
            return new OrderResponse(request.orderId(), status, "Payment authorization failed");
        }

        // -------------------------------------------------------------------------
        // ШАГ 2: Отправка письма-подтверждения покупателю (Activity №2)
        // -------------------------------------------------------------------------
        // Этот шаг запустится ТОЛЬКО после того, как Шаг 1 (оплата) успешно завершился и сохранился в истории.
        status = "SENDING_CONFIRMATION";
        activities.sendNotification(request.orderId(), request.customerEmail());

        // -------------------------------------------------------------------------
        // ШАГ 3: Финиш!
        // -------------------------------------------------------------------------
        // Ожидаемый результат: оба шага позади, ставим финальный статус "COMPLETED"
        // и возвращаем карточку ответа. Сервер Temporal ставит галочку "Выполнено" и закрывает процесс.
        status = "COMPLETED";
        return new OrderResponse(request.orderId(), status, "Order processed and confirmed successfully");
    }

    @Override
    public String getStatus() {
        // Возвращаем то значение, которое прямо сейчас лежит в переменной status
        return status;
    }
}
