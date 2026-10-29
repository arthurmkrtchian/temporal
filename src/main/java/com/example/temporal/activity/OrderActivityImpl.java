package com.example.temporal.activity;

import io.temporal.spring.boot.ActivityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static com.example.temporal.workflow.OrderWorkflow.TASK_QUEUE;

/**
 * РЕАЛИЗАЦИЯ ИСПОЛНИТЕЛЯ (Activity Implementation) для базового заказа.
 *
 * Обрати внимание:
 * Это самый обычный Spring-компонент (@Component)! Сюда можно внедрять любые репозитории (JPA/JDBC),
 * HTTP-клиенты (RestClient / WebClient), KafkaTemplate и т.д.
 *
 * Аннотация @ActivityImpl(taskQueues = "OrderTaskQueue") говорит нашему Работнику (Worker):
 * "Когда сервер Temporal положит на доску 'OrderTaskQueue' задачу на оплату или отправку письма —
 *  вызывай методы вот этого класса!"
 */
@Component
@ActivityImpl(taskQueues = TASK_QUEUE)
public class OrderActivityImpl implements OrderActivity {

    private static final Logger log = LoggerFactory.getLogger(OrderActivityImpl.class);

    /**
     * ВЫПОЛНЕНИЕ ШАГА 1: Списание денег.
     *
     * Что происходит:
     * Работник (Activity Worker) забирает задачу из очереди и запускает этот код.
     * Если бы здесь произошла ошибка сети (вылетело исключение), сервер Temporal автоматически
     * подождал бы 1 секунду и вызвал этот метод заново (до 5 раз подряд по настройкам в OrderWorkflowImpl).
     *
     * Что мы ожидаем увидеть:
     * Две строчки в консоли о начале и успешном завершении оплаты, после чего метод вернёт `true`
     * обратно в дневник истории (Event History) на сервере Temporal.
     */
    @Override
    public boolean processPayment(String orderId, double amount) {
        log.info("💳 [OrderActivity Шаг 1] Списываем оплату ${} для заказа '{}'...", amount, orderId);
        // В реальном проекте здесь стоит вызов внешнего платёжного шлюза (Stripe, ЮKassa, банк и т.д.)
        log.info("✅ [OrderActivity Шаг 1] Оплата для заказа '{}' успешно проведена!", orderId);
        return true;
    }

    /**
     * ВЫПОЛНЕНИЕ ШАГА 2: Отправка уведомления на почту.
     *
     * Когда запускается:
     * Строго после того, как метод processPayment(...) вернул `true` и Workflow перешёл ко второму шагу.
     *
     * Что мы ожидаем увидеть:
     * Сообщение в консоли об отправке письма на указанный email.
     */
    @Override
    public void sendNotification(String orderId, String customerEmail) {
        log.info("📧 [OrderActivity Шаг 2] Отправляем письмо-подтверждение по заказу '{}' на адрес '{}'...", orderId, customerEmail);
        // В реальном проекте здесь вызывается SMTP-сервер или почтовый API (SendGrid, Mailgun и т.д.)
        log.info("✅ [OrderActivity Шаг 2] Уведомление по заказу '{}' успешно отправлено!", orderId);
    }
}
