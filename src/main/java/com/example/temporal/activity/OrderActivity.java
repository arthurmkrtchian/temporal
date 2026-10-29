package com.example.temporal.activity;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * КОНТРАКТ ВНЕШНИХ ОПЕРАЦИЙ ЗАКАЗА (Activity Interface).
 *
 * Роль в системе заказов:
 * Если Workflow — это координатор заказа с чек-листом, то Activity — это конкретные разовые операции во внешнем мире:
 * обращение в платёжный шлюз банка, отправка email, запись в БД или вызов стороннего API.
 *
 * Почему внешние вызовы выносятся в @ActivityMethod:
 * Внешние сервисы могут временно не отвечать из-за проблем с сетью. Вызов через Activity позволяет Temporal
 * автоматически повторять сбойные запросы по правилам RetryOptions и сохранять их результат в журнал заказа.
 */
@ActivityInterface
public interface OrderActivity {

    /**
     * Шаг 1: Сходить в банк и списать деньги за заказ.
     * Ожидаем: true, если оплата прошла успешно.
     */
    @ActivityMethod
    boolean processPayment(String orderId, double amount);

    /**
     * Шаг 2: Отправить клиенту email о том, что заказ оплачен и принят.
     * Ожидаем: письмо отправлено (метод ничего не возвращает, но в логах появится запись об отправке).
     */
    @ActivityMethod
    void sendNotification(String orderId, String customerEmail);
}
