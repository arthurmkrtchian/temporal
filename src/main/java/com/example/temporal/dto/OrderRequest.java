package com.example.temporal.dto;

/**
 * ЗАЯВКА НА СОЗДАНИЕ ЗАКАЗА (DTO) для базового примера.
 *
 * @param orderId       Уникальный номер заказа (например, "101"). Используется для формирования WorkflowId ("order-101").
 * @param customerEmail Почта покупателя для отправки подтверждения заказа (например, "client@example.com").
 * @param amount        Сумма заказа к оплате (например, 49.99).
 */
public record OrderRequest(
    String orderId,
    String customerEmail,
    double amount
) {}
