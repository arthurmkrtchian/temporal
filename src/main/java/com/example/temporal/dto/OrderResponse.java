package com.example.temporal.dto;

/**
 * ИТОГОВЫЙ ОТВЕТ ПО ЗАКАЗУ (DTO), который возвращается покупателю, когда сценарий (Workflow) дошёл до конца.
 *
 * @param orderId Номер обработанного заказа (например, "101").
 * @param status  Чем всё закончилось: "COMPLETED" (успех) или "PAYMENT_FAILED" (не хватило денег).
 * @param message Понятное человеку пояснение результата.
 */
public record OrderResponse(
    String orderId,
    String status,
    String message
) {}
