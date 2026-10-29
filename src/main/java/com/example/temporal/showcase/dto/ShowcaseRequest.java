package com.example.temporal.showcase.dto;

/**
 * ПАРАМЕТРЫ ЗАПУСКА РАСШИРЕННОГО СЦЕНАРИЯ ЗАКАЗА (DTO).
 *
 * Позволяет гибко настроить поведение заказа и смоделировать различные нештатные ситуации
 * (временные сбои платёжного шлюза, ожидание подтверждения оператора, сбой курьерской доставки с откатом Saga):
 *
 * @param scenarioName               Название сценария заказа (метка для логов, например "FULL_SHOWCASE_DEMO").
 * @param timerSeconds               Длительность окна ожидания подтверждения заказа на Шаге 2 (Durable Timer, в секундах).
 * @param flakyFailuresBeforeSuccess Сколько первых попыток обращения в платёжный шлюз на Шаге 3 должны упасть с ошибкой:
 *                                   - 2 -> попытки #1 и #2 упадут со сбоем сети, а попытка #3 пройдёт успешно;
 *                                   - -1 -> симулируется фатальная ошибка валидации (NonRetryable), повторы сразу прекращаются.
 * @param waitForSignal              Если true -> на Шаге 2 заказ ждёт сигнала подтверждения от оператора (@SignalMethod)
 *                                   либо автоматически продолжает работу по истечении timerSeconds секунд.
 * @param waitForAsyncCallback       Если true -> на Шаге 6 заказ сохраняет TaskToken (номер накладной) и ждёт
 *                                   внешнего HTTP-подтверждения через REST API (`/complete-async`).
 * @param triggerSagaRollback        Если true -> на финальном Шаге 7 (Saga) после брони склада и списания оплаты
 *                                   симулируется отказ курьерской службы, запускающий автоматический возврат денег и снятие брони.
 * @param initialBudget              Стартовый бюджет заказа в долларах (например, 1000.0), который можно менять на ходу через @UpdateMethod.
 */
public record ShowcaseRequest(
        String scenarioName,
        int timerSeconds,
        int flakyFailuresBeforeSuccess,
        boolean waitForSignal,
        boolean waitForAsyncCallback,
        boolean triggerSagaRollback,
        double initialBudget
) {
    /**
     * Заготовка №1: "Полный успешный прогон всех возможностей".
     * Что ожидаем:
     * 1. Сценарий откроет 8-секундное окно ожидания сигнала (в это время можно менять бюджет и слать сигнал).
     * 2. Затем переживёт 2 сбоя внешнего сервиса и с 3-й попытки пойдёт дальше.
     * 3. Проведёт гонку 3 курьерских служб, запустит дочерний процесс с пульсом и успешно завершит транзакцию Saga.
     */
    public static ShowcaseRequest fullSuccessDefault() {
        return new ShowcaseRequest(
                "FULL_SHOWCASE_DEMO",
                8,      // Ждём сигнал оператора до 8 секунд (если не пришёл — идём дальше сами)
                2,      // 2 учебных сбоя на Шаге 3, успех с 3-й попытки
                true,   // Включаем ожидание сигнала (@SignalMethod)
                false,  // Шаг 6 (ручной внешний колбэк) пропускаем, чтобы сценарий дошёл до конца сам
                false,  // Шаг 7 (Saga) пройдёт без аварии курьера
                1000.0  // Начальный бюджет $1000
        );
    }

    /**
     * Заготовка №2: "Демонстрация умной отмены (Saga Rollback)".
     * Что ожидаем:
     * На Шаге 7 склад забронируется, деньги ($500) спишутся, но курьерская служба ответит отказом.
     * Сразу после этого запустится обратная цепочка компенсаций: сначала вернутся $500, затем освободится склад!
     */
    public static ShowcaseRequest sagaRollbackDefault() {
        return new ShowcaseRequest(
                "SAGA_ROLLBACK_DEMO",
                1,
                0,
                false,
                false,
                true,   // Включаем аварию на 3-м шаге Saga -> ожидаем автоматический откат!
                500.0
        );
    }

    /**
     * Заготовка №3: "Демонстрация автоматических повторов (Retries)".
     * @param failures сколько раз сервис должен упасть перед успехом (или -1 для неисправимой ошибки).
     */
    public static ShowcaseRequest retriesDefault(int failures) {
        return new ShowcaseRequest(
                "RETRIES_BACKOFF_DEMO",
                1,
                failures,
                false,
                false,
                false,
                200.0
        );
    }
}
