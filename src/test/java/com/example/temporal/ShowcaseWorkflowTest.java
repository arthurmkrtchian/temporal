package com.example.temporal;

import com.example.temporal.showcase.activity.AsyncTokenRegistry;
import com.example.temporal.showcase.activity.ShowcaseActivityImpl;
import com.example.temporal.showcase.dto.ShowcaseRequest;
import com.example.temporal.showcase.dto.ShowcaseState;
import com.example.temporal.showcase.workflow.DeliveryChildWorkflowImpl;
import com.example.temporal.showcase.workflow.ShowcaseWorkflow;
import com.example.temporal.showcase.workflow.ShowcaseWorkflowImpl;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowUpdateException;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * АВТОМАТИЧЕСКИЕ ТЕСТЫ СЦЕНАРИЕВ С "МАШИНОЙ СЖАТИЯ ВРЕМЕНИ" (TestWorkflowEnvironment).
 *
 * Главный вопрос новичка:
 * "А как тестировать Workflow, если для него нужен запущенный Docker с сервером Temporal и базой данных,
 *  да ещё и внутри кода стоят таймеры по 30 секунд (или по 7 дней)?"
 *
 * Ответ — класс `TestWorkflowEnvironment`!
 * 1) Он поднимает лёгкий мини-сервер Temporal прямо в оперативной памяти на время теста (Docker НЕ нужен!).
 * 2) У него есть встроенная суперспособность **Time Skipping (Промотка времени)**:
 *    если ваш сценарий засыпает на `Workflow.sleep(Duration.ofHours(24))`, тестовый сервер видит, что все потоки
 *    просто ждут будильника, и МГНОВЕННО переводит стрелки часов на 24 часа вперёд!
 *    Поэтому тест с многочасовыми паузами проходит за пару секунд.
 */
public class ShowcaseWorkflowTest {

    private TestWorkflowEnvironment testEnv;
    private Worker worker;
    private AsyncTokenRegistry tokenRegistry;

    /**
     * ПОДГОТОВКА ПЕРЕД КАЖДЫМ ТЕСТОМ (@BeforeEach):
     * 1. Создаём игрушечный сервер Temporal в памяти (`TestWorkflowEnvironment.newInstance()`).
     * 2. Создаём Работника (`Worker`), который слушает очередь `"ShowcaseTaskQueue"`.
     * 3. Знакомим Работника с нашими сценариями (`ShowcaseWorkflowImpl` и `DeliveryChildWorkflowImpl`)
     *    и нашим классом курьеров (`ShowcaseActivityImpl`).
     * 4. Нажимаем `testEnv.start()` — мини-сервер готов к бою!
     */
    @BeforeEach
    void setUp() {
        testEnv = TestWorkflowEnvironment.newInstance();
        worker = testEnv.newWorker(ShowcaseWorkflow.TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(
                ShowcaseWorkflowImpl.class,
                DeliveryChildWorkflowImpl.class
        );

        tokenRegistry = new AsyncTokenRegistry();
        worker.registerActivitiesImplementations(new ShowcaseActivityImpl(tokenRegistry));

        testEnv.start();
    }

    /**
     * УБОРКА ПОСЛЕ КАЖДОГО ТЕСТА (@AfterEach):
     * Выключаем мини-сервер и освобождаем память.
     */
    @AfterEach
    void tearDown() {
        testEnv.close();
    }

    /**
     * ТЕСТ №1: Полный успешный прогон сценария от начала до конца.
     *
     * Что за чем мы проверяем:
     * 1. Запускаем сценарий с начальным бюджетом $1000 и 2 запланированными сбоями на Шаге 3.
     * 2. Ждём завершения и проверяем ожидания (assert):
     *    - Финальная фаза должна быть `"COMPLETED_SUCCESSFULLY"`;
     *    - Бюджет остался целым (`1000.0`);
     *    - В параллельной гонке 3 курьерских служб победил самый быстрый — `"FastExpress"`;
     *    - Дочерний сценарий (`DeliveryChildWorkflow`) честно прошёл все 4 шага с пульсом (Heartbeats);
     *    - Список откатов `sagaCompensations` пуст, так как аварий на Шаге 7 не было.
     */
    @Test
    void testFullShowcaseSuccessWithRetriesRaceAndChildWorkflow() {
        ShowcaseWorkflow workflow = testEnv.getWorkflowClient().newWorkflowStub(
                ShowcaseWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId("test-full-showcase")
                        .setTaskQueue(ShowcaseWorkflow.TASK_QUEUE)
                        .build()
        );

        ShowcaseRequest request = new ShowcaseRequest(
                "TEST_FULL_RUN",
                2,
                2,      // Заказываем 2 сбоя на Шаге 3 -> ожидаем, что с 3-й попытки ретрай победит!
                false,
                false,
                false,
                1000.0
        );

        ShowcaseState state = workflow.runShowcase(request);

        assertEquals("COMPLETED_SUCCESSFULLY", state.currentPhase());
        assertEquals(1000.0, state.currentBudget());
        assertTrue(state.raceWinnerProvider().startsWith("FastExpress"));
        assertTrue(state.childWorkflowResult().contains("Completed 4 steps with heartbeats"));
        assertTrue(state.sagaCompensations().isEmpty());
    }

    /**
     * ТЕСТ №2: Проверка автоматического отката транзакции по паттерну SAGA (обратный порядок LIFO).
     *
     * Что за чем мы проверяем:
     * 1. Запускаем заготовку `sagaRollbackDefault()`, где на Шаге 7 склад бронируется, деньги списываются,
     *    а курьер кидает ошибку.
     * 2. Проверяем ожидания:
     *    - Итоговая фаза сценария — `"ROLLED_BACK_BY_SAGA"`;
     *    - Выполнено ровно 2 возврата (компенсации);
     *    - ПЕРВЫМ в списке возвратов (`get(0)`) идёт возврат денег (`"Refunded"` — отмена Шага 2);
     *    - ВТОРЫМ в списке возвратов (`get(1)`) идёт снятие резерва склада (`"Inventory"` — отмена Шага 1).
     */
    @Test
    void testSagaRollbackCompensatesInReverseOrder() {
        ShowcaseWorkflow workflow = testEnv.getWorkflowClient().newWorkflowStub(
                ShowcaseWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId("test-saga-rollback")
                        .setTaskQueue(ShowcaseWorkflow.TASK_QUEUE)
                        .build()
        );

        ShowcaseState state = workflow.runShowcase(ShowcaseRequest.sagaRollbackDefault());

        assertEquals("ROLLED_BACK_BY_SAGA", state.currentPhase());
        assertEquals(2, state.sagaCompensations().size());
        // Проверяем строгий обратный порядок (LIFO): сначала вернули деньги (Шаг 2), затем освободили склад (Шаг 1)
        assertTrue(state.sagaCompensations().get(0).contains("Refunded"));
        assertTrue(state.sagaCompensations().get(1).contains("Inventory"));
    }

    /**
     * ТЕСТ №3: Интерактивное общение с живым сценарием (@UpdateMethod + @UpdateValidatorMethod + @SignalMethod).
     *
     * Что за чем мы делаем:
     * 1. Запускаем сценарий В ФОНЕ (`WorkflowClient.start`) с 30-секундным ожиданием сигнала на Шаге 2.
     * 2. Пока сценарий ждёт на Шаге 2, звоним ему через `adjustBudgetUpdate(250.0)` ->
     *    ожидаем, что стартовый бюджет $500 сразу превратится в $750!
     * 3. Пробуем обмануть сценарий и отнять $5000 (`adjustBudgetUpdate(-5000.0)`) ->
     *    ожидаем, что охранник `@UpdateValidatorMethod` не пустит запрос и выбросит `WorkflowUpdateException`!
     * 4. Шлём сигнал `workflow.approveSignal("JUnitTester")` ->
     *    ожидаем, что сценарий мгновенно проснётся (не дожидаясь 30 секунд!) и дойдёт до победного финиша с бюджетом $750.
     */
    @Test
    void testInteractiveSignalUpdateAndValidator() {
        ShowcaseWorkflow workflow = testEnv.getWorkflowClient().newWorkflowStub(
                ShowcaseWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setWorkflowId("test-interactive")
                        .setTaskQueue(ShowcaseWorkflow.TASK_QUEUE)
                        .build()
        );

        ShowcaseRequest request = new ShowcaseRequest(
                "INTERACTIVE_TEST",
                30,     // Ставим будильник ожидания на 30 секунд
                0,
                true,   // Ждём сигнал оператора
                false,
                false,
                500.0   // Стартовый бюджет $500
        );

        // Запускаем сценарий асинхронно в фоне
        WorkflowClient.start(workflow::runShowcase, request);

        // Шаг 1: Успешно увеличиваем бюджет на +$250 через синхронный @UpdateMethod (500 + 250 = 750)
        double newBudget = workflow.adjustBudgetUpdate(250.0);
        assertEquals(750.0, newBudget);

        // Шаг 2: Пытаемся увести бюджет в минус (-5000) — ожидаем мгновенный отказ от @UpdateValidatorMethod
        assertThrows(WorkflowUpdateException.class, () -> workflow.adjustBudgetUpdate(-5000.0));

        // Шаг 3: Отправляем @SignalMethod, чтобы досрочно разбудить Workflow.await(...)
        workflow.approveSignal("JUnitTester");

        // Подключаемся к результату и проверяем, что все наши изменения сохранились
        ShowcaseState finalState = workflow.runShowcase(request);
        assertTrue(finalState.approvedBySignal());
        assertEquals("JUnitTester", finalState.signalApprover());
        assertEquals(750.0, finalState.currentBudget());
        assertEquals("COMPLETED_SUCCESSFULLY", finalState.currentPhase());
    }
}
