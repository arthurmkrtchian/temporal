# Полное руководство по Temporal в Java и Spring Boot

> **Temporal** — это платформа надежного исполнения кода (Durable Execution Platform), позволяющая писать сложные, отказоустойчивые распределенные процессы так, будто они выполняются в рамках одного приложения на одном сервере, даже если они длятся секунды, месяцы или годы.

---

## Содержание
1. [Главная концепция: Durable Execution](#1-главная-концепция-durable-execution)
2. [Архитектура Temporal](#2-архитектура-temporal)
3. [Основные сущности](#3-основные-сущности)
   - [Workflow (Воркфлоу)](#workflow-воркфлоу)
   - [Activity (Активность)](#activity-активность)
   - [Task Queue (Очередь задач)](#task-queue-очередь-задач)
   - [Worker (Воркер)](#worker-воркер)
4. [Ключевые аннотации и классы Java SDK](#4-ключевые-аннотации-и-классы-java-sdk)
5. [Правила детерминизма (Workflow Constraints)](#5-правила-детерминизма-workflow-constraints)
6. [Практические шаблоны и примеры кода](#6-практические-шаблоны-и-примеры-кода)
   - [1. Таймауты и автоматические ретраи (Retry Policy)](#1-таймауты-и-автоматические-ретраи)
   - [2. Сигналы (Signals), Запросы (Queries) и Апдейты (Updates)](#2-сигналы-signals-запросы-queries-и-апдейты-updates)
   - [3. Долгоживущие процессы и таймеры (Human-in-the-loop)](#3-долгоживущие-процессы-и-таймеры-human-in-the-loop)
   - [4. Паттерн Saga (Компенсация транзакций)](#4-паттерн-saga-компенсация-транзакций)
   - [5. Асинхронность и параллельное выполнение (Promises)](#5-асинхронность-и-параллельное-выполнение-promises)
   - [6. Бесконечные процессы (Continue-As-New)](#6-бесконечные-процессы-continue-as-new)
   - [7. Версионирование кода (Workflow Versioning)](#7-версионирование-кода-workflow-versioning)
7. [Интеграция со Spring Boot](#7-интеграция-со-spring-boot)
8. [Тестирование без поднятия инфраструктуры](#8-тестирование-без-поднятия-инфраструктуры)

---

## 1. Главная концепция: Durable Execution

В классических распределенных системах для реализации многошагового процесса разработчик вынужден решать множество нефункциональных задач:
- Что делать, если сервис упал посередине транзакции?
- Как восстановить состояние из БД?
- Как не послать платеж дважды (идемпотентность)?
- Как организовать ожидание события через 30 дней?

**Как решает это Temporal:**
Вы пишете обычный императивный Java-код с циклами, условиями, вызовами методов и даже `Workflow.sleep(Duration.ofDays(30))`. Temporal берет на себя сохранение состояния каждого шага в **Event History** (журнал событий).

Если сервер, на котором крутится воркер, внезапно выключится:
1. Другой воркер подхватит задачу.
2. Temporal «воспроизведет» (Replay) историю событий до точки сбоя.
3. Код продолжит выполнение ровно с того места, где остановился.

---

## 2. Архитектура Temporal

```text
┌────────────────────────────────────────────────────────┐
│                   Temporal Server                      │
│   (Frontend Service + History + Matching + DB/Postgres)│
│                                                        │
│           Очереди задач (Task Queues)                  │
└───────────────▲──────────────────────────▲─────────────┘
                │                          │
        gRPC    │                  gRPC    │
        (7233)  │                  (7233)  │
                ▼                          ▼
   ┌────────────────────────┐  ┌────────────────────────┐
   │     Spring Boot App    │  │    Client Service      │
   │  ┌──────────────────┐  │  │                        │
   │  │  Temporal Worker │  │  │  WorkflowClient.start  │
   │  │ (Workflow/Act.)  │  │  │  (Запуск / Сигналы)   │
   │  └──────────────────┘  │  └────────────────────────┘
   └────────────────────────┘
```

1. **Temporal Server**: Не исполняет бизнес-логику! Он только координирует шаги, хранит журнал событий, следит за таймерами и распределяет задачи по очередям.
2. **Workers**: Ваше приложение (наш Spring Boot сервис). Воркеры подключаются к серверу, слушают Task Queue, забирают задачи и выполняют код.
3. **Temporal UI (Админка)**: Веб-консоль для визуализации дерева вызовов, стектрейсов, параметров шагов и ручного вмешательства.

---

## 3. Основные сущности

### Workflow (Воркфлоу)
- **Оркестратор процесса**.
- Описывает бизнес-логику («Сначала забронируй отель, потом спиши деньги, если списание не удалось — отмени бронь»).
- **Критическое требование**: код воркфлоу должен быть **строго детерминированным**.

### Activity (Активность)
- **Исполнитель недетерминированных действий**.
- Вызовы внешних API, баз данных, отправка email, сложные вычисления, генерация случайных чисел.
- Активности могут падать — Temporal будет автоматически их перезапускать согласно политике ретраев (`RetryOptions`).

### Task Queue (Очередь задач)
- Именованная очередь на Temporal Server (например, `"OrdersQueue"`).
- Воркеры регистрируются на прослушивание определенной очереди, а клиенты отправляют задачи именно в нее.

### Worker (Воркер)
- Поток или пул потоков внутри вашего Java-процесса, исполняющий воркфлоу и активности.

---

## 4. Ключевые аннотации и классы Java SDK

### Аннотации

| Аннотация | Где применяется | Назначение |
| :--- | :--- | :--- |
| `@WorkflowInterface` | Интерфейс | Объявляет интерфейс как контракт воркфлоу |
| `@WorkflowMethod` | Метод интерфейса | Главная точка входа в воркфлоу (может быть только один) |
| `@SignalMethod` | Метод интерфейса | Асинхронный сигнал в работающий воркфлоу (fire-and-forget) |
| `@QueryMethod` | Метод интерфейса | Синхронное чтение внутреннего состояния воркфлоу (без побочных эффектов) |
| `@UpdateMethod` | Метод интерфейса | Мутация состояния с валидацией и немедленным возвратом результата клиенту |
| `@ActivityInterface` | Интерфейс | Объявляет интерфейс как контракт активностей |
| `@ActivityMethod` | Метод интерфейса | Шаг активности |
| `@WorkflowImpl` | Класс | (Spring Boot Starter) авторегистрация воркфлоу на Task Queue |
| `@ActivityImpl` | Класс / Bean | (Spring Boot Starter) авторегистрация активности на Task Queue |

### Служебные классы

- **`Workflow`**: Главный утилитный класс внутри воркфлоу:
  - `Workflow.sleep(Duration)` — надежный сон (таймер ставится в Temporal Server).
  - `Workflow.currentTimeMillis()` — детерминированное текущее время.
  - `Workflow.newActivityStub(Class, ActivityOptions)` — создание прокси для вызова активности.
  - `Workflow.await(Duration, () -> condition)` — ожидание выполнения условия.
  - `Workflow.newPromiseArray(...)` / `Async.function(...)` — параллельные вызовы.
- **`WorkflowClient`**: Клиент для запуска воркфлоу из веб-контроллеров или внешних сервисов:
  - `workflowClient.newWorkflowStub(Class, WorkflowOptions)` — получение прокси.
  - `WorkflowClient.start(workflow::method, arg)` — асинхронный запуск.

---

## 5. Правила детерминизма (Workflow Constraints)

Поскольку Temporal использует **Event Sourcing** и воспроизводит код при сбоях, код воркфлоу **обязан при одинаковых входных данных повторять абсолютно ту же последовательность шагов**.

### ❌ Что категорически НЕЛЬЗЯ делать в коде Workflow:
1. `Thread.sleep(...)` — блокирует воркер и не создает таймер на сервере.
2. `System.currentTimeMillis()` или `Instant.now()` — при replay вернет другое время.
3. `UUID.randomUUID()` или `new Random()` — даст разные значения.
4. Прямые вызовы БД, сетевые HTTP-запросы или чтение файлов — всё это должно быть внутри Activity!
5. Создание стандартных Java-потоков (`new Thread()`, `Executors`, параллельные `stream()`).
6. Использование глобальных мутабельных статических переменных.

### ✔️ Что нужно использовать взамен:
| Запрещено | Разрешено в Temporal |
| :--- | :--- |
| `Thread.sleep(duration)` | `Workflow.sleep(duration)` |
| `System.currentTimeMillis()` | `Workflow.currentTimeMillis()` |
| `UUID.randomUUID()` | `Workflow.randomUUID()` |
| `new Thread(...)` | `Async.procedure(...)` / `Async.function(...)` |
| HTTP / DB вызовы | `Workflow.newActivityStub(...)` |

---

## 6. Практические шаблоны и примеры кода

### 1. Таймауты и автоматические ретраи

Активности конфигурируются через `ActivityOptions`. Temporal берет на себя повторные попытки при сбоях:

```java
ActivityOptions options = ActivityOptions.newBuilder()
    // Максимальное время выполнения одного вызова активности
    .setStartToCloseTimeout(Duration.ofSeconds(10))
    // Максимальное время на все попытки вместе взятые
    .setScheduleToCloseTimeout(Duration.ofMinutes(5))
    // Политика ретраев
    .setRetryOptions(RetryOptions.newBuilder()
        .setInitialInterval(Duration.ofSeconds(1))
        .setBackoffCoefficient(2.0)
        .setMaximumInterval(Duration.ofSeconds(20))
        .setMaximumAttempts(5)
        // Исключения, при которых ретраить бессмысленно:
        .setDoNotRetry(IllegalArgumentException.class.getName())
        .build())
    .build();

PaymentActivity paymentActivity = Workflow.newActivityStub(PaymentActivity.class, options);
```

---

### 2. Сигналы (Signals), Запросы (Queries) и Апдейты (Updates)

Воркфлоу может быть интерактивным:

```java
@WorkflowInterface
public interface SubscriptionWorkflow {

    @WorkflowMethod
    void runSubscription(String userId);

    // Сигнал: асинхронно передает данные внутрь работающего воркфлоу
    @SignalMethod
    void cancelSubscription();

    // Запрос: синхронно возвращает состояние без записи события в историю
    @QueryMethod
    String getStatus();

    // Апдейт: атомарно меняет состояние и возвращает результат клиенту
    @UpdateMethod
    boolean updatePaymentMethod(String newCardToken);
}
```

Реализация:
```java
@WorkflowImpl(taskQueues = "SUBSCRIPTION_QUEUE")
public class SubscriptionWorkflowImpl implements SubscriptionWorkflow {

    private boolean isCancelled = false;
    private String status = "ACTIVE";

    @Override
    public void runSubscription(String userId) {
        while (!isCancelled) {
            // Ждем 30 дней ИЛИ сигнала отмены
            Workflow.await(Duration.ofDays(30), () -> isCancelled);

            if (!isCancelled) {
                // Списываем абонентскую плату за следующий месяц
                activities.chargeMonthlyFee(userId);
            }
        }
        status = "TERMINATED";
    }

    @Override
    public void cancelSubscription() {
        this.isCancelled = true;
        this.status = "CANCELLING";
    }

    @Override
    public String getStatus() {
        return status;
    }

    @Override
    public boolean updatePaymentMethod(String newCardToken) {
        boolean valid = activities.validateCard(newCardToken);
        if (valid) {
            this.status = "PAYMENT_METHOD_UPDATED";
        }
        return valid;
    }
}
```

---

### 3. Долгоживущие процессы и таймеры (Human-in-the-loop)

Пример процесса подтверждения заявки человеком с таймаутом в 3 дня:

```java
@WorkflowImpl(taskQueues = "APPROVAL_QUEUE")
public class ApprovalWorkflowImpl implements ApprovalWorkflow {

    private boolean approved = false;
    private boolean rejected = false;

    @Override
    public String processApplication(ApplicationData data) {
        activities.sendApprovalEmailToManager(data);

        // Ждем решения менеджера до 72 часов
        boolean decisionMade = Workflow.await(
            Duration.ofDays(3),
            () -> approved || rejected
        );

        if (!decisionMade) {
            // Таймаут истек, менеджер не ответил
            activities.escalateToHeadOfDepartment(data);
            return "ESCALATED";
        }

        if (approved) {
            activities.applyApplication(data);
            return "APPROVED";
        } else {
            activities.notifyRejection(data);
            return "REJECTED";
        }
    }

    @Override
    public void approve() {
        this.approved = true;
    }

    @Override
    public void reject() {
        this.rejected = true;
    }
}
```

---

### 4. Паттерн Saga (Компенсация транзакций)

Когда нужно выполнить несколько распределенных шагов и откатить выполненные назад в случае ошибки:

```java
import io.temporal.workflow.Saga;

@WorkflowImpl(taskQueues = "ORDER_QUEUE")
public class OrderSagaWorkflowImpl implements OrderSagaWorkflow {

    private final HotelActivity hotelActivity = Workflow.newActivityStub(HotelActivity.class, defaultOptions);
    private final FlightActivity flightActivity = Workflow.newActivityStub(FlightActivity.class, defaultOptions);

    @Override
    public void bookTrip(TripRequest request) {
        Saga.Options sagaOptions = new Saga.Options.Builder()
            .setParallelCompensation(false) // Откатывать строго в обратном порядке
            .build();
        Saga saga = new Saga(sagaOptions);

        try {
            // Шаг 1: Бронь отеля
            hotelActivity.bookHotel(request.hotelId());
            // Регистрируем компенсацию (вызовется только при ошибке)
            saga.addCompensation(hotelActivity::cancelHotel, request.hotelId());

            // Шаг 2: Бронь перелета
            flightActivity.bookFlight(request.flightId());
            saga.addCompensation(flightActivity::cancelFlight, request.flightId());

        } catch (Exception e) {
            // При любом сбое автоматически выполняем все зарегистрированные компенсации
            saga.compensate();
            throw e;
        }
    }
}
```

---

### 5. Асинхронность и параллельное выполнение (Promises)

Для параллельного вызова нескольких активностей используйте `Async` и `Promise`:

```java
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;

// Запускаем две активности одновременно
Promise<PriceInfo> vendorA = Async.function(activities::getVendorAPrice, item);
Promise<PriceInfo> vendorB = Async.function(activities::getVendorBPrice, item);

// Ждем завершения обеих
Promise.allOf(vendorA, vendorB).get();

PriceInfo bestPrice = vendorA.get().price() < vendorB.get().price() 
    ? vendorA.get() 
    : vendorB.get();
```

---

### 6. Бесконечные процессы (Continue-As-New)

Если воркфлоу работает непрерывно (например, мониторит устройство или периодически проверяет статус), его Event History может превысить лимит (рекомендуется не более 10 000 событий).

Для этого используется `Workflow.continueAsNew`:

```java
@Override
public void monitorDevice(String deviceId, int iteration) {
    activities.checkHealth(deviceId);
    Workflow.sleep(Duration.ofHours(1));

    if (iteration < 1000) {
        // Очищает историю событий и начинает новую итерацию с текущим состоянием
        Workflow.continueAsNew(deviceId, iteration + 1);
    }
}
```

---

### 7. Версионирование кода (Workflow Versioning)

Если воркфлоу уже запущен в проде (и выполняется недели), нельзя просто так изменить порядок вызовов — replay упадет с `NonDeterministicException`. Для безопасных изменений используется `Workflow.getVersion`:

```java
int version = Workflow.getVersion("AddEmailVerificationStep", Workflow.DEFAULT_VERSION, 1);

if (version == Workflow.DEFAULT_VERSION) {
    // Старый код для воркфлоу, запущенных до релиза
    activities.processOldWay();
} else {
    // Новый код для свежих воркфлоу
    activities.processWithEmailVerification();
}
```

---

## 7. Интеграция со Spring Boot

С зависимостью `temporal-spring-boot-starter`:

1. **Конфигурация (`application.yml`)**:
   ```yaml
   spring:
     temporal:
       connection:
         target: 127.0.0.1:7233
       namespace: default
       workers-auto-discovery:
         packages:
           - com.example.temporal
   ```

2. **Запуск из REST-контроллера**:
   ```java
   @RestController
   @RequestMapping("/api/orders")
   public class OrderController {

       private final WorkflowClient workflowClient;

       public OrderController(WorkflowClient workflowClient) {
           this.workflowClient = workflowClient;
       }

       @PostMapping("/{id}")
       public String startOrder(@PathVariable String id) {
           OrderWorkflow workflow = workflowClient.newWorkflowStub(
               OrderWorkflow.class,
               WorkflowOptions.newBuilder()
                   .setWorkflowId("order-" + id) // Бизнес-идентификатор (предотвращает дубликаты!)
                   .setTaskQueue("OrderTaskQueue")
                   .build()
           );

           // Асинхронный запуск:
           WorkflowClient.start(workflow::processOrder, new OrderRequest(id));
           return "Started: order-" + id;
       }
   }
   ```

---

## 8. Тестирование без поднятия инфраструктуры

Temporal поставляется с встроенным `TestWorkflowEnvironment`, который умеет **ускорять время (Time Skipping)**: таймер на 30 дней в тесте выполнится за миллисекунды.

```java
class OrderWorkflowTest {

    private TestWorkflowEnvironment testEnv;
    private Worker worker;

    @BeforeEach
    void setUp() {
        testEnv = TestWorkflowEnvironment.newInstance();
        worker = testEnv.newWorker("OrderTaskQueue");
        worker.registerWorkflowImplementationTypes(OrderWorkflowImpl.class);
        worker.registerActivitiesImplementations(new FakePaymentActivity());
        testEnv.start();
    }

    @AfterEach
    void tearDown() {
        testEnv.close();
    }

    @Test
    void testWorkflow() {
        OrderWorkflow workflow = testEnv.getWorkflowClient().newWorkflowStub(
            OrderWorkflow.class,
            WorkflowOptions.newBuilder().setTaskQueue("OrderTaskQueue").build()
        );

        OrderResponse res = workflow.processOrder(new OrderRequest("101"));
        assertEquals("COMPLETED", res.status());
    }
}
```

---

## Сводная шпаргалка

| Задача | Решение |
| :--- | :--- |
| Вызов внешнего сервиса / БД | Activity (`@ActivityInterface`) |
| Обработка сбоя сети | `RetryOptions` в `ActivityOptions` |
| Ожидание внешнего события | `@SignalMethod` + `Workflow.await(...)` |
| Просмотр состояния извне | `@QueryMethod` |
| Мутация состояния с ответом | `@UpdateMethod` |
| Таймер / Пауза | `Workflow.sleep(duration)` |
| Откат транзакций | `Saga saga = new Saga(...)` |
| Параллельные шаги | `Async.function(...)` / `Promise.allOf(...)` |
| Тесты без внешнего сервера | `TestWorkflowEnvironment` + `spring.temporal.test-server.enabled=true` |
