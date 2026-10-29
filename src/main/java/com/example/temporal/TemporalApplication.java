package com.example.temporal;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * ГЛАВНАЯ ТОЧКА ВХОДА В ПРИЛОЖЕНИЕ (Spring Boot + Temporal Worker).
 *
 * Что происходит, когда мы запускаем этот класс (mvn spring-boot:run):
 *
 * Шаг 1. Поднимается обычный веб-сервер на порту 8081 (чтобы мы могли слать запросы через curl или браузер).
 * Шаг 2. Стартер Temporal читает настройки из application.yml и подключается к серверу Temporal (порт 7233).
 * Шаг 3. Он находит все наши сценарии (@WorkflowImpl) и исполнителей (@ActivityImpl) в пакете com.example.temporal.
 * Шаг 4. В фоновом режиме запускаются "Работники" (Workers), которые встают на дежурство у очередей
 *        "OrderTaskQueue" и "ShowcaseTaskQueue" и ждут команд от сервера Temporal.
 *
 * Что мы ожидаем увидеть в консоли после старта:
 * Приложение не выключается, а висит в режиме ожидания. Как только кто-то вызовет REST-контроллер,
 * Работники тут же подхватят задачу и начнут печатать подробные шаги выполнения в консоль!
 */
@SpringBootApplication
public class TemporalApplication {

    public static void main(String[] args) {
        SpringApplication.run(TemporalApplication.class, args);
    }
}
