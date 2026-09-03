# ui-llm-compare

## Вспомогательная система для UI тестирования

## Описание

Сервис помогающий при тестировании UI, когда не возможно сравнить текущее состояние с ожидаемым по пиксельно, по селекторам или с помощью OpenCV. Состоит из двух компонентов: **Reciver** и **Worker**

- **Reciver** - принимает запросы от тестового фреймворка(чёрный ящик), сохраняет артефакты, ставит задачи на обработку, возвращает результат обработки
- **Worker** - принимает задачи из очереди на обработку и отправляет c rate limit запросы в LLM, интерпретирует и сохраняет ответы LLM

Сервис предоставляет асинхронное HTTP API. За статусом и результатом обработки надо прийти позже с отдельным запросом.

## Требования

### Must have

- HTTP API сервис для подключения
- `/api/compare` - Точка для отправки 2х изображений в 1 запросе. В ответ должен вернуться идентификатор задачи анализа изображений.
- `/api/tasks/<id>` - Точка для получения ответа в формате JSON о статусе и результате запроса
- Изображения должны сохраняться
- Асинхронное API
- Сохранение информации о запросе в БД
- Чтение конфигурации из JSON файла

### Should have

- Аутентификация по JWT Bearer Token при вызове HTTP API
- `/api/compare` - Сохранение в БД Информации об пользователе запроса
- `/api/tasks` - Точка для получения всех задач для текущего пользователя

### Nice to have

- `/api/tasks` - пагинация
- gRPC интерфейс

## Ифнраструктура

- Postgres - в качетве БД, для хранения данных запросов/состояния
- S3 совместимое хранилище - Объектное хранилище для изображений
- Kafka - для очереди сообщений/задач
- OpenAI-compatible API - API LLM локального сервера для анализа изображения

## Sequence diagram

Диаграмма последовательности описывающая принцип работы с сервисом

```text
┌────────────┐                   ┌───────┐            ┌────────┐ ┌──┐ ┌─────┐   ┌──────┐             ┌───┐
│TestBlackBox│                   │Reciver│            │Postgres│ │S3│ │Kafka│   │Worker│             │LLM│
└──────┬─────┘                   └───┬───┘            └────┬───┘ └─┬┘ └──┬──┘   └───┬──┘             └─┬─┘
       │         Send 2 imgs         │                     │       │     │          │                  │
       │────────────────────────────>│                     │       │     │          │                  │
       │                             │                     │       │     │          │                  │
       │                             │          Save imgs  │       │     │          │                  │
       │                             │────────────────────────────>│     │          │                  │
       │                             │                     │       │     │          │                  │
       │                             │    Save request     │       │     │          │                  │
       │                             │────────────────────>│       │     │          │                  │
       │                             │                     │       │     │          │                  │
       │                             │         Save job with id    │     │          │                  │
       │                             │──────────────────────────────────>│          │                  │
       │                             │                     │       │     │          │                  │
       │           Job id            │                     │       │     │          │                  │
       │< ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│                     │       │     │          │                  │
       │                             │                     │       │     │          │                  │
       │                             │                     │       │     │   Job    │                  │
       │                             │                     │       │     │─────────>│                  │
       │                             │                     │       │     │          │                  │
       │                             │                     │   Get request data     │                  │
       │                             │                     │<───────────────────────│                  │
       │                             │                     │       │     │          │                  │
       │                             │                     │       │ data│          │                  │
       │                             │                     │─ ─ ─ ─│─ ─ ─│─ ─ ─ ─ ─>│                  │
       │                             │                     │       │     │          │                  │
       │                             │                     │       │   Get imgs     │                  │
       │                             │                     │       │<───────────────│                  │
       │                             │                     │       │     │          │                  │
       │                             │                     │       │   binaries     │                  │
       │                             │                     │       │─ ─ ─│─ ─ ─ ─ ─>│                  │
       │                             │                     │       │     │          │                  │
       │                             │                     │       │     │          │ promt with imgs  │
       │                             │                     │       │     │          │─────────────────>│
       │                             │                     │       │     │          │                  │
       │                             │                     │       │     │          │     result       │
       │                             │                     │       │     │          │< ─ ─ ─ ─ ─ ─ ─ ─ │
       │                             │                     │       │     │          │                  │
       │                             │                     │ save result by job id  │                  │
       │                             │                     │<───────────────────────│                  │
       │                             │                     │       │     │          │                  │
       │ Get request info by job id  │                     │       │     │          │                  │
       │────────────────────────────>│                     │       │     │          │                  │
       │                             │                     │       │     │          │                  │
       │                             │ Get data by job id  │       │     │          │                  │
       │                             │────────────────────>│       │     │          │                  │
       │                             │                     │       │     │          │                  │
       │                             │        data         │       │     │          │                  │
       │                             │< ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│       │     │          │                  │
       │                             │                     │       │     │          │                  │
       │        request info         │                     │       │     │          │                  │
       │< ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│                     │       │     │          │                  │
       │                             │                     │       │     │          │                  │
┌──────┴─────┐                   ┌───┴───┐            ┌────┴───┐ ┌─┴┐ ┌──┴──┐   ┌───┴──┐             ┌─┴─┐
│TestBlackBox│                   │Reciver│            │Postgres│ │S3│ │Kafka│   │Worker│             │LLM│
└────────────┘                   └───────┘            └────────┘ └──┘ └─────┘   └──────┘             └───┘
```

### PluntUML

Код для генерации диаграммы последовательности

```pluntuml
@startuml
autonumber
participant TestBlackBox
participant Reciver
database Postgres
collections S3
queue Kafka
participant Worker
participant LLM

TestBlackBox -> Reciver : Send 2 imgs
Reciver -> S3: Save imgs
Reciver -> Postgres : Save request
Reciver -> Kafka: Save job with id
Reciver --> TestBlackBox  : Job id

Kafka -> Worker  : Job
Worker  -> Postgres : Get request data
Postgres --> Worker : data
Worker -> S3 : Get imgs
S3 --> Worker : binaries
Worker -> LLM: promt with imgs
LLM --> Worker : result
Worker -> Postgres : save result by job id

TestBlackBox -> Reciver : Get request info by job id
Reciver -> Postgres : Get data by job id
Postgres --> Reciver : data
Reciver --> TestBlackBox  : request info
@enduml
```
