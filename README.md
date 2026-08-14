# BionicPRO — 9 спринт

Решение проектной работы: усиление безопасности SSO, сервис отчётов на OLAP,
кеширование отчётов в S3 и CDN, перевод выгрузок из CRM на CDC.

## Состав репозитория

| Каталог | Что внутри |
|---|---|
| `docs/` | Диаграммы C4 в draw.io: исходная и новая (`BionicPRO_C4_sprint9.drawio.xml`, две страницы), скриншоты поднятого стенда в `screenshots/` |
| `bionicpro-auth/` | Бэкенд аутентификации (BFF): PKCE, сессии, работа с токенами |
| `frontend/` | React-приложение, работает только с сессионной cookie |
| `reports-api/` | Сервис отчётов: ClickHouse + S3 + CDN |
| `keycloak/` | Экспорт realm после всех настроек — `keycloak-results-export.json` |
| `ldap/` | Конфигурация пользователей и ролей OpenLDAP |
| `airflow/` | DAG ETL из CRM в ClickHouse и SQL витрины |
| `clickhouse/` | DDL таблиц, тестовая телеметрия, KafkaEngine и MaterializedView для CDC |
| `debezium/` | Конфигурация коннектора захвата изменений из CRM |
| `nginx/` | Reverse proxy с кешированием — эмуляция CDN |
| `crm/` | Схема и тестовые данные CRM |

## Запуск

Нужен Docker с плагином compose, свободные порты из таблицы ниже и
примерно 6 ГБ памяти — стенд поднимает Keycloak, ClickHouse, Kafka и Airflow.

```bash
docker compose up -d --build     # первый запуск занимает 5–10 минут
./debezium/register-connector.sh # CDC для задания 4, после старта стенда
```

Отдельно ничего инициализировать не нужно: Airflow сам заводит пользователя,
снимает DAG с паузы и считает витрину `user_report_mart` через минуту после
старта, а `register-connector.sh` дожидается снапшота Debezium и считает
CDC-витрину `user_report_mart_cdc`.

Проверить, что всё поднялось:

```bash
docker compose ps
docker compose exec airflow airflow dags list-runs -d crm_to_clickhouse_etl
curl -s http://localhost:8083/connectors/crm-connector/status
```

Остановить и убрать данные: `docker compose down -v`.

### Адреса и учётные записи

| Сервис | Адрес | Учётные данные |
|---|---|---|
| Фронтенд | http://localhost:3000 | вход через Keycloak, `prothetic1 / prothetic1` |
| Keycloak | http://localhost:8080 | админка — `admin / admin` |
| bionicpro-auth | http://localhost:8000 | только по сессионной cookie |
| Сервис отчётов | http://localhost:8100 | только по access-токену |
| CDN (Nginx) | http://localhost:8081 | по подписанной ссылке из отчёта |
| Airflow | http://localhost:8082 | `admin / admin` |
| MinIO | http://localhost:9001 | `minioadmin / minioadmin` |
| Kafka Connect | http://localhost:8083 | — |
| ClickHouse | http://localhost:8123 | `analytics / analytics_password`, база `bionicpro` |
| CRM (PostgreSQL) | localhost:5436 | `crm_user / crm_password`, база `crm` |

Пользователи реалма `reports-realm` — пароль совпадает с логином, кроме
пользователей из LDAP:

| Логин | Пароль | Роль | Откуда | Что показывает |
|---|---|---|---|---|
| `prothetic1` | `prothetic1` | `prothetic_user` | Keycloak | основной сценарий: отчёт по двум протезам |
| `prothetic2` | `prothetic2` | `prothetic_user` | Keycloak | второй владелец — на нём видно, что чужой отчёт не отдаётся |
| `prothetic3` | `prothetic3` | `prothetic_user` | Keycloak | ещё один владелец |
| `user1`, `user2` | `user1`, `user2` | `user` | Keycloak | без роли `prothetic_user` — отчёт закрыт |
| `john.doe` | `password` | `prothetic_user` | LDAP | роль приезжает из группы каталога |
| `jane.smith` | `password` | `user` | LDAP | LDAP-пользователь без доступа к отчётам |
| `alex.johnson` | `password` | `prothetic_user` | LDAP | ещё один пользователь представительства |

### Первый вход

1. Откройте http://localhost:3000 и нажмите «Войти» — фронтенд уходит
   в Keycloak, токенов он не видит.
2. Введите `prothetic1 / prothetic1`.
3. Keycloak попросит настроить одноразовый пароль — отсканируйте QR-код
   в Google Authenticator или FreeOTP и введите код. Дальше вход всегда
   в два шага.
4. Фронтенд покажет экран согласия на обработку данных. Без согласия отчёты
   не отдаются — это часть задания 1.
5. Выберите период и нажмите «Получить отчёт».

## Задание 1. Безопасность

**Схема работы с токенами.** Фронтенд больше не общается с Keycloak напрямую и
вообще не видит токены. Всё через `bionicpro-auth`:

1. Пользователь нажимает «Войти» → `GET /auth/login`.
2. Сервис генерирует `state` и `code_verifier`, кладёт их в Redis на 5 минут и
   редиректит в Keycloak с `code_challenge` (метод S256).
3. Keycloak возвращает пользователя на `GET /auth/callback`, сервис проверяет
   `state`, меняет `code` на токены, отправляя `code_verifier`.
4. Токены шифруются (Fernet) и кладутся в Redis. Браузеру уходит только
   идентификатор сессии в cookie с флагами `HttpOnly`, `Secure`, `SameSite`.
5. Access-токен живёт 2 минуты, сессия — 30 минут. Когда access-токен протухает,
   сервис сам идёт в Keycloak за новым по refresh-токену, пользователь этого не замечает.
6. На каждом обращении к защищённому ресурсу токены перепривязываются к новому
   идентификатору сессии, cookie обновляется — это закрывает session fixation.
   Отключается переменной `ROTATE_SESSION=false`.

**PKCE.** Клиент `reports-frontend` публичный, в его настройках выставлено
`pkce.code.challenge.method = S256`. Без `code_verifier` обмен кода не проходит —
перехваченный код бесполезен.

**LDAP.** Keycloak ходит в OpenLDAP как в источник учётных записей представительства
(`ou=People,dc=example,dc=com`), режим `READ_ONLY` — персональные данные остаются в
каталоге страны и не копируются в БД Keycloak сверх необходимого. Роли
подтягиваются маппером `role-ldap-mapper` из `ou=Groups`: группа `prothetic_user`
превращается в одноимённую роль реалма, поэтому пользователи разных
представительств получают одинаковые права.

**MFA.** В реалме включена политика TOTP, действие `CONFIGURE_TOTP` назначено
всем пользователям по умолчанию.

**Яндекс ID.** Настроен как внешний провайдер через Identity Brokering. После
входа `bionicpro-auth` показывает экран согласия на обработку данных; если
пользователь соглашается, сервис запрашивает профиль в Keycloak (`/userinfo`,
данные приходят из Яндекса) и сохраняет его в таблицу `user_profiles` в CRM.
Без согласия отчёты недоступны.

В экспорте реалма `clientId` и `clientSecret` провайдера оставлены заглушками
`changeme`: Keycloak 21 не подставляет переменные окружения при импорте реалма.
Свои реквизиты приложения нужно прописать руками — Keycloak → Identity Providers →
yandex. Redirect URI для приложения в Яндексе:
`http://localhost:8080/realms/reports-realm/broker/yandex/endpoint`.

## Задание 2. Сервис отчётов

Данные телеметрии лежат в ClickHouse (`telemetry_raw`), справочники клиентов и
протезов приезжают из CRM. Airflow ежедневно в 02:00 собирает витрину
`user_report_mart` с сортировкой `(user_id, report_date, serial_number)` —
выборка по одному пользователю читает минимум данных.

`GET /reports?from=&to=` возвращает отчёт только по владельцу токена:
идентификатор берётся из `preferred_username`, параметр `user` с чужим значением
даёт 403. Дополнительно проверяется роль `prothetic_user`.

Пользователь может запросить период, которого ещё нет в OLAP. Поэтому сервис
сначала читает `etl_watermark` — дату, по которую витрина посчитана — и обрезает
по ней конец периода. Если запрошенное начало периода позже этой границы,
возвращается 409 с пояснением.

## Задание 3. S3 и CDN

Готовый отчёт складывается в MinIO по ключу
`reports/<user_id>/<витрина>/<версия витрины>/<период>.json`. Версия — это дата из
`etl_watermark`, поэтому после каждого прогона ETL путь меняется и кеш CDN
инвалидируется сам собой, без ручного сброса.

При запросе сервис сначала проверяет наличие объекта в S3. Если объект есть —
запрос в ClickHouse не выполняется вообще, сразу отдаётся ссылка. Если нет —
отчёт считается, кладётся в S3 и только потом отдаётся ссылка.

Ссылка ведёт на Nginx, который проксирует MinIO и кеширует ответы на сутки.
Ссылка подписанная (SigV4, 15 минут) — по чужому пути отчёт не забрать. Ключ
кеша строится только по пути (`proxy_cache_key "$uri"`), поэтому разные подписи
одного и того же отчёта попадают в один и тот же объект кеша.

## Задание 4. CDC

Массовые выгрузки из CRM больше не идут запросами в OLTP-базу. Debezium читает
WAL PostgreSQL и пишет изменения в топики `crm.public.clients` и
`crm.public.prostheses`. ClickHouse забирает их таблицами с движком Kafka,
материализованные представления раскладывают события в `crm_clients_cdc` и
`crm_prostheses_cdc` (ReplacingMergeTree, удаления помечаются `is_deleted`).
Витрина `user_report_mart_cdc` собирается материализованным представлением.

Переключение сервиса отчётов на новую витрину:

```bash
REPORT_MART_TABLE=user_report_mart_cdc docker compose up -d reports-api
```

Подробности и проверка — в [debezium/README.md](debezium/README.md).

## Проверка вручную

```bash
# без аутентификации отчёт не отдаётся
curl -i http://localhost:8100/reports

# чужой отчёт не отдаётся даже с валидным токеном
curl -i "http://localhost:8000/api/reports?user=prothetic2" -b "bp_session=<ваша сессия>"

# витрина посчитана, водяной знак сдвинут
docker compose exec clickhouse clickhouse-client -u analytics --password analytics_password \
  -d bionicpro -q "select count() from user_report_mart; select max(processed_until) from etl_watermark"

# CDC: правка в CRM доезжает в ClickHouse за секунды
docker compose exec crm_db psql -U crm_user -d crm \
  -c "update clients set full_name='Проверка CDC' where user_id='prothetic1'"
docker compose exec clickhouse clickhouse-client -u analytics --password analytics_password \
  -d bionicpro -q "select user_id, full_name from crm_clients_cdc final where user_id='prothetic1'"
```

Ссылку на отчёт из ответа можно дёрнуть дважды подряд: первый раз Nginx
ответит `X-Cache-Status: MISS`, второй — `HIT`. Подмена пути на чужой
(`/reports/prothetic2/...`) даёт 403, потому что подпись считается по пути.

## Скриншоты

Снято на стенде, поднятом с нуля — `docs/screenshots/`:

| Файл | Что на нём |
|---|---|
| `01-keycloak-admin.png` | админка Keycloak под `admin/admin`, пользователи реалма |
| `02-airflow-login.png`, `03-airflow-dag.png` | вход в Airflow под `admin/admin` и успешный прогон ETL |
| `04-frontend-start.png` | стартовый экран фронтенда |
| `05-keycloak-login.png`, `06-keycloak-otp.png` | вход `prothetic1` и второй шаг с одноразовым кодом |
| `07-frontend-consent-request.png`, `08-frontend-consent-granted.png` | согласие на обработку данных |
| `09-frontend-report.png` | сформированный отчёт со ссылкой на скачивание |
| `10-minio-reports.png` | объект отчёта в бакете MinIO |
| `11-debezium-status.png` | статус CDC-коннектора |
| `checks.txt` | вывод проверок: 401 без токена, 403 на чужой отчёт, состояние витрин |
