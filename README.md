# Кейс 2: Расчёт себестоимости при позаказном производстве

Учебный проект моделирует позаказное производство с калькуляцией себестоимости по заказам:
- материалы (`bom`),
- труд (`labor`),
- накладные (`overhead`).

## Что реализовано

- **Схема данных** (SQLite): `orders`, `bom`, `labor`, `overhead` + служебные таблицы (`change_log`, `error_log`, `exchange_log`, `order_cost_snapshot`).
- **Формулы**:
  - `materials = Σ(qty*unit_cost)`
  - `labor = Σ(hours*rate)`
  - `cost = materials + labor + Σ(overhead)`
  - `margin = price - cost` (если `price` задана, иначе `n/a`).
- **Валидации**:
  - положительные количества/ставки/суммы (`CHECK > 0`);
  - невозможность добавить строку затрат без существующего `order_id` (FK);
  - контроль полноты данных при переводе заказа в `completed/closed`.
- **Бизнес-правила**:
  - статусы: `opened → in_progress → completed → closed`;
  - перевод в `completed/closed` требует наличия материалов, труда, накладных;
  - закрытый заказ нельзя редактировать (только через корректирующий документ — в демо ограничено запретом редактирования);
  - в отчёте отображается версия правил распределения накладных (`rule_version`).
- **Отчёты**:
  - `report-order` — отчёт «Себестоимость заказа»;
  - `report-period` — выборка за период с фильтрами;
  - `top` — TOP-N по марже или перерасходу;
  - `export-csv` — экспорт в CSV.
- **Интеграция CSV-макетами**:
  - `import-csv --folder ...` (с транзакцией, логом ошибок и rollback до последней корректной версии).
- **Синтетика**:
  - минимум 3 заказа;
  - минимум 100 строк затрат (в сидере: 111 строк).

## Запуск

```bash
python3 job_order_costing.py init-db
python3 job_order_costing.py seed
python3 job_order_costing.py report-order --order-id ORD-001
python3 job_order_costing.py report-period --date-from 2026-01-01 --date-to 2026-12-31
python3 job_order_costing.py export-csv --date-from 2026-01-01 --date-to 2026-12-31 --out exports/cost_report.csv
python3 job_order_costing.py form --order-id ORD-001
python3 job_order_costing.py top --by margin --n 3
```

## KPI (пример проверки)

Точность расчёта на тестовых заказах можно проверять так:

`|эталон - расчёт| / эталон <= 5%`

В проекте можно добавить эталонные значения в отдельную таблицу/CSV и сравнить с `total_cost`.
