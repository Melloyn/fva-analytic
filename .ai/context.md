# .ai/context.md — fva-analytic

## Краткая карта репозитория
- `backend/analytics/`: финансовая аналитика, KPI, расчетные модели.
- `backend/ingestion/`: прием и нормализация входных данных.
- `backend/utils/`: общие вспомогательные функции.
- `bot/`: Telegram-бот на aiogram.
- `dashboard/`: Streamlit-интерфейс.
- `config/`: конфигурационные файлы проекта.
- `data/`: рабочие данные в рамках принятой структуры.
- `requirements.txt`: зависимости Python.

## Производственный контекст
- Production path: `/opt/projects/fva-analytic`
- Сервисы: `fva-bot`, `fva-streamlit`
- Деплой уже настроен и должен сохраняться:
  `git push` → GitHub → webhook → VPS listener → deploy script → `systemctl restart services`

## Архитектурные границы
- Логика аналитики только в `backend/analytics/`.
- Логика ingestion только в `backend/ingestion/`.
- Общие функции только в `backend/utils/`.
- Логика Telegram-бота только в `bot/`.
- UI Streamlit только в `dashboard/`.

## Do not cross
- Не менять deploy/webhook/systemd без явного подтверждения.
- Не переносить доменную логику в UI.
- Не делать широкие рефакторинги без отдельного запроса.
- Не выходить за границы репозитория.
