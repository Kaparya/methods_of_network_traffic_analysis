# Salary Regression Model

Модуль для обучения и использования регрессионной модели предсказания зарплаты на основе предобработанных данных.

## Структура проекта

- **`main.py`**: Точка входа в приложение (CLI).
- **`src/model.py`**: Логика обучения (CatBoostRegressor), оценки и сохранения модели.
- **`src/data_loader.py`**: Загрузка датасетов `features.npy` и `target.npy`.
- **`src/inference.py`**: Функция для получения предсказаний на новых данных.
- **`src/config.py`**: Конфигурация путей и логирования.

## Требования

Перед запуском убедитесь, что:
1. Выполнен парсинг данных в модуле `parsing`.
2. Файлы `features.npy` и `target.npy` находятся в папке `parsing/`.

## Использование

### 1. Обучение модели
Запускает процесс обучения, рассчитывает метрики (MAE, RMSE, R2) и сохраняет веса модели в `resources/salary_model.cbm`.

```bash
python main.py --train
```

### 2. Инференс (Предсказание)
Принимает путь к файлу с признаками (`.npy`) и сохраняет предсказания в файл `y_pred.npy` в той же директории.

```bash
python main.py <path/to/features.npy>

python main.py parsing/features.npy
```

## Модель

Используется **CatBoostRegressor**.
*   **Библиотека**: `catboost`
*   **Параметры**: Используются дефолтные параметры с фиксированным `random_state`.
*   **Метрики**:
    *   `MAE` (Mean Absolute Error)
    *   `RMSE` (Root Mean Squared Error)
    *   `R2 Score` (Коэффициент детерминации)
