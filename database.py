import os
import ydb
import json
from aiogram import types
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

YDB_ENDPOINT = os.getenv("YDB_ENDPOINT")
YDB_DATABASE = os.getenv("YDB_DATABASE")

def get_ydb_pool(ydb_endpoint, ydb_database, timeout=30):
    ydb_driver_config = ydb.DriverConfig(
        ydb_endpoint,
        ydb_database,
        credentials=ydb.credentials_from_env_variables(),
        root_certificates=ydb.load_ydb_root_certificate(),
    )

    ydb_driver = ydb.Driver(ydb_driver_config)
    ydb_driver.wait(fail_fast=True, timeout=timeout)
    return ydb.SessionPool(ydb_driver)


def _format_kwargs(kwargs):
    return {"${}".format(key): value for key, value in kwargs.items()}


# Заготовки из документации
# https://ydb.tech/en/docs/reference/ydb-sdk/example/python/#param-prepared-queries
def execute_update_query(pool, query, **kwargs):
    def callee(session):
        prepared_query = session.prepare(query)
        session.transaction(ydb.SerializableReadWrite()).execute(
            prepared_query, _format_kwargs(kwargs), commit_tx=True
        )

    return pool.retry_operation_sync(callee)


# Заготовки из документации
# https://ydb.tech/en/docs/reference/ydb-sdk/example/python/#param-prepared-queries
def execute_select_query(pool, query, **kwargs):
    def callee(session):
        prepared_query = session.prepare(query)
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            prepared_query, _format_kwargs(kwargs), commit_tx=True
        )
        return result_sets[0].rows

    return pool.retry_operation_sync(callee)    
   

# Зададим настройки базы данных 
pool = get_ydb_pool(YDB_ENDPOINT, YDB_DATABASE)


get_quizdata = """
    SELECT question,options,correct_option
    FROM `quiz_data`
"""
results = execute_select_query(pool, get_quizdata)

print(f"Текущий результат структура: {results}")
print("Тип данных : ", type(results))

if len(results) == 0:
    quiz_data = [
        {
            'question': 'Что такое Python?',
            'options': ['Язык программирования', 'Тип данных', 'Музыкальный инструмент', 'Змея на английском'],
            'correct_option': 0
        },
        {
            'question': 'Какой тип данных используется для хранения целых чисел?',
            'options': ['int', 'float', 'str', 'natural'],
            'correct_option': 0
        },
        {
            'question': 'Какой метод используется для добавления элемента в список?',
            'options': ['append()', 'add()', 'insert()', 'push()'],
            'correct_option': 0
        },
        {
            'question': 'Какой результат у выражения 5 // 2 в Python?',
            'options': ['2', '2.5', '1', '5.2'],
            'correct_option': 0
        },
        {
            'question': 'Какая функция используется для получения длины строки?',
            'options': ['len()', 'size()', 'length()', 'count()'],
            'correct_option': 0
        },
        {
            'question': 'Какой оператор используется для проверки равенства?',
            'options': ['==', '=', '!=', '==='],
            'correct_option': 0
        },
        {
            'question': 'Что делает метод .split() в Python?',
            'options': ['Разделяет строку на части', 'Объединяет строки', 'Удаляет пробелы', 'Переводит строку в верхний регистр'],
            'correct_option': 0
        },
        {
            'question': 'Какая функция используется для преобразования строки в целое число?',
            'options': ['int()', 'float()', 'str()', 'eval()'],
            'correct_option': 0
        },
        {
            'question': 'Какой модуль используется для работы с рандомными числами?',
            'options': ['random', 'math', 'os', 'sys'],
            'correct_option': 0
        },
        {
            'question': 'Что возвращает функция range(5)?',
            'options': ['Итератор', 'Список [0, 1, 2, 3, 4]', 'Число 5', 'Кортеж'],
            'correct_option': 0
        }
    ]

    data = [
        {
            'id': i + 1,  # Если требуется ID, создайте его вручную
            'question': item['question'],
            'options': json.dumps(item['options']),  # Преобразуем список в JSON
            'correct_option': item['correct_option']
        }
        for i, item in enumerate(quiz_data)
    ]

    set_quiz_data = f"""
        DECLARE $data AS List<Struct<
            id: Uint64,
            question: Utf8,
            options: Utf8,
            correct_option: Uint64
        >>;

        UPSERT INTO quiz_data
        SELECT
            id,
            question,
            options,
            correct_option
        FROM AS_TABLE($data);
    """

    execute_update_query(
        pool,
        set_quiz_data,
        data=data,
    )
else:
    for item in results:
        if "options" in item and isinstance(item["options"], str):
            try:
                item["options"] = json.loads(item["options"])  # Декодирование JSON-строки в список
            except json.JSONDecodeError as e:
                print(f"Ошибка декодирования JSON: {e}, options: {item['options']}")
    quiz_data = results






