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

async def update_quiz_data():
    data = [
        {
            'question': 'Что такое Python?',
            'options': json.dumps(['Язык программирования', 'Тип данных', 'Музыкальный инструмент', 'Змея на английском']),
            'correct_option': 0
        },
        {
            'question': 'Какой тип данных используется для хранения целых чисел?',
            'options': json.dumps(['int', 'float', 'str', 'natural']),
            'correct_option': 0
        },
        {
            'question': 'Какой метод используется для добавления элемента в список?',
            'options': json.dumps(['append()', 'add()', 'insert()', 'push()']),
            'correct_option': 0
        },
        {
            'question': 'Какой результат у выражения 5 // 2 в Python?',
            'options': json.dumps(['2', '2.5', '1', '5.2']),
            'correct_option': 0
        },
        {
            'question': 'Какая функция используется для получения длины строки?',
            'options': json.dumps(['len()', 'size()', 'length()', 'count()']),
            'correct_option': 0
        },
        {
            'question': 'Какой оператор используется для проверки равенства?',
            'options': json.dumps(['==', '=', '!=', '===']),
            'correct_option': 0
        },
        {
            'question': 'Что делает метод .split() в Python?',
            'options': json.dumps(['Разделяет строку на части', 'Объединяет строки', 'Удаляет пробелы', 'Переводит строку в верхний регистр']),
            'correct_option': 0
        },
        {
            'question': 'Какая функция используется для преобразования строки в целое число?',
            'options': json.dumps(['int()', 'float()', 'str()', 'eval()']),
            'correct_option': 0
        },
        {
            'question': 'Какой модуль используется для работы с рандомными числами?',
            'options': json.dumps(['random', 'math', 'os', 'sys']),
            'correct_option': 0
        },
        {
            'question': 'Что возвращает функция range(5)?',
            'options': json.dumps(['Итератор', 'Список [0, 1, 2, 3, 4]', 'Число 5', 'Кортеж']),
            'correct_option': 0
        }
    ]
    new_quiz_data = f"""
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
        new_quiz_data,
        data=data,
    )
    

# Зададим настройки базы данных 
pool = get_ydb_pool(YDB_ENDPOINT, YDB_DATABASE)


get_data = """
    SELECT question,options,correct_option
    FROM `quiz_data`
"""
results = execute_select_query(pool, get_data)

if len(results) == 0:
    return 0

print(f"Текущий результат структура: {results}")

return results


quiz_data = get_quiz_data()
