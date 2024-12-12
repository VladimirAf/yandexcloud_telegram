from  database import pool, execute_update_query, execute_select_query
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram import types
from database import quiz_data

def generate_options_keyboard(answer_options, right_answer):
    builder = InlineKeyboardBuilder()

    for option in answer_options:
        builder.add(types.InlineKeyboardButton(
            text=option,
            callback_data="right_answer" if option == right_answer else "wrong_answer")
        )

    builder.adjust(1)
    return builder.as_markup()





async def get_question(message, user_id):
    
    # Получение текущего вопроса из словаря состояний пользователя
    current_question_index, cur_res = await get_quiz_index(user_id)
    print(current_question_index)
    correct_index = quiz_data[current_question_index]['correct_option']
    opts = quiz_data[current_question_index]['options']
    kb = generate_options_keyboard(opts, opts[correct_index])
    await message.answer(f"{quiz_data[current_question_index]['question']}", reply_markup=kb)


async def new_quiz(message):
    user_id = message.from_user.id
    current_question_index = 0
    cur_res = 0
    await update_quiz_index(user_id, current_question_index, cur_res)
    await get_question(message, user_id)


async def get_quiz_index(user_id):
    get_user_index = f"""
        DECLARE $user_id AS Uint64;
        DECLARE $cur_res AS Int32;

        SELECT question_index,cur_res
        FROM `quiz_state`
        WHERE user_id == $user_id;
    """
    results = execute_select_query(pool, get_user_index, user_id=user_id)

    if len(results) == 0:
        return 0, 0 

    row = results[0]
    print(f"Текущий результат структура: {row}")
    # Извлечение данных из строки
    question_index = row["question_index"] if row["question_index"] is not None else 0
    cur_res = row["cur_res"] if row["cur_res"] is not None else 0

    return question_index, cur_res

async def update_quiz_index(user_id, question_index, cur_res):
    set_quiz_state = f"""
        DECLARE $user_id AS Uint64;
        DECLARE $question_index AS Uint64;
        DECLARE $cur_res AS Int32;

        UPSERT INTO `quiz_state` (`user_id`, `question_index`,`cur_res`)
        VALUES ($user_id, $question_index,$cur_res);
    """

    execute_update_query(
        pool,
        set_quiz_state,
        user_id=user_id,
        question_index=question_index,
        cur_res=cur_res,
    )
