#%%
import requests
import pandas as pd
import datetime as dt
import yaml
import sqlalchemy as sa

cfg = yaml.safe_load(open('cfg.yaml', 'r'))
psql = sa.create_engine(f"postgresql://{cfg['psql']['user']}:{cfg['psql']['pwd']}@{cfg['psql']['host']}:{cfg['psql']['port']}/{cfg['psql']['dbname']}")
url_HH = 'https://api.hh.ru/vacancies'

#%%

"""
В скрипте пропущен тонкий момент с пагинацией, обязательно добавлю!!!
TO-DO:
1. Учемть пагинацию
2. Переписать get_raw_data, сделать понятнее
3. Разобраться с send_data, чтобы грузить весь массив данных
"""


def set_query(date_from, date_to):
    """
    Функция для подготовки поискового запроса
    На неделе созванивались с Димой, он подготовил файлик максимально полоно содержащий список необходимых нам вакансий, файлик прикладываю рядышком
    """ 
    with open('search_key_words.txt', 'r', encoding='utf8') as file:
        lines = file.readlines()
        lines = [line.strip() for line in lines]
        query = ' OR '.join(lines)
        vac_request = f'NAME:({query})'
    query_params = {
        'text': vac_request, 
        'area': 113, 
        'per_page': 100,
        'date_from': date_from,
        'date_to': date_to
        }
    return query_params


# Функцию переделать и надо уточнять общее количество страниц
def get_raw_data(query_params):
    """
    Функция для сбора всех вакансий, соотвествующих поисковому запросу, её, буду переписывать, оставил пока так, как была изначально, когда
    была идея делать 2 базы, тут много лишнего
    """
    df = pd.DataFrame()
    for page in range(20):
        query_params['page'] = page
        response = requests.get(url=url_HH, params=query_params).json()
        df = pd.concat([df, pd.DataFrame(response['items'])])
    return list(set(df.id.head(10)))



def get_vacancies(id_list):
    """
    Функция для создания массива детального описания всех вакансий по заданным критериям
    """
    # global url_HH 
    vac_df = pd.DataFrame()
    for vac_id in id_list:
        vac_responce = requests.get(url=f"{url_HH}/{vac_id}").json()
        # Где функция пробразования JSON в плоскую таблицу
        vac_df = pd.concat([vac_df, pd.DataFrame(pd.json_normalize(vac_responce))])
    return vac_df


def send_data(df):
    """
    После нормализации датафрейм получается на 98 колонок, пробовал закдывать все в базу, но получаю ошибку:
    ProgrammingError: (psycopg2.ProgrammingError) can't adapt type 'dict'
    Пока что её так и не поборол
    В итоге оставил 11 полей, прописал их типы и таки положил в базу.
    """
    global psql
    cols_list = ['id', 'premium', 'name', 'description', 'key_skills', 'archived', 'specializations', 'professional_roles',
                 'published_at', 'created_at', 'initial_created_at']
    # Заччем использовать еще одну переменную?
    base_df = df.loc[:, cols_list]
    base_df.to_sql(
        name='list_of_vacancies',
        con=psql,
        schema='dwh_stage',
        if_exists='replace',
        index=False,
        dtype={
            'id': sa.types.Integer,
            'premium': sa.types.Boolean,
            'name': sa.types.String,
            'description': sa.types.String,
            'key_skills': sa.types.JSON,
            'archived': sa.types.Boolean,
            'specializations': sa.types.JSON,
            'professional_roles': sa.types.JSON,
            'published_at': sa.types.DATE,
            'created_at': sa.types.DATE,
            'initial_created_at': sa.types.DATE
        }
    )


#%%
def main():
   



    """
    ПО поводу фиксации дат, помню нашу прошлую встречу, поправлю, упустил этот момент, пока разбирался с подгрузкой в базу..
    """

    # date_from = '2023-02-20'
    # date_to = '2023-02-20'

    date_from = dt.date.today() - dt.timedelta(days=31)
    date_to = dt.date.today() - dt.timedelta(days=1)
    
    # --- Инициализация базы / -- #
    if psql.url.database == 'hh_analytics':
        psql_con = psql.connect()
        psql_con.execute(sa.text("""
        create schema if not exists dwh_stage;
        commit; -- не забываем комитить работу, так как PSQL транзакционная база
        """))
        psql_con.execute(sa.text("""
        create schema if not exists dwh_mart;
        commit; -- не забываем комитить работу, так как PSQL транзакционная база
        """))
        if psql_con.closed == False:
            psql_con.close()
    else:
        raise ValueError('Неправильное название базы')
    # --- / Инициализация базы -- #
    
    # Создаем параметры для подключения к api
    query_params = set_query(date_from=date_from, date_to=date_to)
    # Получаем список подходящих под наши критерии вакансий
    id_list = get_raw_data(query_params)
    # Создаем датафрейм с детальным описанием
    df_with_vac = get_vacancies(id_list)
    # Загружаем данные в базу
    send_data(df_with_vac)
    

    
if __name__ == "__main__":
    main()
