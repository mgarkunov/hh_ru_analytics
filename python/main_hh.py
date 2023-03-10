#%%
import re
import requests
import pandas as pd
import datetime as dt
import yaml
import sqlalchemy as sa
import psycopg2


### --- Конфигруация логгирования / --- ###
import logging, sys
logger = logging.getLogger('debug')
handler_console = logging.StreamHandler(sys.stdout)
handler_console.setFormatter(logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s', datefmt='%Y-%m-%d %T'))
logger.handlers = []
logger.addHandler(handler_console)
logger.setLevel(logging.DEBUG)
### --- / Конфигруация логгирования --- ###



cfg = yaml.safe_load(open('cfg.yaml', 'r'))
psql = sa.create_engine(f"postgresql://{cfg['psql']['user']}:{cfg['psql']['pwd']}@{cfg['psql']['host']}:{cfg['psql']['port']}/{cfg['psql']['dbname']}")


url_HH = 'https://api.hh.ru/vacancies'
# Исправлять на id
# pкof_id = "(Руководитель отдела аналитики) OR (BI-аналитик, аналитик данных) OR (Аналитик) OR \
#     (Бизнес-аналитик) OR (Продуктовый аналитик) OR (Системный аналитик) OR (Маркетолог-аналитик) \
#         OR (Финансовый аналитик, инвестиционный аналитик) OR (Дата-сайентист)"
# Доработать с учетом синтаксиса поисковых запросов
search = "(Data Engineer OR Data Scientist OR Data Analyst OR data science engineer OR Аналитик данных \
    OR Бизнес аналитик OR финансовый аналитик OR системный аналитик OR системная аналитика OR продуктовый аналитик \
        OR дата инженер OR инженер данных OR devops инженер OR датасайнтист OR\
            (Аналитик AND (систем OR продукт OR бизнес OR данн OR финанс)))"


search

#%%

# # --- Инициализация базы / -- #
# if psql.url.database == 'hh_analytics':
#     psql_con = psql.connect()
#     psql_con.execute(sa.text("""
#     create schema if not exists dwh_stage;
#     commit; -- не забываем комитить работу, так как PSQL транзакционная база
#     """))
#     psql_con.execute(sa.text("""
#     create schema if not exists dwh_mart;
#     commit; -- не забываем комитить работу, так как PSQL транзакционная база
#     """))
#     if psql_con.closed == False:
#         psql_con.close()
# else:
#     raise ValueError('Неправильное название базы')
# # --- / Инициализация базы -- #
#%%

def json_to_flatdf(response):
    """
    Функция преобразования json в плоскую таблицу
    """
    vacancy_response = response.json()
    new_dict = {}
    for key, value in vacancy_response.items():
        if type(value) == dict:
            for item in value.keys():
                if type(value[item]) == dict:
                    for item_2 in value[item].keys():
                        new_dict[f'{key}_{item}_{item_2}'] = value[item][item_2]
                else:
                    new_dict[f'{key}_{item}'] = value[item]
        elif key == 'branded_description':
            pass
        elif key == 'description':
            new_dict[key] = re.sub(r'<.*?>', '', value)
        elif key == 'key_skills':
            help_list = []
            for item in value:
                help_list.append(item['name'])
            new_dict[key] = help_list
        else:
            new_dict[key] = vacancy_response[key]
    
    return pd.DataFrame.from_dict(new_dict, orient='index').transpose()


def load_list_vacancies(start_date:dt.datetime, end_date:dt.datetime, pкof_id:str = "", search:str = ""):
    """
    Функция для загрузки списка вакансий
    НАдо будет сделать цикл, который будет обходит по дням и часам.
    pкof_id - список для id профессий
    search - поиковая фраза
    """
    # проверяем наличие данных в pкof_id и если они есть, то возвращаем результат по pкof_id
    # Если pкof_id и есть данные в search, то выполняем поиск по ключам
    # Если нет данных в pкof_id и search, то возвращаем пустое значение
    # vac_request = f'NAME: {search} OR SPECIALIZATION: {pкof_id}'
    # Где проверка количества страниц и пагинация?
    vac_request = f'NAME: {search}'
    df = pd.DataFrame()
    ids = []
    while start_date < end_date:
        query_params = {
            # 'text': vac_request, 
            'area': 113, 
            'per_page': 10,
            'date_from': start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            'date_to': (start_date + dt.timedelta(hours=1, seconds=-1)).strftime("%Y-%m-%dT%H:%M:%S")
        }
        start_date = start_date + dt.timedelta(hours=1)
        response = requests.get(url=url_HH, params=query_params).json()
        df = pd.concat([df, pd.DataFrame(response['items'])])
    ids = df['id'].to_list()
    return ids



def get_vacancies(id_list):
    """
    Функция для создания массива детального описания всех вакансий по заданным критериям
    """
    vac_df = pd.DataFrame()
    for vac_id in id_list:
        vac_response = requests.get(url=f"{url_HH}/{vac_id}")
        # Где функция пробразования JSON в плоскую таблицу - починил
        vac_df = pd.concat([vac_df, json_to_flatdf(vac_response)])
    return vac_df


def send_data(df):
    """
    После нормализации датафрейм получается на 98 колонок, пробовал закдывать все в базу, но получаю ошибку:
    ProgrammingError: (psycopg2.ProgrammingError) can't adapt type 'dict'
    Пока что её так и не поборол
    В итоге оставил 11 полей, прописал их типы и таки положил в базу.
    """
    cols_list = ['id', 'premium', 'name', 'description', 'key_skills', 'archived', 'specializations', 'professional_roles',
                 'published_at', 'created_at', 'initial_created_at']
    df = df.loc[:, cols_list]
    df.to_sql(
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
# def main():


date_from = dt.datetime(2023,3,10,12)
date_to= dt.datetime(2023,3,10,16)

#%%
# Получаем список подходящих под наши критерии вакансий
id_list = load_list_vacancies(date_from, date_to, search=search)
id_list
#%%

# Создаем датафрейм с детальным описанием
df_with_vac = get_vacancies(id_list)
df_with_vac

#%%
# Загружаем данные в базу
send_data(df_with_vac)


    
# if __name__ == "__main__":
#     main()

# %%

vac_request = f'NAME: {search}'
df = pd.DataFrame()
ids = []
sdt = date_from
edt = sdt + dt.timedelta(hours=1)
while date_from < date_to:
    query_params = {
        'text': vac_request, 
        'area': 113, 
        'per_page': 100,
        'date_from': sdt.strftime("%Y-%m-%dT%H:%M:%S"),
        'date_to': edt.strftime("%Y-%m-%dT%H:%M:%S")
    }
    response = requests.get(url=url_HH, params=query_params).json()
    sdt = sdt + dt.timedelta(hours=1)
    edt = sdt + dt.timedelta(hours=1)
    df = pd.concat([df, pd.DataFrame(response['items'])])


#%%
vac_request = f'NAME: {search}'
sdt = date_from
edt = sdt + dt.timedelta(hours=1)
df = pd.DataFrame()
query_params = {
    # 'text': vac_request, 
    'area': 113, 
    'per_page': 10,
    'date_from': sdt.strftime("%Y-%m-%dT%H:%M:%S"),
    'date_to': edt.strftime("%Y-%m-%dT%H:%M:%S")
}
response = requests.get(url=url_HH, params=query_params).json()
df = pd.concat([df, pd.DataFrame(response['items'])])
df

#%%
df['id'].to_list()