#%%
import re
import requests
import pandas as pd
import datetime as dt
import yaml
import sqlalchemy as sa
import psycopg2
from flatten_json import flatten
import json
import os


### --- Конфигруация логгирования / --- ###
import logging, sys
logger = logging.getLogger('debug')
handler_console = logging.StreamHandler(sys.stdout)
handler_console.setFormatter(logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s', datefmt='%Y-%m-%d %T'))
logger.handlers = []
logger.addHandler(handler_console)
logger.setLevel(logging.DEBUG)
### --- / Конфигруация логгирования --- ###


cfg = yaml.safe_load(open('/home/serge/pet_project/cfg.yaml', 'r'))
psql = sa.create_engine(f"postgresql://{cfg['psql']['user']}:{cfg['psql']['pwd']}@{cfg['psql']['host']}:{cfg['psql']['port']}/{cfg['psql']['dbname']}")


url_HH = 'https://api.hh.ru/vacancies'
pкof_id = [
    10, # Аналитик
    # 40, # Другое - пока что спрятал в коммент, потому что в этот раздел поподает вообще всё, что не подошло к другим специализациям
    134, # Финансовый аналитик, инвестиционный аналитик
    148, # Системный аналитик
    150, # Бизнес-аналитик
    156, # BI-аналитик, аналитик данных
    157, # Руководитель отдела аналитики
    160, # DevOps-инженер
    163, # Маркетолог-аналитик
    164, # Продуктовый аналитик
    165, # Дата-сайентист
    ]
search = "NAME:((Data Engineer) OR (Data Scientist) OR (Data Analyst) OR (data science engineer) \
    OR (Аналитик данных) OR (Бизнес аналитик) OR (финансовый аналитик) OR (системный аналитик) \
        OR (системная аналитика) OR (продуктовый аналитик) OR (дата инженер) OR (инженер данных) \
            OR (devops инженер) OR (датасайнтист) OR (Аналитик AND (систем* OR продукт* OR бизнес* OR данн* OR финанс*)))"


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
#%%

def json_to_flatdf(response):
    """
    Функция преобразования в плоскую таблицу
    """
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(response)
    return pd.DataFrame([out])


def load_list_vacancies(start_date:dt.date, end_date:dt.date, pкof_id:list = [], search:str = ""):
    """
    Функция для загрузки списка вакансий
    НАдо будет сделать цикл, который будет обходит по дням и часам.
    pкof_id - список для id профессий
    search - поиковая фраза
    """
    df = pd.DataFrame()
    while start_date < end_date:
        # сначала исследуем запрос с поисковой фразой в названии
        query_params = {
            'text': search, 
            'area': 113, 
            'per_page': 100,
            'date_from': start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            'date_to': (start_date + dt.timedelta(hours=1, seconds=-1)).strftime("%Y-%m-%dT%H:%M:%S")
            }
        page = 0
        while True:
            query_params['page'] = page
            response = requests.get(url=url_HH, params=query_params).json()
            print(response['found'])
            df = pd.concat([df, pd.DataFrame(response['items'], index=None)])
            if page < (response['pages'] - 1):
                page += 1
                print(page, response['pages'], end='\r')
            else:
                break
        # далее исследуем запрос по профессиональным ролям
        query_params = {
            'professional_role': pкof_id, 
            'area': 113, 
            'per_page': 100,
            'date_from': start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            'date_to': (start_date + dt.timedelta(hours=1, seconds=-1)).strftime("%Y-%m-%dT%H:%M:%S")
            }
        page = 0
        while True:
            query_params['page'] = page
            response = requests.get(url=url_HH, params=query_params).json()
            print(response['found'])
            df = pd.concat([df, pd.DataFrame(response['items'], index=None)])
            if page < (response['pages'] - 1):
                page += 1
                print(page, response['pages'], end='\r')
            else:
                break
        start_date = start_date + dt.timedelta(hours=1)
        os.system("clear")
    # в конце берем столбец id из получившегося датафрейма и возвращаем в качестве списка
    ids = set(df['id'].to_list())
    print(len(ids))
    return ids


def get_vacancies(id_list):
    """
    Функция для создания массива детального описания всех вакансий по заданным критериям
    """
    vacs_list = []
    for vac_id in id_list:
        print(vac_id)
        vac_response = requests.get(url=f"{url_HH}/{vac_id}").json()
        vac_response['description'] = re.sub(r'<.*?>', '', vac_response['description'])
        del vac_response['branded_description']
        vacs_list.append(vac_response)
    
    with open(f"{dt.date.today()}_test.json", "w") as f:
        json.dump(vacs_list, f, indent=4, ensure_ascii=False)
    vac_df = pd.DataFrame(vacs_list)
    vac_df['tech_change_date'] = dt.datetime.now()
    os.system("clear")
    
    return vac_df


def send_data(df):
    """
    Функция записи датафрейма с набором вакансий в базу данных
    """
    df.to_sql(
        name='vacancies_list',
        con=psql,
        schema='dwh_stage',
        if_exists='append',
        index=False,
        dtype={
            'id': sa.types.Integer, 
            'premium': sa.types.Boolean, 
            'billing_type': sa.types.JSON,
            'relations': sa.types.String,
            'name': sa.types.String, 
            'insider_interview': sa.types.Boolean, 
            'response_letter_required': sa.types.Boolean, 
            'area': sa.types.JSON,
            'salary': sa.types.JSON, 
            'type': sa.types.JSON,
            'address': sa.types.JSON,
            'allow_messages': sa.types.Boolean,
            'experience': sa.types.JSON,
            'schedule': sa.types.JSON,
            'employment': sa.types.String, 
            'department': sa.types.JSON,
            'contacts': sa.types.String,
            'description': sa.types.Text,
            'vacancy_constructor_template': sa.types.String,
            'key_skills': sa.types.JSON,
            'accept_handicapped': sa.types.Boolean,
            'accept_kids': sa.types.Boolean,
            'archived': sa.types.Boolean,
            'response_url': sa.types.String,
            'specializations': sa.types.JSON,
            'professional_roles': sa.types.JSON, 
            'code': sa.types.String,
            'hidden': sa.types.Boolean,
            'quick_responses_allowed': sa.types.Boolean,
            'driver_license_types': sa.types.JSON,
            'accept_incomplete_resumes': sa.types.Boolean, 
            'employer': sa.types.JSON,
            'published_at': sa.types.DATE,
            'created_at': sa.types.DATE, 
            'initial_created_at': sa.types.DATE, 
            'negotiations_url': sa.types.String,
            'suitable_resumes_url': sa.types.String, 
            'apply_alternate_url': sa.types.String, 
            'has_test': sa.types.Boolean, 
            'test': sa.types.Boolean,
            'alternate_url': sa.types.String, 
            'working_days': sa.types.JSON,
            'working_time_intervals': sa.types.JSON,
            'working_time_modes': sa.types.String, 
            'accept_temporary': sa.types.Boolean,
            'languages': sa.types.JSON,
            'tech_change_date': sa.types.TIMESTAMP
        }
    )


#%%
def main():

    start_date = dt.datetime.today() - dt.timedelta(days=30)
    end_date = dt.datetime.today() - dt.timedelta(days=27)

    
    # Получаем список подходящих под наши критерии вакансий
    id_list = load_list_vacancies(start_date, end_date, pкof_id, search)
    # Создаем датафрейм с детальным описанием
    df_with_vac = get_vacancies(id_list)
    # Загружаем данные в базу
    df_with_vac.to_csv(f'{start_date}_{end_date}.csv')
    send_data(df_with_vac)


if __name__ == "__main__":
    main()