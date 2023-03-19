#%%
import re
import requests
import pandas as pd
import datetime as dt
import yaml
import sqlalchemy as sa
import psycopg2
from flatten_json import flatten


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
    40, # Другое
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
    Функция преобразования json в плоскую таблицу
    """
    new_dict = {}
    # проходим по словарю и проверяем наличие словарей в значениях, если такие присутствуют, то раскладываем
    for key, value in response.items():
        if type(value) == dict:
            for item in value.keys():
                if type(value[item]) == dict:
                    for item_2 in value[item].keys():
                        new_dict[f'{key}_{item}_{item_2}'] = value[item][item_2]
                else:
                    new_dict[f'{key}_{item}'] = value[item]
    # далее исследуем значения, где могут быть списки со словарями, укладываем их просто в список без словарей
        elif key == 'professional_roles':
            sl = []
            for item in value:
                try:
                    sl.append(item['name'])
                except:
                    pass
            new_dict[key] = sl
        elif key == 'description':
            new_dict[key] = re.sub(r'<.*?>', '', value)
        elif key == 'key_skills':
            help_list = []
            for item in value:
                help_list.append(item['name'])
            new_dict[key] = help_list
        else:
            new_dict[key] = value
    new_dict = flatten(new_dict, root_keys_to_ignore={'professional_roles', 'description', 'key_skills'})
    final_df = pd.DataFrame([new_dict])
    return final_df


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
        response = requests.get(url=url_HH, params=query_params).json()
        # проходим по страницам
        for page in range(response['pages']):
            query_params['page'] = page
            response = requests.get(url=url_HH, params=query_params).json()
            df = pd.concat([df, pd.DataFrame(response['items'])])
        # далее исследуем запрос по профессиональным ролям
        query_params = {
            'professional_role': pкof_id, 
            'area': 113, 
            'per_page': 100,
            'date_from': start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            'date_to': (start_date + dt.timedelta(hours=1, seconds=-1)).strftime("%Y-%m-%dT%H:%M:%S")
            }
        response = requests.get(url=url_HH, params=query_params).json()
        # проходим по страницам
        for page in range(response['pages']):
            query_params['page'] = page
            response = requests.get(url=url_HH, params=query_params).json()
            df = pd.concat([df, pd.DataFrame(response['items'])])
        start_date = start_date + dt.timedelta(hours=1)
    # в конце берем столбец id из получившегося датафрейма и возвращаем в качестве списка
    ids = set(df['id'].to_list())
    return ids



def get_vacancies(id_list):
    """
    Функция для создания массива детального описания всех вакансий по заданным критериям
    """
    vac_df = pd.DataFrame()
    for vac_id in id_list:
        print(vac_id)
        vac_response = requests.get(url=f"{url_HH}/{vac_id}").json()
        # Где функция пробразования JSON в плоскую таблицу - починил

        vac_df = pd.concat([vac_df, json_to_flatdf(vac_response)])
    
    vac_df['tech_change_date'] = dt.datetime.timestamp(dt.datetime.now())
    return vac_df


def send_data(df):
    """
    В 3 часа ночи HH начал выдавать ошибку captcha_required.
    В итоге - когда небольшое кол-во полей (меньше 70) - скрипт укладывает вакансии в базу, когда 
    Когда больше - выдает ошибку, так и не смог понять от чего это зависит. Не успел...
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
            'billing_type_id': sa.types.String, 
            'billing_type_name': sa.types.String, 
            'relations': sa.types.String,
            'name': sa.types.String, 
            'insider_interview': sa.types.Boolean, 
            'response_letter_required': sa.types.Boolean, 
            'area_id': sa.types.Integer,
            'area_name': sa.types.String, 
            'area_url': sa.types.String, 
            'salary': sa.types.Integer, 
            'type_id': sa.types.String, 
            'type_name': sa.types.String,
            'address_city': sa.types.String,
            'address_street': sa.types.String,
            'address_building': sa.types.String,
            'address_lat': sa.types.Float,
            'address_lng': sa.types.Float,
            'address_description': sa.types.String,
            'address_raw': sa.types.String,
            'address_metro_station_name': sa.types.String,
            'address_metro_line_name': sa.types.String,
            'address_metro_station_id': sa.types.String,
            'address_metro_line_id': sa.types.String,
            'address_metro_lat': sa.types.Float,
            'address_metro_lng': sa.types.Float,
            'address_metro_stations': sa.types.String,
            'allow_messages': sa.types.Boolean,
            'experience_id': sa.types.String,
            'experience_name': sa.types.String,
            'schedule_id': sa.types.String,
            'schedule_name': sa.types.String,
            'employment_id': sa.types.String, 
            'employment_name': sa.types.String, 
            'department': sa.types.String,
            'contacts': sa.types.String,
            'description': sa.types.String,
            'vacancy_constructor_template': sa.types.String,
            'key_skills': sa.types.String,
            'accept_handicapped': sa.types.Boolean,
            'accept_kids': sa.types.Boolean,
            'archived': sa.types.Boolean,
            'response_url': sa.types.String,
            'specializations': sa.types.String,
            'professional_roles': sa.types.String, 
            'code': sa.types.String,
            'hidden': sa.types.Boolean,
            'quick_responses_allowed': sa.types.Boolean,
            'driver_license_types': sa.types.String,
            'accept_incomplete_resumes': sa.types.Boolean, 
            'employer_id': sa.types.Integer, 
            'employer_name': sa.types.String,
            'employer_url': sa.types.String,
            'employer_alternate_url': sa.types.String,
            'employer_logo_urls_90': sa.types.String,
            'employer_logo_urls_240': sa.types.String,
            'employer_logo_urls_original': sa.types.String,
            'employer_vacancies_url': sa.types.String, 
            'employer_trusted': sa.types.String, 
            'published_at': sa.types.DATE,
            'created_at': sa.types.DATE, 
            'initial_created_at': sa.types.DATE, 
            'negotiations_url': sa.types.String,
            'suitable_resumes_url': sa.types.String, 
            'apply_alternate_url': sa.types.String, 
            'has_test': sa.types.Boolean, 
            'test': sa.types.Boolean,
            'alternate_url': sa.types.String, 
            'working_days': sa.types.String,
            'working_time_intervals': sa.types.String,
            'working_time_modes': sa.types.String, 
            'accept_temporary': sa.types.Boolean,
            'languages': sa.types.String, 
            'address': sa.types.String,
            'salary_from': sa.types.Integer,
            'salary_to': sa.types.Integer, 
            'salary_currency': sa.types.String, 
            'salary_gross': sa.types.Boolean,
            'address_metro': sa.types.String, 
            'department_id': sa.types.String,
            'department_name': sa.types.String,
            'insider_interview_id': sa.types.String, 
            'insider_interview_url': sa.types.String, 
            'employer_logo_urls': sa.types.String,
            'test_required': sa.types.Boolean, 
            'vacancy_constructor_template_id': sa.types.String,
            'vacancy_constructor_template_name': sa.types.String,
            'vacancy_constructor_template_top_picture_height': sa.types.String,
            'vacancy_constructor_template_top_picture_width': sa.types.String,
            'vacancy_constructor_template_top_picture_path': sa.types.String,
            'vacancy_constructor_template_top_picture_blurred_path': sa.types.String,
            'vacancy_constructor_template_bottom_picture': sa.types.String,
            'vacancy_constructor_template_bottom_picture_height': sa.types.String,
            'vacancy_constructor_template_bottom_picture_width': sa.types.String,
            'vacancy_constructor_template_bottom_picture_path': sa.types.String,
            'vacancy_constructor_template_bottom_picture_blurred_path': sa.types.String,
            'tech_change_date': sa.types.TIMESTAMP
       }
    )


#%%
def main():

    start_date = dt.datetime.today() - dt.timedelta(hours=30)
    end_date = dt.datetime.today() - dt.timedelta(days=1)

    
    # Получаем список подходящих под наши критерии вакансий
    id_list = load_list_vacancies(start_date, end_date, pкof_id, search)
    # Создаем датафрейм с детальным описанием
    df_with_vac = get_vacancies(id_list)
    # Загружаем данные в базу
    send_data(df_with_vac)


if __name__ == "__main__":
    main()