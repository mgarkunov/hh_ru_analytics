#%%
import datetime as dt
import pandas as pd
import requests
import yaml
import sqlalchemy as sa

#%%

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
# psql = sa.create_engine(f"postgresql://{cfg['psql']['user']}:{cfg['psql']['pwd']}@{cfg['psql']['host']}:{cfg['psql']['port']}/{cfg['psql']['dbname']}")

# # --- Инициализация базы / -- #
# if psql.url.database == 'hh_analytics':
#   psql_con = psql.connect()
#   psql_con.execute(sa.text("""
#   create schema if not exists dwh_stage;
#   commit; -- не забываем комитить работу, так как PSQL транзакционная база
#   """))
#   psql_con.execute(sa.text("""
#   create schema if not exists dwh_mart;
#   commit; -- не забываем комитить работу, так как PSQL транзакционная база
#   """))
#   if psql_con.closed == False:
#     psql_con.close()
# else:
#   raise ValueError('Не правильное название базы')
# # --- / Инициализация базы -- #


def get_professional_roles() -> pd.DataFrame:
  """ 
  Функция получает список профессиональных ролей и формирует датафрейм с правильными типами данных
  """
  response = requests.request('GET', 'https://api.hh.ru/professional_roles')
  dict_data = response.json()['categories']
  del(response)
  dict_lst = []
  for d in dict_data:
    for s in d['roles']:
      dict_lst.append({
        'cat_id': d['id'],
        'cat_name': d['name'],
        'role_id': s['id'],
        'role_name': s['name'],
        'accept_incomplete_resumes': s['accept_incomplete_resumes'],
        'is_default': s['is_default'],
      })
  return pd.DataFrame(dict_lst)

get_professional_roles()

#%%

response = requests.request('GET', 'https://api.hh.ru/professional_roles')
response.json()

#%%

def json_to_flatdf(response):
  """
  Функция преобразования json в плоскую таблицу
  """
  pass

def load_list_vacancies(start_date:dt.date, end_date:dt.date, pкof_id:list = [], search:str = ""):
  """
  Функция для загрузки списка вакансий
  НАдо будет сделать цикл, который будет обходит по дням и часам.
  pкof_id - список для id профессий
  search - поиковая фраза
  """
  pass

def load_detail_vacancy(vac_list:list):
  """
  vac_list - список id с вакансиями из списка и далее нужно будет получить детально, 
  т.е. нужно сделать алгоритм обхода списка
  """

# Алогоритм работы скрипта
# 1. Загружаем список всех вакансий через функцию load_list_vacancies
# 2. Далее формируем из полученных вакансий массив для загрузки данных в базу.
# 3. Получаем список id вакансий для детальной загрузки
# 4. Загружаем детальное описание вакансий и кахдые 500+ загрузок складываем в базу.

#%%

# --- пример загрузки в базу / --- #
get_professional_roles().to_sql(
  name='dict_professional_roles',
  con=psql,
  schema='dwh_stage',
  if_exists='replace',
  index=False,
  dtype={
    'cat_id': sa.types.Integer,
    'cat_name': sa.types.String,
    'role_id': sa.types.Integer,
    'role_name': sa.types.String,
    'accept_incomplete_resumes': sa.types.Boolean,
    'is_default': sa.types.Boolean
  }
)
# --- / пример загрузки в базу --- #

#%%


