#%%
import datetime as dt
import pandas as pd
import requests

### --- Конфигруация логгирования / --- ###
import logging, sys
logger = logging.getLogger('debug')
handler_console = logging.StreamHandler(sys.stdout)
handler_console.setFormatter(logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s', datefmt='%Y-%m-%d %T'))
logger.handlers = []
logger.addHandler(handler_console)
logger.setLevel(logging.DEBUG)
### --- / Конфигруация логгирования --- ###


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
# 1. Загружаем 


