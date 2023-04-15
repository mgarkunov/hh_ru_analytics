# load all the using libraries
import pandas as pd
import requests
import datetime as dt
import json
import asyncio
import aiohttp
import nest_asyncio
nest_asyncio.apply()

# set the parameters
url_HH = 'https://api.hh.ru/vacancies'
search = "NAME:((Data Engineer) OR (Data Scientist) OR (Data Analyst) OR (data science engineer) \
    OR (Аналитик данных) OR (Бизнес аналитик) OR (финансовый аналитик) OR (системный аналитик) \
        OR (системная аналитика) OR (продуктовый аналитик) OR (дата инженер) OR (инженер данных) \
            OR (devops инженер) OR (датасайнтист) OR (Аналитик AND (систем* OR продукт* OR бизнес* OR данн* OR финанс*)))"
start_date = dt.datetime(2023, 4, 3, 0, 0)
end_date = dt.datetime(2023, 4, 9, 23, 59)

query_params = {
    'text': search, 
    'area': 113, 
    'per_page': 100,
    'date_from': start_date.strftime("%Y-%m-%dT%H:%M:%S"),
    'date_to': end_date.strftime("%Y-%m-%dT%H:%M:%S")
}

# create the dataframe for the first searching request based on the text searching request
df = pd.DataFrame()
# first discovering page
page = 0
replies = []
# cycle with saving requests according to the specified parameters to bypass the limit of 100 vacancies
while True:
    query_params['page'] = page
    response = requests.get(url=url_HH, params=query_params).json()
    for item in response['items']:
        replies.append(item)
    df = pd.concat([df, pd.DataFrame(response['items'])])
    if page < (response['pages'] - 1):
        page += 1
    else:
        break
with open(f"data/{start_date}_{end_date}.json", "w") as f:
    json.dump(replies, f, indent=4, ensure_ascii=False)

# function that is flatten the     
def json_to_flatdf(response):
    """
    Функция преобразования в плоскую таблицу
    Отдельные поля-исключения:
     - key_skills: все ключевые навыки сохраняются в список
     - station_name: сохраняется только одна станция метро - самая ближайшая
     - languages: сохраняется только список необходимых языков без уровня
    """
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                if a == 'key_skills':
                    help_list = []
                    for item in x[a]:
                        help_list.append(item['name'])
                    out[a] = help_list
                elif a == 'metro':
                    if x[a] != None:
                        out[a] = x[a]["station_name"]
                elif a == "metro_stations":
                    pass
                elif a == 'languages':
                    help_list = []
                    for item in x[a]:
                        help_list.append(item['name'])
                    out[a] = help_list
                else:
                    flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(response)
    return out

# list for further saving of json-vacancies
parsed_json = []

# async function to get data about exact vacancie based on id
async def vacancion_data(session, vac_id):
    async with session.get(url=f"{url_HH}/{vac_id}") as response:
        response_json = await response.json()
        
        parsed_json.append(response_json)

# async function to assemble a json file
async def vacancy_function(replies):
    
    list_of_vacs = []
    for i in replies:
        list_of_vacs.append(i["id"])
    
    chunk = 10
    tasks = []
    pended = 0
    
    async with aiohttp.ClientSession() as session:
        
        for vac_id in list_of_vacs:
            task = asyncio.create_task(vacancion_data(session, vac_id))
            tasks.append(task)
            pended += 1
            if len(tasks) == chunk or pended == len(list_of_vacs):
                print(pended)
                await asyncio.gather(*tasks)
                tasks = []

# dataset collection
loop = asyncio.get_event_loop()
loop.run_until_complete(vacancy_function(replies))


# saving the json
with open(f"data/full_{start_date}_{end_date}.json", "w") as f:
    json.dump(parsed_json, f, indent=4, ensure_ascii=False)


# description of the received dataset 
description_dict = {}
for item in parsed_json:
    flatten_json = json_to_flatdf(item)
    for key in flatten_json.keys():
        if key not in description_dict.keys():
            description_dict[key] = {'Types': [type(flatten_json[key])],
                                      'Qty': [1],
                                      'Samples': [flatten_json[key]]}
        else:
            if type(flatten_json[key]) not in description_dict[key]['Types']:
                description_dict[key]['Types'].append(type(flatten_json[key]))
                description_dict[key]['Qty'].append(1)
                description_dict[key]['Samples'].append(flatten_json[key])
            else:
                description_dict[key]['Qty'][description_dict[key]['Types'].index(type(flatten_json[key]))] += 1

# tranformation to dataframe
df = pd.DataFrame(description_dict).transpose()
# saving to csv
df.to_csv("data/var_for_april-15.csv")