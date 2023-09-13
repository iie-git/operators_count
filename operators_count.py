import os

from datetime import datetime
import re
import math
import time
import statistics
from functools import reduce


import argparse

def read_in_chunks(file_object, chunk_size=1024*1024):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data



def parse(path:str):
    assert os.path.exists(path),f'Файл {path} не существует!'

    pattern=re.compile('FROM:(\d{4}-\d{2}-\d{2} \d{2}:\d{2}) TO:(\d{4}-\d{2}-\d{2} \d{2}:\d{2}) \n')
    calls = {}
    skipped_count=0
    finded_lines=0
    match_end = 0
    not_full_data=''
    with open(path,'r') as file:
        for piece in read_in_chunks(file):
            data=f'{not_full_data}{piece}'
            for match in re.finditer(pattern,data):
                finded_lines+=1
                start_dt= datetime.strptime(match.group(1),"%Y-%m-%d %H:%M")
                end_dt=datetime.strptime(match.group(2),"%Y-%m-%d %H:%M")
                if end_dt<start_dt:
                    skipped_count+=1
                    continue
                current_date=start_dt.date()
                current_hour=start_dt.hour
                duration=(end_dt - start_dt).total_seconds()
                if current_date in calls:
                    if current_hour in calls[current_date]:
                        calls[current_date][current_hour]['duration']+=duration
                        calls[current_date][current_hour]['count']+=1
                    else:
                        calls[current_date][current_hour]={'duration':duration,'count':1}
                else:
                    calls[current_date]={current_hour:{'duration':duration,'count':1}}
                match_end=match.end()
            not_full_data=data[match_end:]
    if len(calls)==0:
        print('Cant find regexp pattern!')
        return None
    print(f'{"-" * 8}\nКол-во обработанных строк: {finded_lines - skipped_count} , пропущено строк : {skipped_count}\n{"-" * 8}')
    return calls


def statistic_by_calls_dict(calls_dict:dict)->list[list[str,int]]:
    operators_by_day=[]
    for key,hour_dict in calls_dict.items():
        str_date=key.strftime("%Y-%m-%d")
        list_of_calls_count_in_hour=[]
        calls_duration_in_hour=[]
        calls_in_day=0
        for hour_key,stat in hour_dict.items():
            list_of_calls_count_in_hour.append(stat['count'])
            calls_duration_in_hour.append(stat['duration']/stat['count'])
            calls_in_day+=stat['count']
        mean_calls_in_hour=reduce(lambda x,y:x+y,list_of_calls_count_in_hour,0)/len(list_of_calls_count_in_hour)
        mean_duration_in_day=reduce(lambda x,y:x+y,calls_duration_in_hour,0)/len(calls_duration_in_hour)
        operators_needed= math.ceil(mean_calls_in_hour * mean_duration_in_day / 3600)
        # print(f'{str_date}')
        # print(f'Кол-во звонков в день: {calls_in_day}')
        # print(f'Кол-во активных часов в день: {len(list_of_calls_count_in_hour)}')
        # print(f'Среднее кол-во звонков в час: {mean_calls_in_hour}')
        # print(f'Среднее время  звонков в день: {mean_duration_in_day}с')
        # print(f'Кол-во операторов: {operators_needed}\n')
        operators_by_day.append([str_date,operators_needed])
    return operators_by_day

def get_operators_count(calls_dct:dict)->int:
    statistic=statistic_by_calls_dict(calls_dct)
    assert isinstance(statistic,list),f'function statistic_by_calls_dict return wrong value type = {type(statistic)}'
    assert len(statistic)>0,f'function statistic_by_calls_dict return empty value'
    statistic=sorted(statistic,key = lambda x: x[1])
    mean_value=math.ceil(statistics.median(list(map(lambda x:x[1],statistic))))
    print(f'Минимальное кол-во операторов {statistic[0][0]} : {statistic[0][1]}')
    print(f'Максимальное кол-во операторов {statistic[-1][0]} : {statistic[-1][1]}')
    print(f'Среднее кол-во операторов: {math.ceil(reduce(lambda x,y:x+y[1],statistic,0)/len(statistic))}')
    print(f'Медианное кол-во операторов: {mean_value}')
    print(f'Кол-во учтенных дней {len(statistic)}')

    return mean_value


parser = argparse.ArgumentParser(description='Get operators count by call logs')
parser.add_argument('-f','--file', help='Call log file',type=str, required=True)
args = vars(parser.parse_args())

start=time.time()
calls_dict=parse(args['file'])
assert not(calls_dict is None),f'Cant parse file {args["file"]}'
result=get_operators_count(calls_dict)


print(f'{"*" * 8}\nВремя выполнения {time.time() - start}\n{"*" * 8}')
