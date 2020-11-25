import pymysql
from datetime import date
from configparser import ConfigParser
import os


class ConnectMySQL:
    """ConnectMySQL"""

    def __init__(self):
        config = ConfigParser()
        config.read('config.ini')

        if os.path.isfile('horecoding/config.ini'):
            print('OK')
        else:
            print('No')

        try:
            self.conn = pymysql.connect(host=config.get('DB', 'db_host'),
                                        user=config.get('DB', 'db_user'),
                                        password=config.get('DB', 'db_password'),
                                        db=config.get('DB', 'db_db'),
                                        charset='utf8')
        except Exception as error:
            print("*" * 100)
            print('Connect error, error: {}'.format(error))
            print("*" * 100)
        except AttributeError as attribute_error:
            print("*" * 100)
            print('AttributeError, error: {}'.format(attribute_error))
            print("*" * 100)


class Etc:
    def __init__(self):
        return

    def week_of_day(yymmdd):

        year = int(yymmdd[0:4])
        month = int(yymmdd[4:6])
        day = int(yymmdd[6:8])
        week = date(year, month, day).weekday()
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        return week, days[week]

    def time_to_second_simsa(time):
        time.replace(' ', '')
        if time.find(':') > 0:
            time_splited = time.split(':')
            if len(time_splited) == 2:
                minute = int(time_splited[0])
                second_splited = time_splited[1].split('.')
                second = int(second_splited[0])
            elif len(time_splited) == 3:
                print('?????????????????????????????????????????????????? 시간이상')
                print(time)
                print('?????????????????????????????????????????????????? 시간이상')
            else:
                return None
            time = str(minute * 60 + second) + '.' + second_splited[1]
        else:
            if time.find('.') > 0:
                time_splited = time.split('.')
                time = time_splited[0] + '.' + time_splited[1]
            else:
                if time != '' and int(time) > 0:
                    time = time + '.0'
                else:
                    time = '0.0'
        # print(time)
        return time


def time_to_second(time):
    if time != '':
        if time.find(':') > 0:
            time_splited = time.split(':')

            minute = int(time_splited[0])
            second_splited = time_splited[1].split('.')
            second = int(second_splited[0])
            strtime = str((minute * 60) + second) + '.' + second_splited[1]
            rsttime = float(strtime)
        else:
            rsttime = float(time)
    else:
        rsttime = 0.0

    # print(rsttime)
    return rsttime


def get_yymmdd_l_no(link, meet):
    exist_link = ''
    # print(link)
    get_error = False
    if meet == '1':
        if len(link) > 1:
            exist_link = str(link[0]) + str(int(link[1]) + 100) + 'S'
        else:
            get_error = True
    elif meet == '3':
        if len(link) > 1:
            exist_link = str(link[0]) + str(int(link[1]) + 100) + 'B'
        else:
            get_error = True
    elif meet == '2':
        if len(link[0]) < 8:
            get_error = True
        else:
            exist_link = str(link[0]) + str(int(link[1]) + 100) + 'J'

    return get_error, exist_link


def get_drt(txt_drt):
    rst_drt = []
    bb = txt_drt.split(': ')
    if len(bb) < 2:
        rst_drt = ['0', '0', '0']
        return rst_drt
    else:
        aa = bb[1].split(' ')
        for i, v in enumerate(aa):
            if v.isnumeric() == False:
                rst_drt.append(v)
            else:
                pass
        return rst_drt


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def get_city_name(meet):
    city_name = ''

    if meet == 1:
        city_name = '서울'
    elif meet == 2:
        city_name = '제주'
    elif meet == 3:
        city_name = '부산'

    return city_name


def get_city_initial(meet):
    city_initial = ''

    if int(meet) == 1:
        city_initial = 'S'
    elif int(meet) == 2:
        city_initial = 'J'
    elif int(meet) == 3:
        city_initial = 'B'

    return city_initial


def get_date(YYMMDD_L_NO):
    return '20' + YYMMDD_L_NO[0:2] + '-' + YYMMDD_L_NO[2:4] + '-' + YYMMDD_L_NO[4:6]


def parse_YYMMDD_L_NO(YYMMDD_L_NO):
    rcDate = '20' + YYMMDD_L_NO.split('_')[0]
    L = YYMMDD_L_NO.split('_')[1]
    if L == 'S':
        meet = 1
    elif L == 'J':
        meet = 2
    elif L == 'B':
        meet = 3
    else:
        meet = 0
    rcNo = YYMMDD_L_NO.split('_')[2]

    return (rcDate, meet, rcNo)
