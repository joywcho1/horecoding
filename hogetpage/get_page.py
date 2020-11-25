# from urllib.error import URLError, HTTPError
# import urllib
from bs4 import BeautifulSoup
import requests
from requests.exceptions import HTTPError
from collections import OrderedDict
import time
import re
import hogetdb.execute_query as hoeq
import hodbsql.sel_sql as hos


class HttpMethods:

    def __init__(self):
        return

    def get_url(self, url):

        tries = 5
        for i in range(tries):
            try:
                f = requests.get(url)
                f.raise_for_status()
            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Python 3.6
                if i < tries - 1:  # i is zero indexed
                    time.sleep(2)
                    continue

            except Exception as err:
                print(f'Other error occurred: {err}')  # Python 3.6
                if i < tries - 1:  # i is zero indexed
                    time.sleep(2)
                    continue
            else:
                return BeautifulSoup(f.content, 'html.parser', from_encoding='utf-8')

    def post_url(self, url, params):
        tries = 5
        for i in range(tries):
            try:
                r = requests.post(url, data=params)
                r.raise_for_status()
            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Python 3.6
                if i < tries - 1:  # i is zero indexed
                    time.sleep(2)
                    continue
            except Exception as err:
                print(f'Other error occurred: {err}')  # Python 3.6
                if i < tries - 1:  # i is zero indexed
                    time.sleep(2)
                    continue
            else:
                return BeautifulSoup(r.content, 'html.parser', from_encoding='utf-8')


class ClsHoinfoOrder:
    def __init__(self):
        self.col = OrderedDict()
        self.col['RH_NM'] = ''
        self.col['GE_MEET'] = ''
        self.col['RH_ID_NO'] = ''
        self.col['RATING1'] = 0
        self.col['RATING2'] = 0
        self.col['RATING3'] = 0
        self.col['RATING4'] = 0
        self.col['IN_GROUP'] = ''
        self.col['IN_GRADE'] = ''
        self.col['RH_PROD_AREA'] = ''
        self.col['RH_GEN'] = ''
        self.col['RH_AGE'] = 0
        self.col['RH_ENNM'] = ''  # 추가
        self.col['GRADE_FIRST_DAY'] = ''  # 추가
        self.col['TRAINER_NM'] = ''  # 추가
        self.col['TRAINER_NM_GRP'] = ''  # 추가
        self.col['RH_BIRTH'] = ''
        self.col['RH_COLOR'] = ''  # 추가
        self.col['RH_OWNER'] = ''  # 추가
        self.col['RH_PERIOD'] = ''  # 추가
        self.col['RH_PRODUCER'] = ''  # 추가
        self.col['RH_SCORE'] = ''
        self.col['RH_WIN_DRT_T'] = 0.0  # 승률
        self.col['RH_SHOW_DRT_T'] = 0.0  # 복승률
        self.col['RH_QQQQ_DRT_T'] = 0.0  # 연승률
        self.col['HEAD_FEAT'] = ''  # 추가
        self.col['NECK_FEAT'] = ''  # 추가
        self.col['LEG_FEAT'] = ''  # 추가
        self.col['TRUNK_FEAT'] = ''  # 추가
        self.col['BRAND'] = ''  # 업테이트 가능 # 추가
        self.col['RH_RESIT_YYMMDD'] = ''  # 추가
        self.col['FST_BUY_AMT'] = 0  # 추가
        self.col['LST_BUY_AMT'] = 0  # 업테이트 가능 # 추가
        self.col['RH_TOT_AMT'] = 0  # 수득상금
        self.col['RH_SIX_AMT'] = 0  # 6개월상금
        self.col['RH_THREE_AMT'] = 0  # 3개월상금


class HorseInfor(ClsHoinfoOrder):
    def __init__(self):
        return

    def horse_info(self, meet, ho_info_v, tbl_type, s_mode):

        self.info_msg = '페이지 읽기'
        self.read_error = False
        self.url = 'http://race.kra.co.kr/racehorse/profileList.do'
        self.check_yn = False

        if s_mode == "rank":
            self.hr_grade = ho_info_v.encode('euc-kr')
            self.params = {'rank': self.hr_grade, 'Sub': '1', 'Act': '07', 'meet': meet}
        elif s_mode == "name":
            self.n_horse_name = ho_info_v[0]
            self.n_horse_num = ho_info_v[1]
            self.params = {'HorseName': self.n_horse_name.encode('euc-kr'), 'Sub': '1', 'Act': '07', 'meet': meet}

        self.ho_infohorse = OrderedDict()

        # try:
        self.horse_page = HttpMethods()
        self.soup_ma = self.horse_page.post_url(self.url, self.params)
        self.rsttbody_ma = self.soup_ma.find_all('tbody')
        self.rsttr_ma = self.rsttbody_ma[0].find_all('tr')
        self.read_error = False
        self.is_existed = 0
        self.to_insert = 0
        for rsttr_ma_i, rsttr_ma_v in enumerate(self.rsttr_ma):
            self.tds_ma_info = rsttr_ma_v.find_all('td')
            for td_i, td in enumerate(self.tds_ma_info):
                if td_i == 0:  # 번호
                    pass
                elif td_i == 1:  # 마명
                    self.p = re.compile("""(\")+(\d)+(\"+\,+\")+(\d+)\"""")
                    self.cc = str(td.find('a'))
                    self.m = self.p.search(self.cc)
                    self.horse_loc = self.m.group(2)
                    self.horse_num = self.m.group(4)
                    self.horse_name_parse = td.get_text().strip()
                    self.horse_name = self.horse_name_parse.replace('[서]', '')
                    self.horse_name = self.horse_name.replace('[부]', '')
                    self.horse_name = self.horse_name.replace('[', '')
                    self.horse_name = self.horse_name.replace(']', '')

                    if s_mode == "name":
                        # print(self.horse_name, self.n_horse_name)
                        if not (self.horse_name == self.n_horse_name and self.horse_num == self.n_horse_num):
                            self.read_error = True
                            break
                        else:
                            # print(self.horse_name, self.n_horse_num)
                            self.to_insert += 1
                            pass
                    else:
                        self.check_horse = hoeq.SelectResult()
                        self.check_sql = hos.check_horse(self.horse_name, self.horse_num, tbl_type)
                        self.check_yn, self.check_db = self.check_horse.select_sql(self.check_sql, 'tuple')
                        self.info_msg = 'check error'

                        if self.check_yn:
                            self.read_error = True
                            self.info_msg = 'DB에 존재'
                            self.is_existed += 1
                            break
                        else:
                            self.to_insert += 1
                            pass

                    self.ho_infohorse[(self.horse_name, self.horse_num)] = ClsHoinfoOrder()
                    self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_NM'] = self.horse_name
                    self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_ID_NO'] = str(self.horse_num)
                    self.ho_infohorse[(self.horse_name, self.horse_num)].col['GE_MEET'] = self.horse_loc

                    profile = HorseProfile(self.ho_infohorse, self.horse_num, self.horse_name)
                    self.ho_infohorse = profile.profile_horse(meet, self.horse_loc, self.horse_num)

                elif td_i == 2:
                    if td.get_text().strip().replace(' ', '') != '':
                        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RATING1'] = int(
                            td.get_text().strip().replace(' ', ''))
                    else:
                        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RATING1'] = 0
                elif td_i == 3:
                    if td.get_text().strip().replace(' ', '') != '':
                        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RATING2'] = int(
                            td.get_text().strip().replace(' ', ''))
                    else:
                        self.ho_infohorse[(self.horse_name, self.horse_num)].col[
                            'RATING2'] = 0
                elif td_i == 4:
                    if td.get_text().strip().replace(' ', '') != '':
                        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RATING3'] = int(
                            td.get_text().strip().replace(' ', ''))
                    else:
                        self.ho_infohorse[(self.horse_name, self.horse_num)].col[
                            'RATING3'] = 0
                elif td_i == 5:
                    if td.get_text().strip().replace(' ', '') != '':
                        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RATING4'] = int(
                            td.get_text().strip().replace(' ', ''))
                    else:
                        self.ho_infohorse[(self.horse_name, self.horse_num)].col[
                            'RATING4'] = 0
                elif td_i == 6:
                    pass

                elif td_i == 7:
                    self.ho_infohorse[(self.horse_name, self.horse_num)].col['IN_GRADE'] = td.get_text().strip()
                elif td_i == 8:
                    self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_PROD_AREA'] = td.get_text().strip()
                elif td_i == 9:
                    self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_GEN'] = td.get_text().strip()
                elif td_i == 10:
                    if td.get_text().strip().replace(' ', '') != '':
                        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_AGE'] = int(
                            td.get_text().strip().replace(' ', ''))
                    else:
                        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_AGE'] = 0
                else:
                    continue
            if self.read_error:
                continue

        if self.to_insert > 0:
            self.insert_error = False
        else:
            self.insert_error = True

        return self.insert_error, self.info_msg, self.ho_infohorse

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


class HorseProfile(HorseInfor):
    def __init__(self, ho_infohorse, horse_num, horse_name):
        self.ho_infohorse = ho_infohorse
        self.horse_num = horse_num
        self.horse_name = horse_name

    def profile_horse(self, meet, horse_loc, horse_num):

        self.url_profile = 'http://race.kra.co.kr/racehorse/profileHorseItem.do?Sub=1&Act=02&jkNo=&trNo=&owNo=&rcDate=&rcNo='
        self.params_profile = {'meet': horse_loc, 'hrNo': horse_num}

        self.prifile_page = HttpMethods()
        self.soup_ma = self.prifile_page.post_url(self.url_profile, self.params_profile)
        self.rsttable_ma = self.soup_ma.find_all('table')
        self.tittrs_ma = self.rsttable_ma[0].find_all('tr')
        self.test = self.tittrs_ma[0].find_all('td')
        self.eng_nm = self.test[0].get_text().strip().split('(')[1][:-1]
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_ENNM'] = self.eng_nm
        self.test1 = self.tittrs_ma[1].find_all('td')
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['IN_GROUP'] = \
            self.test1[0].get_text().strip().split('(')[0]
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['GRADE_FIRST_DAY'] = \
            self.test1[0].get_text().strip().split('(')[1][:-1]
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['TRAINER_NM'] = \
            self.test1[3].get_text().strip().split('(')[0].replace(' ', '')
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['TRAINER_NM_GRP'] = \
            self.test1[3].get_text().strip().split('(')[1][:-1]
        self.tittds_maaa1 = self.tittrs_ma[2].find_all('td')
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_BIRTH'] = self.tittds_maaa1[0].get_text().strip()
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_COLOR'] = self.tittds_maaa1[2].get_text().strip()
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_OWNER'] = self.tittds_maaa1[3].get_text().strip()
        self.test3 = self.tittrs_ma[3].find_all('td')
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_PERIOD'] = self.test3[0].get_text().strip()

        if meet != '2':
            self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_PRODUCER'] = self.test3[3].get_text().strip()
        else:
            self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_PRODUCER'] = self.test3[2].get_text().strip()

        self.tittds_ma = self.tittrs_ma[4].find_all('td')
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_SCORE'] = self.tittds_ma[0].get_text().strip()
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_WIN_DRT_T'] = float(
            self.tittds_ma[1].get_text().strip().split(':')[1].replace('%', '0').replace(' ', ''))
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_SHOW_DRT_T'] = float(
            self.tittds_ma[2].get_text().strip().split(':')[1].replace('%', '0').replace(' ', ''))
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_QQQQ_DRT_T'] = float(
            self.tittds_ma[3].get_text().strip().split(':')[1].replace('%', '0').replace(' ', ''))
        self.test5 = self.tittrs_ma[5].find_all('td')
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['HEAD_FEAT'] = self.test5[0].get_text().strip()
        self.test6 = self.tittrs_ma[6].find_all('td')
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['NECK_FEAT'] = self.test6[0].get_text().strip()
        self.test7 = self.tittrs_ma[7].find_all('td')
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['LEG_FEAT'] = self.test7[0].get_text().strip()
        self.test8 = self.tittrs_ma[8].find_all('td')
        self.ho_infohorse[(self.horse_name, self.horse_num)].col['TRUNK_FEAT'] = self.test8[0].get_text().strip()
        self.test9 = self.tittrs_ma[9].find_all('td')

        if self.test9[0].get_text().strip() != "( 좌 ) 　　　　　( 우 ) 　　　　　(기타)":
            self.ho_infohorse[(self.horse_name, self.horse_num)].col['BRAND'] = self.test9[0].get_text().strip()
        else:
            self.ho_infohorse[(self.horse_name, self.horse_num)].col['BRAND'] = ""
        if meet != '2':
            self.tittds_maa = self.tittrs_ma[10].find_all('td')
            self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_RESIT_YYMMDD'] = self.tittds_maa[
                0].get_text().strip()
            self.ho_infohorse[(self.horse_name, self.horse_num)].col['FST_BUY_AMT'] = self.tittds_maa[
                1].get_text().strip()
            self.ho_infohorse[(self.horse_name, self.horse_num)].col['LST_BUY_AMT'] = self.tittds_maa[
                2].get_text().strip()
            self.tittds_maa1 = self.tittrs_ma[11].find_all('td')
            if self.tittds_maa1[0].get_text().strip().replace(',', '').replace('원', '') != '':
                self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_TOT_AMT'] = int(
                    self.tittds_maa1[0].get_text().strip().replace(',', '').replace('원', ''))
            else:
                self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_TOT_AMT'] = 0
            if self.tittds_maa1[1].get_text().strip().replace(',', '').replace('원', '') != '':
                self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_SIX_AMT'] = int(
                    self.tittds_maa1[1].get_text().strip().replace(',', '').replace('원', ''))
            else:
                self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_SIX_AMT'] == 0
            if self.tittds_maa1[2].get_text().strip().replace(',', '').replace('원', '') != '':
                self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_THREE_AMT'] = int(
                    self.tittds_maa1[2].get_text().strip().replace(',', '').replace('원', ''))
            else:
                self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_THREE_AMT'] = 0
        else:
            self.tittds_maa = self.tittrs_ma[10].find_all('td')
            self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_RESIT_YYMMDD'] = self.tittds_maa[
                0].get_text().strip()
            if self.tittds_maa[1].get_text().strip().replace(',', '').replace('원', '') != '':
                self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_TOT_AMT'] = int(
                    self.tittds_maa[1].get_text().strip().replace(',', '').replace('원', ''))
            else:
                self.ho_infohorse[(self.horse_name, self.horse_num)].col['RH_TOT_AMT'] = 0

        return self.ho_infohorse


class SimsaHorang:  # 경마일자별경주정보
    def __init__(self):
        self.col = OrderedDict()

        self.col['YYMMDD_L_NO'] = ' '  # VARCHAR(13)  NOT NULL COMMENT '20170930_S_01', -- 경마일자_장소_경주라운드
        self.col['RACE_LEVEL_DIV'] = ' '  # VARCHAR(50)  NULL     COMMENT '등급구분', -- 등급구분
        self.col['RACE_SCALEWEIGHT_TYPE'] = ''
        self.col['RACE_DISTANCE'] = 0  # INT          NULL     COMMENT '경주거리', -- 경주거리
        self.col['RACE_NM'] = ' '  # VARCHAR(100) NULL     COMMENT '경주명', -- 경주명
        self.col['TODAY_RACE_WEATHER'] = ''  # VARCHAR(10)  NULL     COMMENT '당일경주날씨', -- 당일경주날씨
        self.col['TODAY_TRACK_STATE'] = ''  # VARCHAR(10)  NULL     COMMENT '당일경주주로상태', -- 당일경주주로상태
        self.col['TODAY_TRACK_MOISTURE'] = 0  # FLOAT(10,1)  NULL     COMMENT '당일경주함수율', -- 당일경주함수율
        self.col['STAGE'] = ''


class SimsaHorangSub:  #
    def __init__(self):
        self.col = OrderedDict()

        self.col['YYMMDD_L_NO'] = ''  # VARCHAR(13) NOT NULL COMMENT 'yyyymmdd_s_no',-- 경마일자_장소_경주라운드
        self.col['RH_NM'] = ''  # VARCHAR(50) NOT NULL COMMENT '경주마이름', -- 경주마이름
        self.col['RH_ID_NO'] = ''
        self.col['RHT_START_LANE_NO'] = 0  # INT   NULL     COMMENT 'RACE_HORSE_TODAY_START_LANE_NO', -- 경주출발레인번호
        self.col['RH_PROD_AREA'] = ''  # VARCHAR(50) NULL     COMMENT '산지', -- 산지
        self.col['RH_GEN'] = ''  # CHAR(2)     NULL     COMMENT '성별', -- 성별
        self.col['RH_AGE'] = 0  # INT         NULL     COMMENT '연령', -- 연령
        self.col['RH_RATING'] = 0  # INT         NULL     COMMENT 'RACE_HORSE_TODAY_START_LANE_NO', -- 경주출발레인번호
        self.col['RHT_SCALEWEIGHT'] = 0.0  # INT         NULL     COMMENT '부담중량', -- 부담중량
        self.col['RH_SCALEWEIGHT_DIFF'] = 0.0  # INT         NULL     COMMENT '부담중량증감', -- 부담중량증감
        self.col['RHT_JOCKEY_NM'] = ''  # VARCHAR(10) NULL     COMMENT '기수명', -- 기수명
        self.col['TRAINER_NM'] = ''  # VARCHAR(10) NULL     COMMENT '조교사명', -- 조교사명
        self.col['RH_OWNER'] = ''  # VARCHAR(50) NULL     COMMENT '마주명', -- 마주명
        self.col['TRAIN_NUM'] = 0  # VARCHAR(50) NULL     COMMENT '마주명', -- 마주명
        self.col['ENTER_PERIOD'] = 0  # INT         NULL     COMMENT '판정결과', -- 판정결과
        self.col['RHT_ACCESSORY'] = ''  # INT         NULL     COMMENT '판정결과', -- 판정결과
        self.col['RH_ENTER_YN_REASON'] = ''  # VARCHAR(50) NULL     COMMENT '출주여부사유', -- 출주여부사유
        self.col['RHT_WEIGHT'] = 0  # INT         NULL     COMMENT '마체중', -- 마체중
        self.col['RH_WEIGHT_DIFF'] = 0  # INT         NULL     COMMENT '마체중증감', -- 마체중증감
        self.col['RACE_TERM'] = 0  # INT         NULL     COMMENT '마체중증감', -- 마체중증감
        self.col['RHT_JOCKEY_WEIGTH'] = 0.0  # INT         NULL     COMMENT '기수체중', -- 기수체중
        self.col['RST_RANK'] = 0  # INT         NULL     COMMENT '순위', -- 순위
        self.col['RST_TIME'] = 0.0  # DATETIME    NULL     COMMENT '경주기록시간', -- 경주기록시간
        self.col['S1FRANK'] = 0  # INT         NULL     COMMENT 'S-1F순위', -- S-1F순위
        self.col['S1FTIME'] = 0.0  # DATETIME    NULL     COMMENT 'S-1F시간', -- S-1F시간
        self.col['1CRANK'] = 0  # INT         NULL     COMMENT '1코너순위', -- 1코너순위
        self.col['1CTIME'] = 0.0  # DATETIME    NULL     COMMENT '1코너시간', -- 1코너시간
        self.col['2CRANK'] = 0  # INT         NULL     COMMENT '2코너순위', -- 2코너순위
        self.col['2CTIME'] = 0.0  # DATETIME    NULL     COMMENT '2코너시간', -- 2코너시간
        self.col['3CRANK'] = 0  # INT         NULL     COMMENT '3코너순위', -- 3코너순위
        self.col['3CTIME'] = 0.0  # DATETIME    NULL     COMMENT '3코너시간', -- 3코너시간
        self.col['4CRANK'] = 0  # INT         NULL     COMMENT '4코너순위', -- 4코너순위
        self.col['4CTIME'] = 0.0  # DATETIME    NULL     COMMENT '4코너시간', -- 4코너시간
        self.col['G3FTIME'] = 0  # DATETIME    NULL     COMMENT 'G-3F시간', -- G-3F시간
        self.col['G1FRANK'] = 0  # INT         NULL     COMMENT 'G-1F순위', -- G-1F순위
        self.col['G1FTIME'] = 0.0  # DATETIME    NULL     COMMENT 'G-1F시간', -- G-1F시간
        self.col['RH_WIN_DRT'] = 0.0  # DATETIME    NULL     COMMENT 'G-1F시간', -- G-1F시간
        self.col['RH_SHOW_DRT'] = 0.0  # DATETIME    NULL     COMMENT 'G-1F시간', -- G-1F시간
        self.col['RACE_DISTANCE'] = 0  # INT          NULL     COMMENT '경주거리', -- 경주거리
        self.col['RACE_LEVEL_DIV'] = ''
        self.col['RACE_SCALEWEIGHT_TYPE'] = ''
        self.col['STAGE'] = ''


class SimaLink:
    def __init__(self):
        return

    def simsa_get_link(self, **kwargs):

        self.url = 'http://race.kra.co.kr/referee/RacingTrainCheckList.do?Sub=8&meet=' + kwargs['meet'] + '&Act=06'
        self.param = {'pageIndex': kwargs['page_num']}

        self.simsa_link = HttpMethods()
        self.soup = self.simsa_link.post_url(self.url, self.param)
        self.divs = self.soup.find_all('div', attrs={'class': 'tableType2'})
        self.trs = self.divs[0].find_all('tr')
        self.params = []
        check_recode = hoeq.SelectResult()
        for tr_i, tr in enumerate(self.trs):
            if tr_i == 0:
                continue
            else:
                self.tds = tr.find_all('td')
                self.text_is = self.tds[1].get_text().strip().replace(' ', '')
                if self.text_is == '결과표':
                    for link in tr.find_all('a'):
                        self.pa = re.compile("""(\")+([0-9])+(\"+,+\")+([0-9]+)\"""")
                        self.cc = str(link.get('onclick'))
                        self.m = self.pa.search(self.cc)

                        self.trno = self.m.group(2)
                        self.rcdate = self.m.group(4)
                        self.strparams = self.trno + '/' + self.rcdate

                        self.check_sql = hos.check_simsa(self.rcdate, self.trno, kwargs['meet'], kwargs['tbl_type'])
                        self.check_yn, self.check_db = check_recode.select_sql(self.check_sql, 'tuple')

                        if len(self.check_db) == 0:
                            self.params.append(self.strparams)
                        else:
                            continue
                else:
                    continue
        return self.params

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


class SimsaPage:
    def __init__(self):
        return

    def time_to_second_simsa(self, time):
        time.replace(' ', '')
        if time.find(':') > 0:
            self.time_splited = time.split(':')
            if len(self.time_splited) == 2:
                self.minute = int(self.time_splited[0])
                self.second_splited = self.time_splited[1].split('.')
                self.second = int(self.second_splited[0])
            elif len(self.time_splited) == 3:
                print(time)
            else:
                return None
            time = str(self.minute * 60 + self.second) + '.' + self.second_splited[1]
        else:
            if time.find('.') > 0:
                self.time_splited = time.split('.')
                time = self.time_splited[0] + '.' + self.time_splited[1]
            else:
                if time != '' and int(time) > 0:
                    time = time + '.0'
                else:
                    time = '0.0'
        # print(time)
        return time

    def simsa_get_page(self, **kwarg):
        self.ho_simsaorder = OrderedDict()
        self.ho_simsainfo = OrderedDict()
        self.rx = re.compile("\s+|\t+|\n+|''+")
        self.exclude_list = []
        self.simsa_err = False
        self.simsa_msg = ''

        url = 'http://race.kra.co.kr/referee/RacingTrainCheckScoreTable.do'
        params = {'meet': kwarg['meet'], 'trno': kwarg['trNo'], 'date': kwarg['rcDate'], 'Act': '02', 'Sub': '1'}

        self.simsa_page = HttpMethods()
        try:
            self.soup = self.simsa_page.post_url(url, params)

            if len(self.soup.findAll(text='자료가 없습니다.')) > 0:
                self.simsa_err = True
                return self.simsa_err, self.simsa_msg, self.ho_simsainfo, self.ho_simsaorder

            self.divs_sjb = self.soup.find_all('div', attrs={'class': 'tableType1'})

            self.ths_sjb = self.divs_sjb[0].find_all('th')
            if kwarg['meet'] == '2':
                self.dstc_je = self.ths_sjb[0].get_text().strip()
                self.dstc = int(self.dstc_je[-4:-1].replace(' ', ''))
            else:
                self.dstc = 1000

            self.race_type = self.ths_sjb[1].get_text().strip()

            self.ho_simsainfo[(kwarg['yymmdd_l_no'])] = SimsaHorang()
            self.ho_simsainfo[(kwarg['yymmdd_l_no'])].col['YYMMDD_L_NO'] = kwarg['yymmdd_l_no']
            self.ho_simsainfo[(kwarg['yymmdd_l_no'])].col['STAGE'] = 'SIMSA'
            self.ho_simsainfo[(kwarg['yymmdd_l_no'])].col['RACE_LEVEL_DIV'] = '주행심사'

            self.ho_simsainfo[(kwarg['yymmdd_l_no'])].col['RACE_SCALEWEIGHT_TYPE'] = self.race_type
            self.ho_simsainfo[(kwarg['yymmdd_l_no'])].col['RACE_NM'] = '주행심사'

            self.today_weather = self.ths_sjb[2].get_text().strip().split(':')
            self.ho_simsainfo[(kwarg['yymmdd_l_no'])].col['TODAY_RACE_WEATHER'] = self.today_weather[1].replace(' ', '')

            self.track_splited = self.ths_sjb[3].get_text().strip().replace(' ', '').split('(')
            self.m = re.search(r"(\d+)", self.track_splited[-1])

            if self.m is None:
                pass
            else:
                self.today_moisture = self.m.group()
                self.ho_simsainfo[kwarg['yymmdd_l_no']].col['TODAY_TRACK_MOISTURE'] = self.today_moisture
                self.today_track_splited = self.track_splited[0].split(':')
                self.today_track = self.today_track_splited[-1].replace(' ', '')
                self.ho_simsainfo[kwarg['yymmdd_l_no']].col['TODAY_TRACK_STATE'] = self.today_track

            self.divs = self.soup.find_all('div', attrs={'class': 'tableType2'})
            self.trs = self.divs[0].find_all('tr')

            self.ho_simsainfo[(kwarg['yymmdd_l_no'])].col['RACE_DISTANCE'] = self.dstc

            # rst_rank = 0

            for self.tr_i, self.tr in enumerate(self.trs):

                if self.tr_i == 0:
                    continue
                else:
                    # horse_lane = 0
                    self.tds_simsa = self.tr.find_all('td')
                    for self.td_i, self.td in enumerate(self.tds_simsa):
                        if self.td_i == 0:  # 번호

                            if self.td.get_text().strip() == '':
                                self.rst_rank = 99
                            else:
                                self.rst_rank = int(self.td.get_text().strip())

                        elif self.td_i == 1:  # 마번
                            self.horse_lane = self.td.get_text().strip().replace(' ', '')
                            if self.horse_lane != '':
                                self.lane_no = int(self.horse_lane)
                            else:
                                self.lane_no = 99
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)] = SimsaHorangSub()
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                'RHT_START_LANE_NO'] = self.lane_no
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['YYMMDD_L_NO'] = kwarg[
                                'yymmdd_l_no']
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['STAGE'] = 'SIMSA'
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['RACE_DISTANCE'] = self.dstc
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['RACE_LEVEL_DIV'] = '주행심사'
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                'RACE_SCALEWEIGHT_TYPE'] = self.race_type
                            # print(dstc)
                        elif self.td_i == 2:  # 마명
                            self.p = re.compile(r"(\')+(\d+)+(\'+\,+\')+(\d)")
                            self.cc = str(self.td.find('a'))
                            self.m = self.p.search(self.cc)
                            self.horse_num = self.m.group(2)
                            self.horse_nm = self.td.get_text().strip()
                            # print(horse_nm)
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['RH_NM'] = self.horse_nm
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['RH_ID_NO'] = self.horse_num
                        elif self.td_i == 3:
                            continue
                        elif self.td_i == 4:
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                'RH_PROD_AREA'] = self.td.get_text().strip()
                        elif self.td_i == 5:
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                'RH_GEN'] = self.td.get_text().strip()
                        elif self.td_i == 6:
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['RH_AGE'] = int(
                                self.td.get_text().strip())
                            # # print(td.get_text().strip(), end='\t')
                        elif self.td_i == 7:
                            self.scaleweight = self.td.get_text().strip().replace(' ', '').split('+')
                            # print(scaleweight)
                            if len(self.scaleweight) == 1:
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    'RHT_SCALEWEIGHT'] = float(self.scaleweight[0].replace("\'", ''))
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    'RHT_JOCKEY_WEIGTH'] = float(self.scaleweight[0].replace("\'", ''))
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    'RH_SCALEWEIGHT_DIFF'] = 0

                            elif len(self.scaleweight) == 2:
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    'RHT_SCALEWEIGHT'] = float(self.scaleweight[0].replace("\'", ''))
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    'RHT_JOCKEY_WEIGTH'] = float(self.scaleweight[0].replace("\'", ''))
                                if self.scaleweight[1] != '':
                                    self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                        'RH_SCALEWEIGHT_DIFF'] = float(self.scaleweight[1].replace("\'", ''))
                                else:
                                    self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                        'RH_SCALEWEIGHT_DIFF'] = 0

                        elif self.td_i == 8:
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                'RHT_JOCKEY_NM'] = self.td.get_text().strip()

                        elif self.td_i == 9:
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                'TRAINER_NM'] = self.td.get_text().strip()

                        elif self.td_i == 10:
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                'RH_OWNER'] = self.td.get_text().strip()
                        elif self.td_i == 11:
                            horse_weight = self.td.get_text().strip().replace(' ', '')
                            if horse_weight != '':
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['RHT_WEIGHT'] = float(
                                    horse_weight)
                        elif self.td_i == 12:
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                'RST_TIME'] = self.time_to_second_simsa(
                                re.sub(self.rx, '', self.td.get_text().strip().replace(' ', '')))
                        elif self.td_i == 13:
                            continue
                        elif self.td_i == 14:
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                'RHT_ACCESSORY'] = self.td.get_text().strip().replace(' ', '')

                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['RST_RANK'] = self.rst_rank
                        elif self.td_i == 15:
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                'RH_ENTER_YN_REASON'] = self.td.get_text().strip().replace(' ', '')
                        else:
                            continue
            self.trs_time = self.divs[1].find_all('tr')
        except IndexError as index:
            self.simsa_err = True
            self.simsa_msg = index + ' 페이지 점검중이니 나중에 해라.'
            return self.simsa_err, self.simsa_msg, self.ho_simsainfo, self.ho_simsaorder
        try:
            for self.tr_timei, self.tr_timev in enumerate(self.trs_time):
                if self.tr_timei == 0:
                    continue
                else:
                    self.tds_simsas = self.tr_timev.find_all('td')
                    if len(self.tds_simsas) < 3:
                        break
                    for self.tds_i, self.tds in enumerate(self.tds_simsas):

                        if self.tds_i == 0:
                            self.rst_rank = self.tds.get_text().strip().replace(' ', '')
                        elif self.tds_i == 1:
                            self.horse_lane = self.tds.get_text().strip().replace(' ', '')
                            if self.rst_rank == '' and self.horse_lane == '':
                                continue
                        elif self.tds_i == 2:
                            self.sectionrank = re.sub(self.rx, '', self.tds.get_text().strip()).split('-')

                            if self.sectionrank[0] != '':
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['S1FRANK'] = int(
                                    self.sectionrank[0].replace(' ', ''))
                            if self.sectionrank[3] != '':
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['3CRANK'] = int(
                                    self.sectionrank[3].replace(' ', ''))
                            if self.sectionrank[4] != '':
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['4CRANK'] = int(
                                    self.sectionrank[4].replace(' ', ''))
                            if self.sectionrank[5] != '':
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['G1FRANK'] = int(
                                    self.sectionrank[5].replace(' ', ''))
                        elif self.tds_i == 3:
                            self.s1ftime = self.time_to_second_simsa(
                                re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                            # print(s1ftime)
                            self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col['S1FTIME'] = self.s1ftime
                        elif self.tds_i == 4:  # 400
                            if kwarg['meet'] == '3':
                                continue
                            else:
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    '1CTIME'] = self.time_to_second_simsa(
                                    re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                        elif self.tds_i == 5:  # 400
                            if kwarg['meet'] == '3':
                                continue
                            else:
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    '2CTIME'] = self.time_to_second_simsa(
                                    re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                        elif self.tds_i == 6:  # 400
                            if kwarg['meet'] == '3':
                                continue
                            else:
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    '3CTIME'] = self.time_to_second_simsa(
                                    re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                        elif self.tds_i == 7:  # 400
                            if kwarg['meet'] == '3':
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    '3CTIME'] = self.time_to_second_simsa(
                                    re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                            else:
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    '4CTIME'] = self.time_to_second_simsa(
                                    re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                        elif self.tds_i == 8:  # 400
                            if kwarg['meet'] == '3':
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    '4CTIME'] = self.time_to_second_simsa(
                                    re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                            else:
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    'G3FTIME'] = self.time_to_second_simsa(
                                    re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                        elif self.tds_i == 9:  # G3FTIME G2FTIME
                            if kwarg['meet'] == '3':
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    'G3FTIME'] = self.time_to_second_simsa(
                                    re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                            else:
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    'G1FTIME'] = self.time_to_second_simsa(
                                    re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                        elif self.tds_i == 10:  # G1FTIME RST_TIME
                            if kwarg['meet'] == '3':
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    'G1FTIME'] = self.time_to_second_simsa(
                                    re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                            else:
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    'RST_TIME'] = self.time_to_second_simsa(
                                    re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                        elif self.tds_i == 11:  # RST_TIME G3FTIME
                            if kwarg['meet'] == '3':
                                self.ho_simsaorder[(kwarg['yymmdd_l_no'], self.horse_lane)].col[
                                    'RST_TIME'] = self.time_to_second_simsa(
                                    re.sub(self.rx, '', self.tds.get_text().strip().replace(' ', '')))
                            else:
                                continue
                        else:
                            continue
        except KeyError:
            self.simsa_err = True
            self.simsa_msg = self.simsa_msg + kwarg['yymmdd_l_no'] + 'key error' + '\n'
            return self.simsa_err, self.simsa_msg, self.ho_simsainfo, self.ho_simsaorder

        return self.simsa_err, self.simsa_msg, self.ho_simsainfo, self.ho_simsaorder


class TblHorang:  # 경마일자별경주정보
    def __init__(self):
        self.col = OrderedDict()

        self.col['YYMMDD_L_NO'] = ''  # VARCHAR(13)  NOT NULL COMMENT '20170930_S_01', -- 경마일자_장소_경주라운드
        self.col['RACE_LEVEL_DIV'] = ''  # VARCHAR(50)  NULL     COMMENT '등급구분', -- 등급구분
        self.col['RACE_DISTANCE'] = 0  # INT          NULL     COMMENT '경주거리', -- 경주거리
        self.col['RACE_NM'] = ''  # VARCHAR(100) NULL     COMMENT '경주명', -- 경주명
        self.col['EXPECT_START_TIME'] = ''  # DATETIME     NULL     COMMENT '예상경주시간', -- 예상경주시간
        self.col['RACE_SCALEWEIGHT_TYPE'] = ''  # VARCHAR(50)  NULL     COMMENT '부담중량방식', -- 부담중량방식
        self.col['RACE_RATING_SCALE_DIV'] = ''  # VARCHAR(50)  NULL     COMMENT '레이팅범위구분', -- 레이팅범위구분
        self.col['RACE_RH_ENTER_DIV'] = ''  # VARCHAR(50)  NULL     COMMENT '경주마성별연령참가구분', -- 경주마성별연령참가구분
        self.col['PRISE_MONEY1'] = 0  # INT          NULL     COMMENT '경주상금1', -- 경주상금1
        self.col['PRISE_MONEY2'] = 0  # INT          NULL     COMMENT '경주상금2', -- 경주상금2
        self.col['PRISE_MONEY3'] = 0  # INT          NULL     COMMENT '경주상금3', -- 경주상금3
        self.col['PRISE_MONEY4'] = 0  # INT          NULL     COMMENT '경주상금4', -- 경주상금4
        self.col['PRISE_MONEY5'] = 0  # INT          NULL     COMMENT '경주상금5', -- 경주상금5
        self.col['REAL_START_TIME'] = ''  # DATETIME     NULL     COMMENT '실제경주시간', -- 실제경주시간
        self.col['TODAY_RACE_WEATHER'] = ''  # VARCHAR(10)  NULL     COMMENT '당일경주날씨', -- 당일경주날씨
        self.col['TODAY_TRACK_STATE'] = ''  # VARCHAR(10)  NULL     COMMENT '당일경주주로상태', -- 당일경주주로상태
        self.col['TODAY_TRACK_MOISTURE'] = 0  # FLOAT(10,1)  NULL     COMMENT '당일경주함수율', -- 당일경주함수율
        self.col['WIN_SALE_AMT'] = 0  # INT          NULL     COMMENT '단승식매출액', -- 단승식매출액
        self.col['SHOW_SALE_AMT'] = 0  # INT          NULL     COMMENT '연승식매출액', -- 연승식매출액
        self.col['QUINELLA_SALE_AMT'] = 0  # INT          NULL     COMMENT '복승식매출액', -- 복승식매출액
        self.col['PERFECTA_SALE_AMT'] = 0  # INT          NULL     COMMENT '쌍승식매출액', -- 쌍승식매출액
        self.col['QUINELLA_PLACE_SALE_AMT'] = 0  # INT          NULL     COMMENT '복연승식매출액', -- 복연승식매출액
        self.col['QUINELLA_TREBLES_SALE_AMT'] = 0  # INT          NULL     COMMENT '삼복승식매출액', -- 삼복승식매출액
        self.col['TRIFECTA_SALE_AMT'] = 0  # INT          NULL     COMMENT '삼쌍승식매출액', -- 삼쌍승식매출액
        self.col['AMT_SUM'] = 0  # INT          NULL     COMMENT '합계매출액', -- 합계매출액
        self.col['RST_WIN_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '단승식순위마배당률', -- 단승식순위마배당률
        self.col['RST_SHOW_DRT1'] = ''  # FLOAT(10,2)  NULL     COMMENT '연승식순위마배당률1', -- 연승식순위마배당률1
        self.col['RST_SHOW_DRT2'] = ''  # FLOAT(10,2) NULL     COMMENT '연승식순위마배당률2', -- 연승식순위마배당률2
        self.col['RST_SHOW_DRT3'] = ''  # FLOAT(10,2)  NULL     COMMENT '연승식순위마배당률3', -- 연승식순위마배당률3
        self.col['RST_QUINELLA_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '복승식순위마배당률', -- 복승식순위마배당률
        self.col['RST_PERFECTA_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '쌍승식순위마배당률', -- 쌍승식순위마배당률
        self.col['RST_QUINELLA_PLACE_DRT1'] = ''  # FLOAT(10,2)  NULL     COMMENT '복연승식순위마배당률1', -- 복연승식순위마배당률1
        self.col['RST_QUINELLA_PLACE_DRT2'] = ''  # FLOAT(10,2)  NULL     COMMENT '복연승식순위마배당률2', -- 복연승식순위마배당률2
        self.col['RST_QUINELLA_PLACE_DRT3'] = ''  # FLOAT(10,2)  NULL     COMMENT '복연승식순위마배당률3', -- 복연승식순위마배당률3
        self.col['RST_QUINELLA_TREBLES_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '삼복승식순위마배당률', -- 삼복승식순위마배당률
        self.col['RST_TRIFECTA_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '삼쌍승식순위마배당률' -- 삼쌍승식순위마배당률
        self.col['STAGE'] = ''  # FLOAT(10,2)  NULL     COMMENT '삼쌍승식순위마배당률' -- 삼쌍승식순위마배당률


class TblHorangSub:  # 경주별상세정보
    def __init__(self):
        self.col = OrderedDict()

        self.col['YYMMDD_L_NO'] = ''  # VARCHAR(13) NOT NULL COMMENT 'yyyymmdd_s_no',-- 경마일자_장소_경주라운드
        self.col['RACE_DISTANCE'] = 0  # INT          NULL     COMMENT '경주거리', -- 경주거리
        self.col['RH_NM'] = ''  # VARCHAR(50) NOT NULL COMMENT '경주마이름', -- 경주마이름
        self.col['RH_ID_NO'] = ''
        self.col['RHT_START_LANE_NO'] = 0  # INT         NULL     COMMENT 'RACE_HORSE_TODAY_START_LANE_NO', -- 경주출발레인번호
        self.col['RH_PROD_AREA'] = ''  # VARCHAR(50) NULL     COMMENT '산지', -- 산지
        self.col['RH_GEN'] = ''  # CHAR(2)     NULL     COMMENT '성별', -- 성별
        self.col['RH_AGE'] = 0  # INT         NULL     COMMENT '연령', -- 연령
        self.col['RH_RATING'] = 0  # INT         NULL     COMMENT '레이팅', -- 레이팅
        self.col['RH_NEXT_RATING'] = 0  # INT         NULL     COMMENT '레이팅', -- 레이팅
        self.col['RHT_SCALEWEIGHT'] = 0  # INT         NULL     COMMENT '부담중량', -- 부담중량
        self.col['RH_SCALEWEIGHT_DIFF'] = 0.0  # INT         NULL     COMMENT '부담중량증감', -- 부담중량증감
        self.col['RHT_JOCKEY_NM'] = ''  # VARCHAR(10) NULL     COMMENT '기수명', -- 기수명
        self.col['TRAINER_NM'] = ''  # VARCHAR(10) NULL     COMMENT '조교사명', -- 조교사명
        self.col['RH_OWNER'] = ''  # VARCHAR(50) NULL     COMMENT '마주명', -- 마주명
        self.col['TRAIN_NUM'] = 0  # INT         NULL     COMMENT '훈련횟수', -- 훈련횟수
        self.col['ENTER_PERIOD'] = 0  # INT         NULL     COMMENT '출전주기', -- 출전주기
        self.col['RHT_ACCESSORY'] = ''  # VARCHAR(50) NULL     COMMENT '장구현황', -- 장구현황
        self.col['RH_ENTER_YN_REASON'] = ''  # VARCHAR(50) NULL     COMMENT '출주여부사유', -- 출주여부사유
        self.col['RHT_WEIGHT'] = 0.0  # INT         NULL     COMMENT '마체중', -- 마체중
        self.col['RH_WEIGHT_DIFF'] = 0.0  # INT         NULL     COMMENT '마체중증감', -- 마체중증감
        self.col['RACE_TERM'] = 0  # INT         NULL     COMMENT '출전휴식기간', -- 출전휴식기간
        self.col['RHT_JOCKEY_WEIGTH'] = 0.0  # INT         NULL     COMMENT '기수체중', -- 기수체중
        self.col['RST_RANK'] = 0  # INT         NULL     COMMENT '순위', -- 순위
        self.col['RST_TIME'] = 0.0  # DATETIME    NULL     COMMENT '경주기록시간', -- 경주기록시간
        self.col['S1FRANK'] = 0  # INT         NULL     COMMENT 'S-1F순위', -- S-1F순위
        self.col['S1FTIME'] = 0.0  # DATETIME    NULL     COMMENT 'S-1F시간', -- S-1F시간
        self.col['1CRANK'] = 0  # INT         NULL     COMMENT '1코너순위', -- 1코너순위
        self.col['1CTIME'] = 0.0  # DATETIME    NULL     COMMENT '1코너시간', -- 1코너시간
        self.col['2CRANK'] = 0  # INT         NULL     COMMENT '2코너순위', -- 2코너순위
        self.col['2CTIME'] = 0.0  # DATETIME    NULL     COMMENT '2코너시간', -- 2코너시간
        self.col['3CRANK'] = 0  # INT         NULL     COMMENT '3코너순위', -- 3코너순위
        self.col['3CTIME'] = 0.0  # DATETIME    NULL     COMMENT '3코너시간', -- 3코너시간
        self.col['4CRANK'] = 0  # INT         NULL     COMMENT '4코너순위', -- 4코너순위
        self.col['4CTIME'] = 0.0  # DATETIME    NULL     COMMENT '4코너시간', -- 4코너시간
        self.col['G3FTIME'] = 0.0  # DATETIME    NULL     COMMENT 'G-3F시간', -- G-3F시간
        self.col['G1FRANK'] = 0  # INT         NULL     COMMENT 'G-1F순위', -- G-1F순위
        self.col['G1FTIME'] = 0.0  # DATETIME    NULL     COMMENT 'G-1F시간', -- G-1F시간
        self.col['RH_WIN_DRT'] = 0.0  # FLOAT(10,2) NULL     COMMENT '단승배당', -- 단승배당
        self.col['RH_SHOW_DRT'] = 0.0  # FLOAT(10,2) NULL     COMMENT '연승배당' -- 연승배당
        self.col['RACE_LEVEL_DIV'] = ''
        self.col['RACE_SCALEWEIGHT_TYPE'] = ''
        self.col['STAGE'] = ''  # FLOAT(10,2)  NULL     COMMENT '삼쌍승식순위마배당률' -- 삼쌍승식순위마배당률


class ChulLink:
    def __init__(self):
        return

    def get_city_initial(self, meet):

        if meet == '1':
            city_initial = 'S'
        elif meet == '2':
            city_initial = 'J'
        elif meet == '3':
            city_initial = 'B'
        else:
            city_initial = None
        return city_initial

    def chul_get_link(self, **kwargs):

        self.chul_msg = ' > 출마표 페이지 리스트 '
        self.chul_list = []
        self.url = 'http://race.kra.co.kr/chulmainfo/ChulmaDetailInfoList.do?Act=02&Sub=1&meet=' + kwargs['meet']
        self.chul_err = False

        self.chul_link = HttpMethods()
        try:
            self.soup = self.chul_link.get_url(self.url)
            self.divs = self.soup.find('div', {'class': 'tableType2'})
            self.a_s = self.divs.find_all('a')
        except IndexError as index:
            self.chul_msg = index + ' 페이지 점검중이니 나중에 해라.'
            self.chul_err = True
            return self.chul_err, self.chul_msg, self.chul_list

            # try:
        self.check_recode = hoeq.SelectResult()

        for self.a in self.a_s:
            if '#KRA' in self.a.get('href'):
                # if a.attrs['onclick'].find('javascript:goChulmapyo(') >= 0:
                self.onclick = self.a.attrs['onclick'].replace('javascript:goChulmapyo(', '').replace(')',
                                                                                                      '').replace('"',
                                                                                                                  '').strip()
                self.onclick_list = self.onclick.split(',')

                if len(self.onclick_list) != 3:
                    continue

                self.yymmdd_l_no = self.onclick_list[1][0:8] + str(
                    100 + int(self.onclick_list[2])) + self.get_city_initial(self.onclick_list[0])

                self.check_info_sql = hos.check_info_link(self.yymmdd_l_no, kwargs['meet'], kwargs['tbl_type'])
                self.check_info_yn, self.check_info_db = self.check_recode.select_sql(self.check_info_sql, 'tuple')

                if len(self.check_info_db) > 0:
                    continue
                else:
                    self.chul_list.append(self.onclick_list)
            else:
                continue

        return self.chul_err, self.chul_msg, self.chul_list

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


class ChulPage:
    def __init__(self):
        return

    def get_city_initial(self, meet):

        if meet == '1':
            city_initial = 'S'
        elif meet == '2':
            city_initial = 'J'
        elif meet == '3':
            city_initial = 'B'
        else:
            city_initial = None
        return city_initial

    def chul_get_page(self, **kwargs):  # 출마정보 결과정보는 아래
        # print(meet+rcDate+rcNo)
        self.chul_msg = ' > 출마표 읽기'
        self.chul_day = OrderedDict()
        self.chul_detail = OrderedDict()
        self.url = 'http://race.kra.co.kr/chulmainfo/chulmaDetailInfoChulmapyo.do?Sub=1&meet=' + kwargs[
            'meet'] + '&Act=02'
        self.chul_error = False
        if kwargs['span']:
            self.links_v = kwargs['links_arranged_v'].split('/')
            self.rcNo = self.links_v[0]
            self.rcDate = self.links_v[1]
        else:
            self.rcDate = kwargs['links_arranged_v'][1]
            self.rcNo = kwargs['links_arranged_v'][2]

        self.yymmdd_l_no = self.rcDate + str(100 + int(self.rcNo)) + self.get_city_initial(kwargs['meet'])
        self.params = {'rcDate': self.rcDate, 'rcNo': self.rcNo}

        self.chul_page = HttpMethods()
        try:

            self.soup = self.chul_page.post_url(self.url, self.params)

            self.divs = self.soup.find_all('div', {'id': 'contents'})
            self.tables = self.divs[0].find_all('table')
            self.trs = self.tables[0].find_all('tr')
            self.ths = self.trs[0].find_all('th')

            if self.ths[0].get_text().find(self.rcDate[:4]) < 0:
                self.chul_msg = 'check page'
                return self.chul_error, self.chul_msg, self.chul_day, self.chul_detail

            else:
                self.chul_day[self.yymmdd_l_no] = TblHorang()
                self.chul_day[self.yymmdd_l_no].col['YYMMDD_L_NO'] = self.yymmdd_l_no
                self.chul_day[self.yymmdd_l_no].col['STAGE'] = 'CHULMA'

                self.expect_start_time = self.ths[1].get_text().strip()
                self.chul_day[self.yymmdd_l_no].col['EXPECT_START_TIME'] = self.expect_start_time

                self.tdss = self.trs[1].find_all('td')

                self.lebel_tdss = self.tdss[0].get_text().strip().replace('\xa0', ' ').split(' ')
                self.race_lebel = str(self.lebel_tdss[0])
                if self.race_lebel == '1등급' or self.race_lebel == '2등급':
                    self.race_lebel = '국' + self.race_lebel

                self.chul_day[self.yymmdd_l_no].col['RACE_LEVEL_DIV'] = self.race_lebel.replace('등급', '')

                self.dist_tdss = self.tdss[0].get_text().strip().split('M')
                self.chul_dstc = int(str(self.dist_tdss[0])[-4:])
                self.chul_day[self.yymmdd_l_no].col['RACE_DISTANCE'] = self.chul_dstc

                self.race_type = self.dist_tdss[-1].strip()
                self.chul_day[self.yymmdd_l_no].col['RACE_SCALEWEIGHT_TYPE'] = self.race_type

                self.chul_day[self.yymmdd_l_no].col['RACE_NM'] = self.tdss[1].get_text().strip()

                self.chul_day[self.yymmdd_l_no].col['RACE_RATING_SCALE_DIV'] = self.tdss[2].get_text().strip()

                self.chul_day[self.yymmdd_l_no].col['RACE_RH_ENTER_DIV'] = self.tdss[3].get_text().strip()

                self.info_trs = self.tables[1].find('tr')
                self.info_tds = self.info_trs.find_all('td')
                self.chul_day[self.yymmdd_l_no].col['PRISE_MONEY1'] = self.info_tds[0].get_text().strip().replace(',',
                                                                                                                  '').replace(
                    '원', '')
                self.chul_day[self.yymmdd_l_no].col['PRISE_MONEY2'] = self.info_tds[1].get_text().strip().replace(',',
                                                                                                                  '').replace(
                    '원', '')
                self.chul_day[self.yymmdd_l_no].col['PRISE_MONEY3'] = self.info_tds[2].get_text().strip().replace(',',
                                                                                                                  '').replace(
                    '원', '')
                self.chul_day[self.yymmdd_l_no].col['PRISE_MONEY4'] = self.info_tds[3].get_text().strip().replace(',',
                                                                                                                  '').replace(
                    '원', '')
                self.chul_day[self.yymmdd_l_no].col['PRISE_MONEY5'] = self.info_tds[4].get_text().strip().replace(',',
                                                                                                                  '').replace(
                    '원', '')

                self.trs = self.tables[2].find_all('tr')
                for self.tr_i, self.tr in enumerate(self.trs):
                    if self.tr_i == 0:  # 번호,마명,산지,성별,연령,레이팅,중량,증감,기수명,조교사명,마주명,훈련횟수,출전주기,장구현황,특이사항
                        pass
                    else:
                        self.tds = self.tr.find_all('td')
                        for self.td_i, self.td in enumerate(self.tds):
                            if self.td_i == 0:  # 번호
                                self.lane = self.td.get_text().strip()
                            elif self.td_i == 1:  # 마명
                                self.p = re.compile(r"(\')+(\d+)+(\'+\,+\s+\')+(\d)")
                                self.cc = str(self.td.find('a'))
                                self.m = self.p.search(self.cc)
                                self.horse_num = self.m.group(2)

                                self.horse_name_parse = self.td.get_text().strip().replace('★',
                                                                                           '')
                                self.horse_name = self.horse_name_parse.replace('[서]', '')
                                self.horse_name = self.horse_name.replace('[부]', '')
                                self.horse_name = self.horse_name.replace('[', '')
                                self.horse_name = self.horse_name.replace(']', '')
                                self.chul_detail[(self.yymmdd_l_no, self.lane)] = TblHorangSub()  # 출마정보 객체 생성
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col['YYMMDD_L_NO'] = self.yymmdd_l_no
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col['STAGE'] = 'CHULMA'
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col['RHT_START_LANE_NO'] = self.lane
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col['RH_NM'] = self.horse_name
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col['RH_ID_NO'] = self.horse_num
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col[
                                    'RACE_LEVEL_DIV'] = self.race_lebel.replace('등급', '')
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col[
                                    'RACE_SCALEWEIGHT_TYPE'] = self.race_type
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col['RACE_DISTANCE'] = self.chul_dstc
                            elif self.td_i == 2:  # 산지
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col[
                                    'RH_PROD_AREA'] = self.td.get_text().strip().replace(' ', '')
                            elif self.td_i == 3:  # 성별
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col[
                                    'RH_GEN'] = self.td.get_text().strip().replace(' ', '')
                            elif self.td_i == 4:  # 연령
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col['RH_AGE'] = int(
                                    self.td.get_text().strip())
                            elif self.td_i == 5:  # 레이팅
                                if self.td.get_text().strip().replace(' ', '') != '':
                                    self.rh_rt = self.td.get_text().strip().replace(' ', '').split('(')
                                    self.chul_detail[(self.yymmdd_l_no, self.lane)].col['RH_RATING'] = self.rh_rt[0]

                            elif self.td_i == 6:  # 중량
                                self.j_w = self.td.get_text().strip().replace('*', '')
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col['RHT_SCALEWEIGHT'] = self.j_w
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col['RHT_JOCKEY_WEIGTH'] = self.j_w
                            elif self.td_i == 7:  # 증감
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col[
                                    'RH_SCALEWEIGHT_DIFF'] = self.td.get_text().strip().replace(' ', '')
                            elif self.td_i == 8:  # 기수명
                                self.jockey_name_parse = self.td.get_text().strip()
                                self.jockey_name = re.sub(r'\(|-|[0-9]|\)', '', self.jockey_name_parse)
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col['RHT_JOCKEY_NM'] = self.jockey_name
                            elif self.td_i == 9:  # 조교사명
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col[
                                    'TRAINER_NM'] = self.td.get_text().strip()
                            elif self.td_i == 10:  # 마주명
                                self.chul_detail[(self.yymmdd_l_no, self.lane)].col[
                                    'RH_OWNER'] = self.td.get_text().strip().replace('♠', '').replace(" ", "")
                            elif self.td_i == 11:  # 훈련횟수
                                if kwargs['meet'] != '2':
                                    self.chul_detail[(self.yymmdd_l_no, self.lane)].col['TRAIN_NUM'] = int(
                                        self.td.get_text().strip())
                                else:
                                    self.chul_detail[(self.yymmdd_l_no, self.lane)].col[
                                        'ENTER_PERIOD'] = self.td.get_text().strip().replace('주', '')
                            elif self.td_i == 12:  # 훈련횟수
                                if kwargs['meet'] != '2':
                                    self.chul_detail[(self.yymmdd_l_no, self.lane)].col[
                                        'ENTER_PERIOD'] = self.td.get_text().strip().replace('주', '')
                                else:
                                    self.chul_detail[(self.yymmdd_l_no, self.lane)].col[
                                        'RHT_ACCESSORY'] = self.td.get_text().strip()
                            elif self.td_i == 13:  # 훈련횟수
                                if kwargs['meet'] != '2':
                                    self.chul_detail[(self.yymmdd_l_no, self.lane)].col[
                                        'RHT_ACCESSORY'] = self.td.get_text().strip()
                                else:
                                    pass
                            else:
                                continue
        except AttributeError as e:
            self.chul_msg = e
            self.chul_error = True

        return self.chul_error, self.chul_msg, self.chul_day, self.chul_detail

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


class UpdateTblHorang:  # 경마일자별경주정보
    def __init__(self):
        self.col = OrderedDict()
        self.col['REAL_START_TIME'] = ''  # DATETIME     NULL     COMMENT '실제경주시간', -- 실제경주시간
        self.col['TODAY_RACE_WEATHER'] = ''  # VARCHAR(10)  NULL     COMMENT '당일경주날씨', -- 당일경주날씨
        self.col['TODAY_TRACK_STATE'] = ''  # VARCHAR(10)  NULL     COMMENT '당일경주주로상태', -- 당일경주주로상태
        self.col['TODAY_TRACK_MOISTURE'] = 0  # FLOAT(10,1)  NULL     COMMENT '당일경주함수율', -- 당일경주함수율
        self.col['WIN_SALE_AMT'] = 0  # INT          NULL     COMMENT '단승식매출액', -- 단승식매출액
        self.col['SHOW_SALE_AMT'] = 0  # INT          NULL     COMMENT '연승식매출액', -- 연승식매출액
        self.col['QUINELLA_SALE_AMT'] = 0  # INT          NULL     COMMENT '복승식매출액', -- 복승식매출액
        self.col['PERFECTA_SALE_AMT'] = 0  # INT          NULL     COMMENT '쌍승식매출액', -- 쌍승식매출액
        self.col['QUINELLA_PLACE_SALE_AMT'] = 0  # INT          NULL     COMMENT '복연승식매출액', -- 복연승식매출액
        self.col['QUINELLA_TREBLES_SALE_AMT'] = 0  # INT          NULL     COMMENT '삼복승식매출액', -- 삼복승식매출액
        self.col['TRIFECTA_SALE_AMT'] = 0  # INT          NULL     COMMENT '삼쌍승식매출액', -- 삼쌍승식매출액
        self.col['AMT_SUM'] = 0  # INT          NULL     COMMENT '합계매출액', -- 합계매출액
        self.col['RST_WIN_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '단승식순위마배당률', -- 단승식순위마배당률
        self.col['RST_SHOW_DRT1'] = ''  # FLOAT(10,2)  NULL     COMMENT '연승식순위마배당률1', -- 연승식순위마배당률1
        self.col['RST_SHOW_DRT2'] = ''  # FLOAT(10,2) NULL     COMMENT '연승식순위마배당률2', -- 연승식순위마배당률2
        self.col['RST_SHOW_DRT3'] = ''  # FLOAT(10,2)  NULL     COMMENT '연승식순위마배당률3', -- 연승식순위마배당률3
        self.col['RST_QUINELLA_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '복승식순위마배당률', -- 복승식순위마배당률
        self.col['RST_PERFECTA_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '쌍승식순위마배당률', -- 쌍승식순위마배당률
        self.col['RST_QUINELLA_PLACE_DRT1'] = ''  # FLOAT(10,2)  NULL     COMMENT '복연승식순위마배당률1', -- 복연승식순위마배당률1
        self.col['RST_QUINELLA_PLACE_DRT2'] = ''  # FLOAT(10,2)  NULL     COMMENT '복연승식순위마배당률2', -- 복연승식순위마배당률2
        self.col['RST_QUINELLA_PLACE_DRT3'] = ''  # FLOAT(10,2)  NULL     COMMENT '복연승식순위마배당률3', -- 복연승식순위마배당률3
        self.col['RST_QUINELLA_TREBLES_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '삼복승식순위마배당률', -- 삼복승식순위마배당률
        self.col['RST_TRIFECTA_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '삼쌍승식순위마배당률' -- 삼쌍승식순위마배당률
        self.col['STAGE'] = ''


class UpdateTblHorangSub:  # 경주별상세정보
    def __init__(self):
        self.col = OrderedDict()
        self.col['RH_ENTER_YN_REASON'] = ''  # VARCHAR(50) NULL     COMMENT '출주여부사유', -- 출주여부사유
        self.col['RHT_WEIGHT'] = 0.0  # INT         NULL     COMMENT '마체중', -- 마체중
        self.col['RH_WEIGHT_DIFF'] = 0.0  # INT         NULL     COMMENT '마체중증감', -- 마체중증감
        self.col['RST_RANK'] = 0  # INT         NULL     COMMENT '순위', -- 순위
        self.col['RST_TIME'] = 0.0  # DATETIME    NULL     COMMENT '경주기록시간', -- 경주기록시간
        self.col['S1FRANK'] = 0  # INT         NULL     COMMENT 'S-1F순위', -- S-1F순위
        self.col['S1FTIME'] = 0.0  # DATETIME    NULL     COMMENT 'S-1F시간', -- S-1F시간
        self.col['1CRANK'] = 0  # INT         NULL     COMMENT '1코너순위', -- 1코너순위
        self.col['1CTIME'] = 0.0  # DATETIME    NULL     COMMENT '1코너시간', -- 1코너시간
        self.col['2CRANK'] = 0  # INT         NULL     COMMENT '2코너순위', -- 2코너순위
        self.col['2CTIME'] = 0.0  # DATETIME    NULL     COMMENT '2코너시간', -- 2코너시간
        self.col['3CRANK'] = 0  # INT         NULL     COMMENT '3코너순위', -- 3코너순위
        self.col['3CTIME'] = 0.0  # DATETIME    NULL     COMMENT '3코너시간', -- 3코너시간
        self.col['4CRANK'] = 0  # INT         NULL     COMMENT '4코너순위', -- 4코너순위
        self.col['4CTIME'] = 0.0  # DATETIME    NULL     COMMENT '4코너시간', -- 4코너시간
        self.col['G3FTIME'] = 0.0  # DATETIME    NULL     COMMENT 'G-3F시간', -- G-3F시간
        self.col['G1FRANK'] = 0  # INT         NULL     COMMENT 'G-1F순위', -- G-1F순위
        self.col['G1FTIME'] = 0.0  # DATETIME    NULL     COMMENT 'G-1F시간', -- G-1F시간
        self.col['RH_WIN_DRT'] = 0.0  # FLOAT(10,2) NULL     COMMENT '단승배당', -- 단승배당
        self.col['RH_SHOW_DRT'] = 0.0  # FLOAT(10,2) NULL     COMMENT '연승배당' -- 연승배당
        self.col['RACE_SCALEWEIGHT_TYPE'] = ''
        self.col['STAGE'] = ''


class ResultLink:
    def __init__(self):
        return

    def get_city_initial(self, meet):

        if meet == '1':
            city_initial = 'S'
        elif meet == '2':
            city_initial = 'J'
        elif meet == '3':
            city_initial = 'B'
        else:
            city_initial = None

        return city_initial

    def rst_get_link(self, **kwargs):

        self.rst_list = []
        self.update_list = []

        if kwargs['span']['span_mode']:
            self.url = 'http://race.kra.co.kr/raceScore/scoretablePeriodScoreList.do'
            self.params = {'nextFlag': 'false', 'meet': kwargs['meet'], 'realRcDate': '', 'realRcNo': '', 'Act': '04',
                           'Sub': '1',
                           'pageIndex': str(kwargs['page_num']), 'fromDate': kwargs['span']['span_from_to'][0],
                           'toDate': kwargs['span']['span_from_to'][1]}
        else:
            self.url = 'http://race.kra.co.kr/raceScore/ScoretableScoreList.do'
            self.params = {'nextFlag': 'false', 'meet': kwargs['meet'], 'Act': '04', 'Sub': '1',
                           'pageIndex': kwargs['page_num'],
                           'fromDate': '',
                           'toDate': ''}

        self.rst_link = HttpMethods()
        self.rst_err = False

        try:
            self.soup = self.rst_link.post_url(self.url, self.params)
            self.links = self.soup.find('p', {'class': 'crumbs'})

            if self.links.get_text().find('경주성적표') < 0:
                self.rst_msg = ' >> 결과 리스트 읽기 실패'
                return self.rst_err, self.rst_msg, self.rst_list, self.update_list
            else:
                self.a_s = self.soup.find_all('a')
                self.check_recode = hoeq.SelectResult()
                for self.a in self.a_s:
                    if self.a.attrs['href'].find('javascript:ScoreDetailPopup(') >= 0:
                        self.href = self.a.attrs['href'].replace('javascript:ScoreDetailPopup(', '').replace(');',
                                                                                                             '').replace(
                            "'",
                            "").strip()
                        self.href_list = self.href.split(',')
                        if len(self.href_list) != 3:
                            print(self.href_list)
                            self.rst_msg = ' >>> 결과 링크 읽기 실패' + self.href_list
                            continue
                        # print(self.href_list[0])
                        self.yymmdd_l_no = self.href_list[1][0:8] + str(100 + int(self.href_list[2])) + \
                                           self.get_city_initial(self.href_list[0])
                        # # print(yymmdd_l_no)
                        self.check_info_sql = hos.check_rst_link(self.yymmdd_l_no, kwargs["meet"], kwargs["tbl_type"])
                        self.check_info_yn, self.check_info_db = self.check_recode.select_sql(self.check_info_sql,
                                                                                              'tuple')

                        if len(self.check_info_db) > 0:
                            self.update_list.append(self.href_list)
                        else:
                            self.check_update_sql = hos.check_update_link(self.yymmdd_l_no, kwargs["meet"],
                                                                          kwargs["tbl_type"])
                            self.check_update_yn, self.check_update_db = self.check_recode.select_sql(
                                self.check_update_sql,
                                'tuple')
                            if len(self.check_update_db) < 1:
                                self.rst_list.append(self.href_list)
                            else:
                                continue
        except AttributeError as error:
            print(error)
            self.rst_err = True
        return self.rst_err, self.rst_list, self.update_list

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


class ResultPage:
    def __init__(self):
        return

    def get_city_initial(self, meet):

        if meet == '1':
            city_initial = 'S'
        elif meet == '2':
            city_initial = 'J'
        elif meet == '3':
            city_initial = 'B'
        else:
            city_initial = None
        return city_initial

    def time_to_second(self, time):
        if time != '':
            if time.find(':') > 0:
                self.time_splited = time.split(':')

                self.minute = int(self.time_splited[0])
                self.second_splited = self.time_splited[1].split('.')
                self.second = int(self.second_splited[0])
                self.strtime = str((self.minute * 60) + self.second) + '.' + self.second_splited[1]
                self.rsttime = float(self.strtime)
            else:
                self.rsttime = float(time)
        else:
            self.rsttime = 0.0

        # print(rsttime)
        return self.rsttime

    def rst_update_page(self, **kwargs):
        # print("어디가 이상하냐????")
        self.rst_error = False
        self.data_update_day = OrderedDict()
        self.data_update_detail = OrderedDict()

        self.rcDate = kwargs['rcDatercNo'][1]
        self.rcNo = kwargs['rcDatercNo'][2]

        self.yymmdd_l_no = self.rcDate + str(100 + int(self.rcNo)) + self.get_city_initial(kwargs['meet'])

        self.url = 'http://race.kra.co.kr/raceScore/ScoretableDetailList.do'

        self.params = {'nextFlag': 'false', 'meet': kwargs['meet'], 'realRcDate': self.rcDate, 'realRcNo': self.rcNo,
                       'Act': '04',
                       'Sub': '1', 'pageIndex': '1', 'fromDate': '', 'toDate': ''}

        self.rst_page = HttpMethods()
        try:
            self.rstsoup = self.rst_page.post_url(self.url, self.params)
            self.ca = self.rstsoup.find('li', {'class', 'on'}).find('a')
            self.findcaption = self.ca.get_text().strip().replace(" ", "")
        except AttributeError as e:

            self.rst_error = True
            return self.rst_error, None, None

        if self.findcaption == '경주별상세성적표':

            self.data_update_day[self.yymmdd_l_no] = UpdateTblHorang()
            self.data_update_day[self.yymmdd_l_no].col['STAGE'] = 'RESULT'

            self.rstdivs = self.rstsoup.find_all('div')
            self.rsttable = self.rstdivs[0].find_all('table')

            self.rst0trs = self.rsttable[0].find_all('tr')
            for self.rsttr_i, self.rsttr in enumerate(self.rst0trs):

                if self.rsttr_i == 0:
                    self.tds = self.rsttr.find_all('th')

                    for self.td_i, self.td in enumerate(self.tds):
                        if self.td_i == 0:  # 날짜,장소,날씨,경주로,함수율
                            continue
                        elif self.td_i == 1:  # 날짜,장소,날씨,경주로,함수율
                            continue
                        elif self.td_i == 2:  # 날짜,장소,날씨,경주로,함수율
                            self.data_update_day[self.yymmdd_l_no].col[
                                'TODAY_RACE_WEATHER'] = self.td.get_text().strip()
                        elif self.td_i == 3:  # 날짜,장소,날씨,경주로,함수율
                            self.data_update_day[self.yymmdd_l_no].col['TODAY_TRACK_STATE'] = self.td.get_text().strip()
                        elif self.td_i == 4:  # 날짜,장소,날씨,경주로,함수율
                            self.data_update_day[self.yymmdd_l_no].col[
                                'TODAY_TRACK_MOISTURE'] = self.td.get_text().strip().replace('%', '')
                        elif self.td_i == 5:  # 실제경주시간
                            self.data_update_day[self.yymmdd_l_no].col['REAL_START_TIME'] = self.td.get_text().strip()

                elif self.rsttr_i == 1:  # 등급, 거리,...
                    self.tds = self.rsttr.find_all('td')
                    for self.td_i, self.td in enumerate(self.tds):
                        if self.td_i == 2:  # 등급, 거리, 경주명
                            # self.td_splited = self.td.get_text().strip().replace('\xa0', '').split(' ')
                            self.race_type = self.td.get_text().strip()
                        else:
                            continue

            self.race_d_type = self.race_type
            self.rst1trs = self.rsttable[1].find_all('tr')
            for self.rsttr_i, self.rsttr in enumerate(self.rst1trs):
                if self.rsttr_i == 0:
                    continue
                else:
                    continue

            if kwargs['meet'] == '2':
                self.srsttrs = self.rsttable[4].find_all('tr')
            elif kwargs['meet'] == '1':
                self.srsttrs = self.rsttable[5].find_all('tr')
            elif kwargs['meet'] == '3':
                self.srsttrs = self.rsttable[6].find_all('tr')

            for self.rsttr_i, self.rsttr in enumerate(self.srsttrs):
                if self.rsttr_i == 0:  # 단승식, 연승식
                    self.tds = self.rsttr.find_all('td')
                    if len(self.tds) != 2:
                        return True, None, None

                    self.data_update_day[self.yymmdd_l_no].col['WIN_SALE_AMT'] = int(
                        self.tds[0].get_text().strip().replace(',', '').split(' ')[
                            -1])
                    self.data_update_day[self.yymmdd_l_no].col['SHOW_SALE_AMT'] = int(
                        self.tds[1].get_text().strip().replace(',', '').split(' ')[
                            -1])
                elif self.rsttr_i == 1:  # 복승식, 쌍승식
                    self.tds = self.rsttr.find_all('td')
                    if len(self.tds) != 2:
                        return True, None, None

                    if self.tds[0].get_text().strip().replace(',', '').split(':')[1] == '':
                        self.data_update_day[self.yymmdd_l_no].col['QUINELLA_SALE_AMT'] = 0
                    else:
                        self.data_update_day[self.yymmdd_l_no].col['QUINELLA_SALE_AMT'] = \
                            int(self.tds[0].get_text().strip().replace(',', '').split(' ')[-1])
                    if self.tds[1].get_text().strip().replace(',', '').split(':')[1] == '':
                        self.data_update_day[self.yymmdd_l_no].col['PERFECTA_SALE_AMT'] = 0
                    else:
                        self.data_update_day[self.yymmdd_l_no].col['PERFECTA_SALE_AMT'] = \
                            int(self.tds[1].get_text().strip().replace(',', '').split(' ')[-1])
                elif self.rsttr_i == 2:  # 복연승식, 삼복승식
                    self.tds = self.rsttr.find_all('td')
                    if len(self.tds) != 2:
                        return True, None, None

                    if self.tds[0].get_text().strip().replace(',', '').split(':')[1] == '':
                        self.data_update_day[self.yymmdd_l_no].col['QUINELLA_PLACE_SALE_AMT'] = 0
                    else:
                        self.data_update_day[self.yymmdd_l_no].col['QUINELLA_PLACE_SALE_AMT'] = \
                            int(self.tds[0].get_text().strip().replace(',', '').split(' ')[-1])
                    if self.tds[1].get_text().strip().replace(',', '').split(':')[1] == '':
                        self.data_update_day[self.yymmdd_l_no].col['QUINELLA_TREBLES_SALE_AMT'] = 0
                    else:
                        self.data_update_day[self.yymmdd_l_no].col['QUINELLA_TREBLES_SALE_AMT'] = \
                            int(self.tds[1].get_text().strip().replace(',', '').split(' ')[-1])
                elif self.rsttr_i == 3:  # 삼쌍승식, 합계
                    self.tds = self.rsttr.find_all('td')
                    if len(self.tds) != 2:
                        return True, None, None

                    if self.tds[0].get_text().strip().replace(',', '').split(':')[1] == '':
                        self.data_update_day[self.yymmdd_l_no].col['TRIFECTA_SALE_AMT'] = 0
                    else:
                        self.data_update_day[self.yymmdd_l_no].col['TRIFECTA_SALE_AMT'] = \
                            int(self.tds[0].get_text().strip().replace(',', '').split(' ')[-1])

                    if self.tds[1].get_text().strip().replace(',', '').split(':')[1] == '':
                        self.data_update_day[self.yymmdd_l_no].col['AMT_SUM'] = 0
                    else:
                        self.data_update_day[self.yymmdd_l_no].col['AMT_SUM'] = int(
                            self.tds[1].get_text().strip().replace(',', '').split(' ')[-1])
                    #  # print('AMT_SUM :' + tds[1].get_text().strip().replace(',', '').split(' ')[-1])

            # pttn = re.compile('(\d+\.\d+)+')
            if kwargs['meet'] == '2':
                self.brsttrs = self.rsttable[5].find_all('tr')
            elif kwargs['meet'] == '1':
                self.brsttrs = self.rsttable[6].find_all('tr')
            elif kwargs['meet'] == '3':
                self.brsttrs = self.rsttable[7].find_all('tr')

            for self.rsttr_i, self.rsttr in enumerate(self.brsttrs):
                if self.rsttr_i == 0:  # 단승식, 연승식
                    self.tds = self.rsttr.find_all('td')
                    if len(self.tds) != 2:
                        return True, None, None

                    self.td0_text = self.tds[0].get_text().strip().replace('  ', '').replace('\n', '').replace('\r',
                                                                                                               '').replace(
                        '\xa0', '')
                    for i in range(18):  # 원숫자 제거
                        self.td0_text = self.td0_text.replace(chr(0x2460 + i), '')

                    self.td0_splited = self.td0_text.split(' ')

                    if len(self.td0_splited) == 5:
                        self.data_update_day[self.yymmdd_l_no].col['RST_WIN_DRT'] = self.td0_splited[-1] + ',' + \
                                                                                    self.td0_splited[-3]
                    elif len(self.td0_splited) == 3:
                        self.data_update_day[self.yymmdd_l_no].col['RST_WIN_DRT'] = self.td0_splited[-1]

                    self.td_text = self.tds[1].get_text().strip().replace('  ', '').replace('\n', '').replace('\r',
                                                                                                              '').replace(
                        '\xa0', '')
                    for i in range(18):  # 원숫자 제거
                        self.td_text = self.td_text.replace(chr(0x2460 + i), '')

                    self.td_splited = self.td_text.split(' ')
                    # print(td_splited)
                    if len(self.td_splited) == 9:
                        self.data_update_day[self.yymmdd_l_no].col['RST_SHOW_DRT1'] = self.td_splited[-7]
                        self.data_update_day[self.yymmdd_l_no].col['RST_SHOW_DRT2'] = self.td_splited[-5]
                        self.data_update_day[self.yymmdd_l_no].col['RST_SHOW_DRT3'] = self.td_splited[-1] + ',' + \
                                                                                      self.td_splited[-3]
                    elif len(self.td_splited) == 7:
                        self.data_update_day[self.yymmdd_l_no].col['RST_SHOW_DRT1'] = self.td_splited[-5]
                        self.data_update_day[self.yymmdd_l_no].col['RST_SHOW_DRT2'] = self.td_splited[-3]
                        self.data_update_day[self.yymmdd_l_no].col['RST_SHOW_DRT3'] = self.td_splited[-1]

                    elif len(self.td_splited) == 5:
                        self.data_update_day[self.yymmdd_l_no].col['RST_SHOW_DRT1'] = self.td_splited[-3]
                        self.data_update_day[self.yymmdd_l_no].col['RST_SHOW_DRT2'] = self.td_splited[-1]

                elif self.rsttr_i == 1:  # 복승식, 쌍승식
                    self.tds = self.rsttr.find_all('td')
                    if len(self.tds) != 2:
                        return True, None, None

                    self.td0_text = self.tds[0].get_text().strip().replace('  ', '').replace('\n', '').replace('\r',
                                                                                                               '').replace(
                        '\xa0', '')
                    # print(td0_text)
                    if self.td0_text.split(':')[1] == '':
                        self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_DRT'] = 0
                    else:
                        for i in range(18):  # 원숫자 제거
                            self.td0_text = self.td0_text.replace(chr(0x2460 + i), '')
                        # print(td0_text)
                        self.td0_splited = self.td0_text.split(' ')

                        if len(self.td0_splited) == 5:
                            self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_DRT'] = self.td0_splited[
                                                                                                 -1] + ',' + \
                                                                                             self.td0_splited[-3]
                        elif len(self.td0_splited) == 3:
                            self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_DRT'] = self.td0_splited[-1]

                    self.td_text = self.tds[1].get_text().strip().replace('  ', '').replace('\n', '').replace('\r',
                                                                                                              '').replace(
                        '\xa0', '')

                    if self.td_text.split(':')[1] == '':
                        self.data_update_day[self.yymmdd_l_no].col['RST_PERFECTA_DRT'] = 0
                    else:

                        for self.i in range(18):  # 원숫자 제거
                            self.td_text = self.td_text.replace(chr(0x2460 + self.i), '')

                        self.td_splited = self.td_text.split(' ')

                        if len(self.td_splited) == 5:
                            self.data_update_day[self.yymmdd_l_no].col['RST_PERFECTA_DRT'] = self.td_splited[-1] + ',' + \
                                                                                             self.td_splited[-3]
                        elif len(self.td_splited) == 3:
                            self.data_update_day[self.yymmdd_l_no].col['RST_PERFECTA_DRT'] = self.td_splited[-1]

                elif self.rsttr_i == 2:  # 복연승식, 삼복승식
                    self.tds = self.rsttr.find_all('td')
                    if len(self.tds) != 2:
                        return True, None, None

                    self.td0_text = self.tds[0].get_text().strip().replace('  ', '').replace('\n', '').replace('\r',
                                                                                                               '').replace(
                        '\xa0', '')
                    if self.td0_text.split(':')[1] == '':
                        self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_TREBLES_DRT'] = 0
                    else:
                        for self.i in range(18):  # 원숫자 제거
                            self.td0_text = self.td0_text.replace(chr(0x2460 + i), '')

                        self.td0_splited = self.td0_text.split(' ')

                        if len(self.td0_splited) == 9:
                            self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_PLACE_DRT1'] = self.td0_splited[-7]
                            self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_PLACE_DRT2'] = self.td0_splited[-5]
                            self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_PLACE_DRT3'] = self.td0_splited[
                                                                                                        -1] + ',' + \
                                                                                                    self.td0_splited[-3]
                        elif len(self.td0_splited) == 7:
                            self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_PLACE_DRT1'] = self.td0_splited[-5]
                            self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_PLACE_DRT2'] = self.td0_splited[-3]
                            self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_PLACE_DRT3'] = self.td0_splited[-1]

                    self.td_text = self.tds[1].get_text().strip().replace('  ', '').replace('\n', '').replace('\r',
                                                                                                              '').replace(
                        '\xa0', '')
                    if self.td_text.split(':')[1] == '':
                        self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_TREBLES_DRT'] = 0
                    else:
                        for i in range(20):  # 원숫자 제거
                            self.td_text = self.td_text.replace(chr(0x2460 + i), '')

                        self.td_splited = self.td_text.split(' ')

                        if len(self.td_splited) == 5:
                            self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_TREBLES_DRT'] = self.td_splited[
                                                                                                         -1] + ',' + \
                                                                                                     self.td_splited[-3]
                        elif len(self.td_splited) == 3:
                            self.data_update_day[self.yymmdd_l_no].col['RST_QUINELLA_TREBLES_DRT'] = self.td_splited[-1]
                            #  # print(td_splited[-1])

                elif self.rsttr_i == 3:  # 삼쌍승식, 합계
                    self.tds = self.rsttr.find_all('td')
                    if len(self.tds) != 2:
                        # print('이상')
                        continue
                    if self.tds[0].get_text().strip().replace(',', '').split(':')[1] == '':
                        self.data_update_day[self.yymmdd_l_no].col['RST_TRIFECTA_DRT'] = 0
                    else:
                        self.data_update_day[self.yymmdd_l_no].col['RST_TRIFECTA_DRT'] = \
                            self.tds[0].get_text().strip().split(' ')[-1]

                        # print(tds[0].get_text().strip().split(':')[-1])

            self.rst2trs = self.rsttable[2].find_all('tr')

            for self.rsttr_i, self.rsttr in enumerate(self.rst2trs):
                if self.rsttr_i == 0:  # 번호,마명,산지,성별,연령,레이팅,중량,증감,기수명,조교사명,마주명,훈련횟수,출전주기,장구현황,특이사항
                    pass
                else:
                    self.rsttds = self.rsttr.find_all('td')
                    for self.rsttd_i, self.rsttd in enumerate(self.rsttds):
                        if self.rsttd_i == 0:  # t순위
                            if self.rsttd.get_text().strip() == '':
                                self.rstrank = 99
                            else:
                                self.rstrank = int(self.rsttd.get_text().strip())

                        elif self.rsttd_i == 1:  # 마번
                            self.lane = int(self.rsttd.get_text().strip())
                        elif self.rsttd_i == 2:  # 마명
                            self.parse = self.rsttd.get_text().strip().replace('★',
                                                                               '')  # '★' 표시는 끝번 신청말, 체중페이지에는 표시 되지 않아 지운다.
                            self.horse_name = self.parse.replace('[서]', '')
                            self.horse_name = self.horse_name.replace('[부]', '')
                            self.horse_name = self.horse_name.replace('[', '')
                            self.horse_name = self.horse_name.replace(']', '')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)] = UpdateTblHorangSub()  # 출마정보 객체 생성
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['STAGE'] = 'RESULT'
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col[
                                'RACE_SCALEWEIGHT_TYPE'] = self.race_d_type
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['RST_RANK'] = self.rstrank

                        elif self.rsttd_i > 2 and self.rsttd_i < 12:  # 산지
                            pass

                        elif self.rsttd_i == 12:  # ㅋㅋ
                            self.weight_splited = self.rsttd.get_text().strip().replace(')', '').split('(')
                            if self.weight_splited[0] == '':
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['RHT_WEIGHT'] = 0
                            else:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['RHT_WEIGHT'] = float(
                                    self.weight_splited[0])  # 마체중
                            if self.weight_splited[1] == '':
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['RH_WEIGHT_DIFF'] = 0
                            else:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['RH_WEIGHT_DIFF'] = float(
                                    self.weight_splited[1])  # 마체중 증감

                        elif self.rsttd_i == 13:  # ㅋㅋ
                            if self.rsttd.get_text().strip() == '----':
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['RH_WIN_DRT'] = 0.0
                            else:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['RH_WIN_DRT'] = float(
                                    self.rsttd.get_text().strip())
                        elif self.rsttd_i == 14:  # ㅋㅋ
                            if self.rsttd.get_text().strip() == '----':
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['RH_SHOW_DRT'] = 0.0
                            else:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['RH_SHOW_DRT'] = float(
                                    self.rsttd.get_text().strip())

                        elif self.rsttd_i == 15:  # ㅋㅋ
                            pass
                        else:
                            pass

            self.rst3trs = self.rsttable[3].find_all('tr')
            self.header1 = []
            self.header = []
            for self.tr_i, self.tr in enumerate(self.rst3trs):

                if kwargs['meet'] == '1':
                    if self.tr_i == 0:  # 헤더

                        self.ths = self.tr.find_all('th')
                        for self.th in self.ths:
                            self.header.append(
                                self.th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace(
                                    '\r',
                                    '').replace(
                                    '\xa0',
                                    ''))
                    elif self.tr_i == 1:
                        self.ths = self.tr.find_all('th')
                        for self.th in self.ths:
                            self.header1.append(
                                self.th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace(
                                    '\r',
                                    '').replace(
                                    '\xa0',
                                    ''))

                    else:
                        self.tds = self.tr.find_all('td')

                        for self.td_i, self.td in enumerate(self.tds):
                            if self.td_i == 0:
                                continue
                            elif self.td_i == 1:
                                self.lane = int(self.td.get_text().strip())
                            elif self.td_i == 2:
                                self.td_text = self.td.get_text().strip().replace(' ', '').replace('\t',
                                                                                                   '').replace(
                                    '\n',
                                    '').replace(
                                    '\r',
                                    '').replace(
                                    '\xa0', '')
                                self.sc_rank_splited = self.td_text.split('-')
                                # print(self.sc_rank_splited)

                                try:
                                    self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['S1FRANK'] = int(
                                        self.sc_rank_splited[0])
                                except ValueError:
                                    self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['S1FRANK'] = 91

                                try:
                                    self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['1CRANK'] = int(
                                        self.sc_rank_splited[1])
                                except ValueError:
                                    self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['1CRANK'] = 91

                                try:
                                    self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['2CRANK'] = int(
                                        self.sc_rank_splited[2])
                                except ValueError:
                                    self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['2CRANK'] = 91

                                try:
                                    self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['3CRANK'] = int(
                                        self.sc_rank_splited[3])
                                except ValueError:
                                    self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['3CRANK'] = 91

                                try:
                                    self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['4CRANK'] = int(
                                        self.sc_rank_splited[5])
                                except ValueError:
                                    self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['4CRANK'] = 91

                                try:
                                    self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['G1FRANK'] = int(
                                        self.sc_rank_splited[6])
                                except ValueError:
                                    self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['G1FRANK'] = 91

                            elif self.td_i == 3:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col[
                                    'S1FTIME'] = self.time_to_second(
                                    self.td.get_text().strip())
                            elif self.td_i == 4:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col[
                                    '1CTIME'] = self.time_to_second(
                                    self.td.get_text().strip())
                            elif self.td_i == 5:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col[
                                    '2CTIME'] = self.time_to_second(
                                    self.td.get_text().strip())

                            elif self.td_i == 6:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col[
                                    '3CTIME'] = self.time_to_second(
                                    self.td.get_text().strip())
                            elif self.td_i == 7:
                                continue
                            elif self.td_i == 8:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col[
                                    '4CTIME'] = self.time_to_second(
                                    self.td.get_text().strip())

                            elif self.td_i == 9:
                                continue

                            elif self.td_i == 10:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col[
                                    'G3FTIME'] = self.time_to_second(
                                    self.td.get_text().strip())

                            elif self.td_i == 11:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col[
                                    'G1FTIME'] = self.time_to_second(
                                    self.td.get_text().strip())


                            elif self.td_i == 12:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col[
                                    'RST_TIME'] = self.time_to_second(
                                    self.td.get_text().strip())

                # 순위가 없는 경우(출전제외 등의 이유)

                elif kwargs['meet'] == '2':  # 부산은 헤더가 다름
                    if self.tr_i == 0:  # 헤더

                        self.ths = self.tr.find_all('th')
                        for self.th in self.ths:
                            self.header.append(
                                self.th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace(
                                    '\r',
                                    '').replace(
                                    '\xa0',
                                    ''))
                    else:
                        self.tds = self.tr.find_all('td')
                        pass

                        try:
                            self.td_index = self.header.index('순위')
                            if self.tds[self.td_index].get_text().strip() == '':
                                continue
                        except:
                            return True, None, None

                        # 마번
                        try:
                            self.td_index = self.header.index('마번')
                            self.lane = int(self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        try:
                            self.td_index = self.header.index('S1F-1C-2C-3C-4C-G1F')

                            self.td_text = self.tds[self.td_index].get_text().strip().replace(' ',
                                                                                              '').replace('\t',
                                                                                                          '').replace(
                                '\n',
                                '').replace(
                                '\r',
                                '').replace(
                                '\xa0', '')
                            self.sc_rank_splited = self.td_text.split('-')
                            try:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['S1FRANK'] = int(
                                    self.sc_rank_splited[0])
                            except ValueError:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['S1FRANK'] = 91

                            try:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['1CRANK'] = int(
                                    self.sc_rank_splited[1])
                            except ValueError:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['1CRANK'] = 91

                            try:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['2CRANK'] = int(
                                    self.sc_rank_splited[2])
                            except ValueError:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['2CRANK'] = 91

                            try:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['3CRANK'] = int(
                                    self.sc_rank_splited[3])
                            except ValueError:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['3CRANK'] = 91

                            try:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['4CRANK'] = int(
                                    self.sc_rank_splited[4])
                            except ValueError:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['4CRANK'] = 91

                            try:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['G1FRANK'] = int(
                                    self.sc_rank_splited[5])
                            except ValueError:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['G1FRANK'] = 91
                        except:
                            return True, None, None

                        try:
                            self.td_index = self.header.index('S-1F')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['S1FTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        # 1코너
                        try:
                            self.td_index = self.header.index('1코너')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['1CTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        # 2코너
                        try:
                            self.td_index = self.header.index('2코너')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['2CTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        # 3코너
                        try:
                            self.td_index = self.header.index('3코너')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['3CTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        # 2코너
                        try:
                            self.td_index = self.header.index('4코너')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['4CTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        # G-3F
                        try:
                            self.td_index = self.header.index('G-3F')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['G3FTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        try:
                            self.td_index = self.header.index('G-1F')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['G1FTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        try:
                            self.td_index = self.header.index('경주기록')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col[
                                'RST_TIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None



                elif kwargs['meet'] == '3':

                    if self.tr_i == 0:  # 헤더

                        self.ths = self.tr.find_all('th')
                        for self.th in self.ths:
                            self.header.append(
                                self.th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace(
                                    '\r',
                                    '').replace(
                                    '\xa0',
                                    ''))
                        # print(self.header)

                    else:
                        self.tds = self.tr.find_all('td')
                        try:
                            self.td_index = self.header.index('순위')
                            if self.tds[self.td_index].get_text().strip() == '':
                                continue
                        except:
                            return True, None, None

                        # 마번
                        try:
                            self.td_index = self.header.index('마번')
                            self.lane = int(self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        try:
                            self.td_index = self.header.index('구간별통과순위(구간기록의이해/활용)S1F-G8F-G6F-G4F-G3F-G2F-G1F')
                            self.td_text = self.tds[self.td_index].get_text().strip().replace(' ', '').replace('\t',
                                                                                                               '').replace(
                                '\n',
                                '').replace(
                                '\r',
                                '').replace(
                                '\xa0', '')
                            self.sc_rank_splited = self.td_text.split('-')

                            try:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['S1FRANK'] = int(
                                    self.sc_rank_splited[0])
                            except ValueError:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['S1FRANK'] = 91

                            try:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['1CRANK'] = int(
                                    self.sc_rank_splited[2])
                            except ValueError:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['1CRANK'] = 91

                            try:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['2CRANK'] = int(
                                    self.sc_rank_splited[3])
                            except ValueError:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['2CRANK'] = 91

                            try:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['3CRANK'] = int(
                                    self.sc_rank_splited[4])
                            except ValueError:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['3CRANK'] = 91

                            try:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['4CRANK'] = int(
                                    self.sc_rank_splited[5])
                            except ValueError:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['4CRANK'] = 91

                            try:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['G1FRANK'] = int(
                                    self.sc_rank_splited[6])
                            except ValueError:
                                self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['G1FRANK'] = 91
                        except:
                            return True, None, None

                        try:
                            self.td_index = self.header.index('S-1F')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['S1FTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        # 1코너
                        try:
                            self.td_index = self.header.index('8-6F')
                        except:
                            return True, None, None

                        self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['1CTIME'] = self.time_to_second(
                            self.tds[self.td_index].get_text().strip())

                        # 2코너
                        try:
                            self.td_index = self.header.index('6-4F')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['2CTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        # 3코너
                        try:
                            self.td_index = self.header.index('4-2F')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['3CTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        # 2코너
                        try:
                            self.td_index = self.header.index('2F-G')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['4CTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        # G-3F
                        try:
                            self.td_index = self.header.index('3F-G')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['G3FTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        # print('G3FTIME')
                        # print(hocm.time_to_second(tds[td_index].get_text().strip()))
                        # G-1F
                        try:
                            self.td_index = self.header.index('1F-G')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col['G1FTIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

                        # 경주기록
                        try:
                            self.td_index = self.header.index('경주기록')
                            self.data_update_detail[(self.yymmdd_l_no, self.lane)].col[
                                'RST_TIME'] = self.time_to_second(
                                self.tds[self.td_index].get_text().strip())
                        except:
                            return True, None, None

        return self.rst_error, self.data_update_day, self.data_update_detail


class Bedang:
    def __init__(self):
        self.col = OrderedDict()
        self.col['YYMMDD_L_NO'] = ''
        self.col['NUM_HO'] = 0
        self.col['RACE_RANK'] = ''
        self.col['RST_WIN_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '단승식순위마배당률', -- 단승식순위마배당률
        self.col['RST_SHOW_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '연승식순위마배당률1', -- 연승식순위마배당률1
        self.col['RST_QUINELLA_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '복승식순위마배당률', -- 복승식순위마배당률
        self.col['RST_PERFECTA_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '쌍승식순위마배당률', -- 쌍승식순위마배당률
        self.col['RST_QUINELLA_PLACE_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '복연승식순위마배당률1', -- 복연승식순위마배당률1
        self.col['RST_QUINELLA_TREBLES_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '삼복승식순위마배당률', -- 삼복승식순위마배당률
        self.col['RST_TRIFECTA_DRT'] = ''  # FLOAT(10,2)  NULL     COMMENT '삼쌍승식순위마배당률' -- 삼쌍승식순위마배당률
        self.col['STAGE'] = ''


class AboutOddsInPage:  # 경주별상세정보
    def __init__(self):
        self.odds_page = HttpMethods()
        return

    def odds_win_page_read(self, yymmdd, num_ho):  # 결과정보 결과정보는 아래

        odds_msg = '배당페이지 읽기 '

        rcDate = yymmdd[0:8]
        rcNo = yymmdd[9:11]

        if yymmdd[-1] == "S":
            meet = '1'
        elif yymmdd[-1] == "J":
            meet = '2'
        if yymmdd[-1] == "B":
            meet = '3'

        url_odds = 'https://race.kra.co.kr/raceScore/ScoretableBettingprofitScm.do'

        params = {'Act': '04', 'Sub': '1', 'meet': meet, 'fromDate': '', 'toDate': '',
                  'hrNo': '', 'jkNo': '', 'trNo': '', 'owNo': '',
                  'realRcDate': rcDate, 'realRcNo': rcNo}
        # win_odds = HttpMethods()
        win_drt = ''
        race_rank = ''
        page_error = False
        try:
            soup = self.odds_page.post_url(url_odds, params)
            fieldset_v = soup.find('fieldset', attrs={'class': 'searchBox'})
            p_v = fieldset_v.find('p')
            span_v = p_v.find('span')
            race_rank = span_v.get_text().strip()
            race_rank = race_rank.replace('  ', '').replace('\n', ',').replace('\r', '').replace(
                '\xa0', '')
            race_rank = race_rank.replace(',,', ',')

            tables = soup.find_all('table')

            ca = tables[0].find('caption')
            # findcaption = ca.get_text().strip()
            # print(findcaption)

        except AttributeError as e:
            odds_msg += " >>> 페이지 결과 없음"
            page_error = True
            return page_error, win_drt, race_rank

        header1 = []
        header2 = []
        header3 = []
        trs = tables[0].find_all('tr')
        # bedang = dict()

        for tr_i, tr in enumerate(trs):
            if tr_i == 0:
                ths = tr.find_all('th')
                for th in ths:
                    header1.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))

            elif tr_i == 1:
                ths = tr.find_all('th')
                for th in ths:
                    header2.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))

            elif tr_i == 2:
                ths = tr.find_all('th')
                for th in ths:
                    header3.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))

            elif tr_i > 2 and tr_i < num_ho + 3:
                td_b = tr.find_all('td')

                for td_i, td in enumerate(td_b):
                    if td_i == 0:
                        win = td.get_text().strip()
                        if win == '-' or win == '----' or win == '0.':
                            continue
                    elif td_i == 1:
                        pass
                    elif td_i == 2:
                        start_lane_row = td.get_text().strip()
                        bedang_m = "(" + start_lane_row + "):" + win + "/"
                        # bedang[bedang_m] = win
                        win_drt += bedang_m
                    else:
                        break
            else:
                break
        win_drt = win_drt[0:-1]
        return page_error, win_drt, race_rank

    def odds_show_page_read(self, yymmdd, num_ho):  # 결과정보 결과정보는 아래

        odds_msg = '배당페이지 읽기 '
        page_error = False

        rcDate = yymmdd[0:8]
        rcNo = yymmdd[9:11]

        if yymmdd[-1] == "S":
            meet = '1'
        elif yymmdd[-1] == "J":
            meet = '2'
        if yymmdd[-1] == "B":
            meet = '3'

        url_odds = 'https://race.kra.co.kr/raceScore/ScoretableBettingprofitScm.do'

        params = {'Act': '04', 'Sub': '1', 'meet': meet, 'fromDate': '', 'toDate': '',
                  'hrNo': '', 'jkNo': '', 'trNo': '', 'owNo': '',
                  'realRcDate': rcDate, 'realRcNo': rcNo}

        # show_odds = HttpMethods()
        show_drt = ''
        try:
            soup = self.odds_page.post_url(url_odds, params)
            tables = soup.find_all('table')
            ca = tables[0].find('caption')
            findcaption = ca.get_text().strip()
            odds_msg += ' >>> ' + findcaption
        except AttributeError as e:
            odds_msg += " >>> 페이지 결과 없음"
            page_error = True
            return page_error, show_drt

        header1 = []
        header2 = []
        header3 = []

        # bedang = dict()
        trs = tables[0].find_all('tr')

        for tr_i, tr in enumerate(trs):

            if tr_i == 0:
                ths = tr.find_all('th')
                for th in ths:
                    header1.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))
                # print(header1)
            elif tr_i == 1:
                ths = tr.find_all('th')
                for th in ths:
                    header2.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))
                # print(header2)
            elif tr_i == 2:
                ths = tr.find_all('th')
                for th in ths:
                    header3.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))
                # print(header3)
            elif tr_i > 2 and tr_i < num_ho + 3:

                td_b = tr.find_all('td')

                for td_i, td in enumerate(td_b):
                    # print(td.get_text().strip())
                    if td_i == 0:
                        pass
                    elif td_i == 1:
                        show = td.get_text().strip()
                        if show == '-' or show == '----' or show == '0.':
                            continue
                    elif td_i == 2:
                        start_lane_row = td.get_text().strip()
                        bedang_m = "(" + start_lane_row + "):" + show + "/"
                        # bedang[bedang_m] = show
                        show_drt += bedang_m
                    else:
                        break
            else:
                break
        show_drt = show_drt[0:-1]
        return page_error, show_drt

    def odds_quinella_page_read(self, yymmdd, num_ho):  # 결과정보 결과정보는 아래

        odds_msg = '배당페이지 읽기 '
        page_error = False

        rcDate = yymmdd[0:8]
        rcNo = yymmdd[9:11]

        if yymmdd[-1] == "S":
            meet = '1'
        elif yymmdd[-1] == "J":
            meet = '2'
        if yymmdd[-1] == "B":
            meet = '3'

        url_odds = 'https://race.kra.co.kr/raceScore/ScoretableBettingprofitScm.do'

        params = {'Act': '04', 'Sub': '1', 'meet': meet, 'fromDate': '', 'toDate': '',
                  'hrNo': '', 'jkNo': '', 'trNo': '', 'owNo': '',
                  'realRcDate': rcDate, 'realRcNo': rcNo}

        # quinella_odds = HttpMethods()
        quinella_drt = ''
        try:
            soup = self.odds_page.post_url(url_odds, params)
            tables = soup.find_all('table')
            ca = tables[0].find('caption')
            findcaption = ca.get_text().strip()
            # odds_msg += ' >>> ' + findcaption
        except AttributeError as e:
            odds_msg += " >>> 페이지 결과 없음"
            page_error = True
            return page_error, quinella_drt

        header1 = []
        header2 = []
        header3 = []

        # bedang = dict()
        trs = tables[0].find_all('tr')
        for tr_i, tr in enumerate(trs):
            res = []
            if tr_i == 0:
                ths = tr.find_all('th')
                for th in ths:
                    header1.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))
                # print(header1)
            elif tr_i == 1:
                ths = tr.find_all('th')
                for th in ths:
                    header2.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))
                # print(header2)
            elif tr_i == 2:
                ths = tr.find_all('th')
                for th in ths:
                    header3.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))
                # print(header3)

            elif tr_i > 2 and tr_i < num_ho + 3:
                td_b = tr.find_all('td')
                for td_i, td in enumerate(td_b):
                    if td_i == 0:
                        pass
                    elif td_i == 1:
                        pass
                    elif td_i == 2:
                        start_lane_row = td.get_text().strip()
                    elif td_i > 2 and td_i <= num_ho + 2:
                        quinella = td.get_text().strip()
                        if quinella == '-' or quinella == '----':
                            pass
                        else:
                            bedang_m = "(" + start_lane_row + "," + str(td_i - 2) + "):" + quinella + "/"
                            quinella_drt += bedang_m
                    else:
                        break
            else:
                break
        quinella_drt = quinella_drt[0:-1]
        # print("복승식 :", quinella_drt)
        return page_error, quinella_drt

    def odds_perfetica_page_read(self, yymmdd, num_ho):  # 결과정보 결과정보는 아래

        odds_msg = '배당페이지 읽기 '
        page_error = False

        rcDate = yymmdd[0:8]
        rcNo = yymmdd[9:11]

        if yymmdd[-1] == "S":
            meet = '1'
        elif yymmdd[-1] == "J":
            meet = '2'
        if yymmdd[-1] == "B":
            meet = '3'

        url_odds = 'https://race.kra.co.kr/raceScore/ScoretableBettingprofitBoth.do'

        params = {'Act': '04', 'Sub': '1', 'meet': meet,
                  'realRcDate': rcDate, 'realRcNo': rcNo}

        # perfetica_odds = HttpMethods()
        perfetica_drt = ''
        try:
            soup = self.odds_page.post_url(url_odds, params)
            tables = soup.find_all('table')
            ca = tables[0].find('caption')
            findcaption = ca.get_text().strip()
            odds_msg += ' >>> ' + findcaption
        except AttributeError as e:
            odds_msg += " >>> 페이지 결과 없음"
            page_error = True
            return page_error, perfetica_drt

        header1 = []
        header2 = []
        header3 = []

        # bedang = dict()
        trs = tables[0].find_all('tr')
        for tr_i, tr in enumerate(trs):
            res = []
            if tr_i == 0:
                ths = tr.find_all('th')
                for th in ths:
                    header1.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))
                # print(header1)

            elif tr_i == 1:
                ths = tr.find_all('th')
                for th in ths:
                    header2.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))
                # print(header2)

            elif tr_i == 2:
                ths = tr.find_all('th')
                for th in ths:
                    header3.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))
                # print(header3)
            elif tr_i > 2 and tr_i < num_ho + 3:
                td_b = tr.find_all('td')

                start_lane_row = td_b[header3.index('출전번호')].get_text().strip()

                if start_lane_row == '자료가 없습니다.':
                    perfetica_drt = '자료가 없습니다.'
                    break
                else:

                    for i in range(num_ho):
                        perfetica = td_b[header3.index(str(i + 1))].get_text().strip()
                        if perfetica == '-' or perfetica == '----':
                            pass
                        else:
                            perfetica_drt += "(" + str(i + 1) + "," + start_lane_row + "):" + perfetica + "/"
            else:
                break
        perfetica_drt = perfetica_drt[0:-1]
        # print("쌍승식 배당 : ", perfetica_drt)
        return page_error, perfetica_drt

    def odds_qunellaplace_page_read(self, yymmdd, num_ho):  # 결과정보 결과정보는 아래

        odds_msg = '배당페이지 읽기 '
        page_error = False

        rcDate = yymmdd[0:8]
        rcNo = yymmdd[9:11]

        if yymmdd[-1] == "S":
            meet = '1'
        elif yymmdd[-1] == "J":
            meet = '2'
        if yymmdd[-1] == "B":
            meet = '3'

        url_odds = 'https://race.kra.co.kr/raceScore/ScoretableBettingprofitBc.do'

        params = {'Act': '04', 'Sub': '1', 'meet': meet,
                  'realRcDate': rcDate, 'realRcNo': rcNo}

        qunellaplace_drt = ''
        # qunellaplace_odds = HttpMethods()
        try:
            soup = self.odds_page.post_url(url_odds, params)
            tables = soup.find_all('table')
            ca = tables[0].find('caption')
            findcaption = ca.get_text().strip()
            odds_msg += ' >>> ' + findcaption
        except AttributeError as e:
            odds_msg += " >>> 페이지 결과 없음"
            page_error = True
            return page_error, qunellaplace_drt

        header1 = []
        header2 = []
        header3 = []

        # bedang = dict()
        trs = tables[0].find_all('tr')
        for tr_i, tr in enumerate(trs):
            res = []
            if tr_i == 0:
                ths = tr.find_all('th')
                for th in ths:
                    header1.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))
                # print(header1)

            elif tr_i == 1:
                ths = tr.find_all('th')
                for th in ths:
                    header2.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))
                # print(header2)

            elif tr_i == 2:
                ths = tr.find_all('th')
                for th in ths:
                    header3.append(
                        th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                           '').replace(
                            '\xa0',
                            ''))
                # print(header3)
            elif tr_i > 2 and tr_i < num_ho + 3:
                td_b = tr.find_all('td')
                for td_i, td in enumerate(td_b):
                    if td_i == 0:
                        start_lane_row = td.get_text().strip()
                    elif td_i > 0 and td_i <= num_ho:
                        qunellaplace = td.get_text().strip()
                        if qunellaplace == '-' or qunellaplace == '----':
                            pass
                        else:
                            qunellaplace_drt += "(" + start_lane_row + "," + str(td_i) + "):" + qunellaplace + "/"
            else:
                break
        # print("복연승식 배당 : ",qunellaplace_drt)
        qunellaplace_drt = qunellaplace_drt[0:-1]
        if qunellaplace_drt == '':
            qunellaplace_drt = '자료가 없습니다'
        return page_error, qunellaplace_drt

    def odds_trebles_page_read(self, yymmdd, num_ho):  # 결과정보 결과정보는 아래

        odds_msg = '배당페이지 읽기 '
        page_error = False

        rcDate = yymmdd[0:8]
        rcNo = yymmdd[9:11]

        if yymmdd[-1] == "S":
            meet = '1'
        elif yymmdd[-1] == "J":
            meet = '2'
        if yymmdd[-1] == "B":
            meet = '3'

        url_odds = 'https://race.kra.co.kr/raceScore/ScoretableBettingprofit3Bc.do'
        trebles_drt = ''
        for num_start in range(num_ho):
            params = {'Act': '04', 'Sub': '1', 'meet': meet, 'chulNo1': str(num_start + 1),
                      'realRcDate': rcDate, 'realRcNo': rcNo}

            try:
                soup = self.odds_page.post_url(url_odds, params)
                tables = soup.find_all('table')
                ca = tables[0].find('caption')
                # findcaption = ca.get_text().strip()
                # odds_msg += ' >>> ' + findcaption
            except AttributeError as e:
                odds_msg += " >>> 페이지 결과 없음"
                page_error = True
                return page_error, trebles_drt

            header1 = []
            header2 = []
            header3 = []

            trs = tables[0].find_all('tr')

            for tr_i, tr in enumerate(trs):
                res = []
                if tr_i == 0:
                    ths = tr.find_all('th')
                    for th in ths:
                        header1.append(
                            th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                               '').replace(
                                '\xa0',
                                ''))
                    # print(header1)

                elif tr_i == 1:
                    ths = tr.find_all('th')
                    for th in ths:
                        header2.append(
                            th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                               '').replace(
                                '\xa0',
                                ''))
                    # print(header2, str(num_start + 1), '번 ' )

                elif tr_i == 2:
                    ths = tr.find_all('th')
                    for th in ths:
                        header3.append(
                            th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                               '').replace(
                                '\xa0',
                                ''))
                    # print(header3)
                elif tr_i > 2 and tr_i < num_ho + 3:
                    td_b = tr.find_all('td')
                    for td_i, td in enumerate(td_b):
                        if td_i == 0:
                            start_lane_row = td.get_text().strip()
                        elif td_i > 0 and td_i <= num_ho:
                            trebles = td.get_text().strip()
                            if trebles == '-' or trebles == '----':
                                pass
                            else:
                                trebles = td.get_text().strip()
                                trebles_drt += "(" + str(num_start + 1) + "," + start_lane_row + "," + str(
                                    td_i) + "):" + trebles + "/"
                else:
                    exit_loop = True
                    break
            if exit_loop:
                # print("삼복승식 배당 : ", trebles_drt)
                continue
        trebles_drt = trebles_drt[0:-1]
        if trebles_drt == '':
            trebles_drt = '자료가 없습니다'
        return page_error, trebles_drt

    def odds_trifetica_page_read(self, yymmdd, num_ho):  # 결과정보 결과정보는 아래

        odds_msg = '배당페이지 읽기 '
        page_error = False

        rcDate = yymmdd[0:8]
        rcNo = yymmdd[9:11]

        if yymmdd[-1] == "S":
            meet = '1'
        elif yymmdd[-1] == "J":
            meet = '2'
        if yymmdd[-1] == "B":
            meet = '3'

        url_odds = 'https://race.kra.co.kr/raceScore/ScoretableBettingprofit3Both.do'
        # bedang = dict()
        trifetica_drt = ""
        # trifetica_odds = HttpMethods()
        for num_start in range(num_ho):
            params = {'Act': '04', 'Sub': '1', 'meet': meet, 'chulNo1': str(num_start + 1),
                      'realRcDate': rcDate, 'realRcNo': rcNo}

            soup = self.odds_page.post_url(url_odds, params)

            try:
                tables = soup.find_all('table')
                ca = tables[0].find('caption')
                findcaption = ca.get_text().strip()
                # odds_msg += ' >>> ' + findcaption
            except AttributeError as e:
                odds_msg += " >>> 페이지 결과 없음"
                page_error = True
                return page_error, trifetica_drt

            header1 = []
            header2 = []
            header3 = []

            trs = tables[0].find_all('tr')
            for tr_i, tr in enumerate(trs):
                res = []
                if tr_i == 0:
                    ths = tr.find_all('th')
                    for th in ths:
                        header1.append(
                            th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                               '').replace(
                                '\xa0',
                                ''))
                    # print(header1)

                elif tr_i == 1:
                    ths = tr.find_all('th')
                    for th in ths:
                        header2.append(
                            th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                               '').replace(
                                '\xa0',
                                ''))
                    # print(header2, str(num_start + 1), '번 ' )

                elif tr_i == 2:
                    ths = tr.find_all('th')
                    for th in ths:
                        header3.append(
                            th.get_text().strip().replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
                                                                                                               '').replace(
                                '\xa0',
                                ''))
                    # print(header3)
                elif tr_i > 2 and tr_i < num_ho + 3:
                    td_b = tr.find_all('td')
                    for td_i, td in enumerate(td_b):

                        if td_i == 0:
                            start_lane_row = td.get_text().strip()
                        elif td_i > 0 and td_i <= num_ho:
                            trifetica = td.get_text().strip()
                            if trifetica == '-' or trifetica == '----':
                                pass
                            else:
                                trifetica = td.get_text().strip()
                                trifetica_drt += "(" + str(num_start + 1) + "," + str(
                                    td_i) + "," + start_lane_row + "):" + trifetica + "/"

                else:
                    break
            # print("삼쌍승식 배당 : ", trifetica_drt)
        trifetica_drt = trifetica_drt[0:-1]
        if trifetica_drt == '':
            trifetica_drt = '자료가 없습니다'
        return page_error, trifetica_drt

    def odds_page_read(self, yymmdd, num_ho):

        bedang_dict = OrderedDict()
        bedang_dict[(yymmdd, num_ho)] = Bedang()
        bedang_dict[(yymmdd, num_ho)].col['YYMMDD_L_NO'] = yymmdd
        bedang_dict[(yymmdd, num_ho)].col['NUM_HO'] = num_ho
        win_error, win_odds, race_rank = self.odds_win_page_read(yymmdd, num_ho)
        bedang_dict[(yymmdd, num_ho)].col['RACE_RANK'] = race_rank
        bedang_dict[(yymmdd, num_ho)].col['RST_WIN_DRT'] = win_odds
        show_error, show_odds = self.odds_show_page_read(yymmdd, num_ho)
        bedang_dict[(yymmdd, num_ho)].col['RST_SHOW_DRT'] = show_odds
        quinella_error, quinella_odds = self.odds_quinella_page_read(yymmdd, num_ho)
        bedang_dict[(yymmdd, num_ho)].col['RST_QUINELLA_DRT'] = quinella_odds
        perfetica_error, perfetica_odds = self.odds_perfetica_page_read(yymmdd, num_ho)
        bedang_dict[(yymmdd, num_ho)].col['RST_PERFECTA_DRT'] = perfetica_odds
        qu_place_error, qu_place_odds = self.odds_qunellaplace_page_read(yymmdd, num_ho)
        bedang_dict[(yymmdd, num_ho)].col['RST_QUINELLA_PLACE_DRT'] = qu_place_odds
        trebles_error, trebles_odds = self.odds_trebles_page_read(yymmdd, num_ho)
        bedang_dict[(yymmdd, num_ho)].col['RST_QUINELLA_TREBLES_DRT'] = trebles_odds
        trefetica_error, trefetica_odds = self.odds_trifetica_page_read(yymmdd, num_ho)
        bedang_dict[(yymmdd, num_ho)].col['RST_TRIFECTA_DRT'] = trefetica_odds
        bedang_dict[(yymmdd, num_ho)].col['STAGE'] = 'RESULT'
        return bedang_dict

    def __del__(self):
        return
