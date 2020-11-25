import pandas as pd
import sys
from collections import OrderedDict
import hogetpage.get_page as hgp
import hogetdb.execute_query as hoeq
import hodbsql.sel_sql as hos


def get_horse_info(test_mode, meet, tbl_type):
    info_msg = 'get_ho_info_data '

    s_mode = 'name'

    if s_mode == 'rank':
        if meet == '1':
            horse_to_insert = ['1등급', '2등급', '3등급', '4등급', '국5', '국6', '미검']
        elif meet == '2':
            horse_to_insert = ['한1', '한2', '한3', '한4', '제1', '제2', '제3', '제4', '제5', '제6', '미', '외부']
        elif meet == '3':
            horse_to_insert = ['1등급', '2등급', '3등급', '4등급', '국5', '국6', '미검']
        else:
            print('작업할 base_meet이 없음')
        print(horse_to_insert)
    elif s_mode == 'name':
        rh_nm_sql = ['가', '힣']
        horse_to_insert = []
        check_horse = hoeq.SelectResult()

        horse_sql = hos.horse_list(meet, tbl_type, rh_nm_sql)

        horse_yn, horse_in_detail = check_horse.select_sql(horse_sql, 'tuple')

        n = 0
        for v, i in horse_in_detail:
            n += 1
            sys.stdout.write(
                '\r' + 'profile_horse [' + v + ']...' + str(n) + '/' + str(
                    len(horse_in_detail)) + '[' + str(100 * n // len(horse_in_detail)) + '%]')
            sys.stdout.flush()
            check_sql = hos.check_horse(v, i, tbl_type)
            check_error, check_db_and_db = check_horse.select_sql(check_sql, 'tuple')

            if len(check_db_and_db) > 0:
                continue
            else:
                print(v, i)
                horse_to_insert.append((v, i))

        if len(horse_to_insert) < 1:
            info_msg += info_msg
            return info_msg
        else:
            info_msg += " >>> Insert 할 데이터가 " + str(len(horse_to_insert)) + " 개 존재."
        info_msg = '이름으로 작업 : ' + info_msg

    ho_infoorder = OrderedDict()
    for ho_info_i, ho_info_v in enumerate(horse_to_insert):
        if s_mode == 'rank':
            ho_info_p = ho_info_v
        else:
            ho_info_p = ho_info_v[0]

        get_horse = hgp.HorseInfor()
        ho_error, ho_msg, ho_info = get_horse.horse_info(meet, ho_info_v, tbl_type, s_mode)

        if ho_error:
            # print('not exist insert info : to_insert_error', ho_error)
            continue
        else:
            sys.stdout.write(
                '\r' + 'profile_horse [' + ho_info_p + ']...' + str(ho_info_i + 1) + '/' + str(
                    len(horse_to_insert)) + '[' + str(100 * ho_info_i // len(horse_to_insert)) + '%]')
            sys.stdout.flush()
            ho_infoorder.update(ho_info)

    if test_mode:
        info_msg = 'Test Mode : ' + info_msg
        return info_msg
    else:
        pass
    # print(ho_infoorder)
    insert_infor = hoeq.InsertResult()
    hoinfodb_error = insert_infor.insert_hoinfodb(ho_infoorder, tbl_type)
    if not hoinfodb_error:
        info_msg = info_msg + ' >>> DB 작업 완료'
    else:
        info_msg = info_msg + ' >>> DB 작업 에러'

    return info_msg


def simsa_work(test_mode, meet, tbl_type, span):
    simsa_msg = 'get_simsa_data '

    if meet == '1':
        meet_nm = '서울'
    elif meet == '2':
        meet_nm = '제주'
    elif meet == '3':
        meet_nm = '부산'
    else:
        print('작업할 base_meet이 없음')

    print('\n' + '-' * 100)
    print('[' + meet_nm + '][' + simsa_msg + ']')
    print('-' * 100)

    if span['span_mode']:

        if meet == '1':
            week_int = [0] * 3
            week_int[0] = 'W-WED'
            week_int[1] = 'W-THU'
            week_int[2] = 'W-FRI'
        elif meet == '2':
            week_int = [0] * 3
            week_int[0] = 'W-WED'
            week_int[1] = 'W-THU'
            week_int[2] = 'W-FRI'
        elif meet == '3':
            week_int = [0] * 3
            week_int[0] = 'W-WED'
            week_int[1] = 'W-THU'
            week_int[2] = 'W-SAT'
        else:
            simsa_msg = 'rcDate_listv error'
        rcDate_list = []
        for week_int_i, week_int_v in enumerate(week_int):
            dt_index = pd.date_range(start=span['span_from_to'][0], end=span['span_from_to'][1], freq=week_int_v)
            dt_list = dt_index.strftime("%Y%m%d").tolist()
            for v in dt_list:
                rcDate_list.append(v)

        rcDate_list.sort()
        links_simsa_params = []
        check_recode = hoeq.SelectResult()
        for rcdate_list_i, rcdate_list_v in enumerate(rcDate_list):
            for trno_i in range(1, 11):
                check_sql = hos.check_simsa(rcdate_list_v, str(trno_i), meet, tbl_type)
                check_yn, check_db = check_recode.select_sql(check_sql, 'tuple')
                if check_yn or len(check_db) > 0:
                    continue
                else:
                    sys.stdout.write(
                        '\r' + str(trno_i) + ' / ' + str(rcdate_list_v) + ' 날짜 읽기 : ')
                    sys.stdout.flush()
                    # print(str(trno_i), ' / ', rcdate_list_v)
                    params = str(trno_i) + '/' + rcdate_list_v
                    links_simsa_params.append(params)
    else:
        links_simsa_params = []
        simsa_link = hgp.SimaLink()
        for page_num in range(span['page_from_to'][0], span['page_from_to'][1]):
            params = simsa_link.simsa_get_link(page_num=page_num, meet=meet, tbl_type=tbl_type)
            links_simsa_params.extend(params)

    if test_mode:
        simsa_msg = 'Test_mode : ' + str(len(links_simsa_params))
        print(links_simsa_params)
        return simsa_msg
    else:
        pass

    ho_simsainfo = OrderedDict()
    ho_simsdetail = OrderedDict()
    if len(links_simsa_params) > 0:
        simsa_page = hgp.SimsaPage()
        for params_i, params_v in enumerate(links_simsa_params):
            sys.stdout.write(
                '\r' + str(params_i + 1) + '/' + str(len(links_simsa_params)) + ' 심사 페이지 읽기 : ' + params_v)
            sys.stdout.flush()
            trno = params_v.split('/')[0]
            rcdate = params_v.split('/')[1]

            if meet == '1':
                yymmdd_l_no = str(int(rcdate)) + str(100 + int(trno)) + 'S'
            elif meet == '2':
                yymmdd_l_no = str(int(rcdate)) + str(100 + int(trno)) + 'J'
            elif meet == '3':
                yymmdd_l_no = str(int(rcdate)) + str(100 + int(trno)) + 'B'
            else:
                print("뭥미")

            simsa_err, simsa_msg, simsainfo, simsdetail = simsa_page.simsa_get_page(trNo=trno, rcDate=rcdate, meet=meet,
                                                                                    yymmdd_l_no=yymmdd_l_no)
            if simsa_err:
                continue
            else:
                ho_simsainfo.update(simsainfo)
                ho_simsdetail.update(simsdetail)

        simsa_msg = simsa_msg + ' 기본정보 : [' + str(len(ho_simsainfo)) + '], 상세정보 : [' + str(len(ho_simsdetail)) + ']'
        if len(ho_simsainfo) > 0 and len(ho_simsdetail) > 0:
            db_error = False
            if meet == '1':
                tblnm = tbl_type + '_tblsehorang'
                tblnmsub = tbl_type + '_tblsehorangsub'

            elif meet == '2':
                tblnm = tbl_type + '_tbljehorang'
                tblnmsub = tbl_type + '_tbljehorangsub'

            elif meet == '3':
                tblnm = tbl_type + '_tblbshorang'
                tblnmsub = tbl_type + '_tblbshorangsub'
            else:
                print(meet + '에 해당 테이블이 존재하지 않습니다.')
                db_error = True

            if not db_error:
                simsa_msg += ' > 심사 페이지 잘 읽음 > '
                insert_simsa_basic = hoeq.InsertResult()
                insert_base_simsa_error = insert_simsa_basic.insert_simsa_base(ho_simsainfo, tblnm)
                if not insert_base_simsa_error:
                    insert_simsa_detail = hoeq.InsertResult()
                    simsa_msg = simsa_msg + ' >>> 심사 기본정보 Inserted...'
                    insert_detail_simsa_error = insert_simsa_detail.insert_simsa_detail(ho_simsdetail, tblnmsub)
                    if not insert_detail_simsa_error:
                        simsa_msg = simsa_msg + ' >>> 심사 상세정보 Inserted...'
                    else:
                        delete_simsa = hoeq.DeleteResult()
                        base_simsa_del_error = delete_simsa.delete_exist_data(ho_simsainfo, tblnm)
                        if base_simsa_del_error:
                            simsa_msg = simsa_msg + ' >>> 심사 상세정보 error >>>  심사 기본정보 삭제 error... '
                        else:
                            simsa_msg = simsa_msg + ' >>> 심사 상세정보 error >>>  심사 기본정보 삭제 완료... '
                else:
                    simsa_msg = simsa_msg + ' >>> 심사 기본정보 error...'
        else:
            simsa_msg = simsa_msg + '심사페이지 읽기 오류'
    else:
        simsa_msg = simsa_msg + '입력할 심사 정보 없음'
    return simsa_msg


def chul_work(test_mode, meet_v, tbl_type, span):
    ################################################################
    # step 1
    # 저장된 상세결과 링크페이지에서 각 항목들 DB에 저장하기
    ################################################################
    chul_msg = 'get_chulma_data '

    if meet_v == '1':
        meet_nm = '서울'
        tblnm = tbl_type + '_tblsehorang'
        tblnmsub = tbl_type + '_tblsehorangsub'
    elif meet_v == '2':
        meet_nm = '제주'
        tblnm = tbl_type + '_tbljehorang'
        tblnmsub = tbl_type + '_tbljehorangsub'
    elif meet_v == '3':
        meet_nm = '부산'
        tblnm = tbl_type + '_tblbshorang'
        tblnmsub = tbl_type + '_tblbshorangsub'
    else:
        chul_msg = chul_msg + ' >>> 작업할 base_meet이 없음'
        return chul_msg

    print('\n' + '-' * 100)
    print('[' + meet_nm + '][' + chul_msg + ']')
    print('-' * 100)

    if span['span_mode']:
        rcDate_list = []

        if meet_v == '1':
            week_int = [0] * 3
            week_int[0] = 'W-FRI'
            week_int[1] = 'W-SAT'
            week_int[2] = 'W-SUN'
            # week_int[2] = 'W-FRI'

        elif meet_v == '2':
            week_int = [0] * 3
            week_int[0] = 'W-FRI'
            week_int[1] = 'W-SAT'
            week_int[2] = 'W-SUN'
            # week_int[0] = 'W-SUN'
        elif meet_v == '3':
            week_int = [0] * 3
            week_int[0] = 'W-FRI'
            week_int[1] = 'W-SAT'
            week_int[2] = 'W-SUN'
            # week_int[2] = 'W-SAT'

        for week_int_i, week_int_v in enumerate(week_int):
            dt_index = pd.date_range(start=span['span_from_to'][0], end=span['span_from_to'][1], freq=week_int_v)
            dt_list = dt_index.strftime("%Y%m%d").tolist()
            for v in dt_list:
                rcDate_list.append(v)
        rcDate_list.sort()
        links_chul_params = []

        check_recode = hoeq.SelectResult()
        for rcdate_list_i, rcdate_list_v in enumerate(rcDate_list):
            for trno_i in range(1, 17):
                params = str(trno_i) + '/' + rcdate_list_v
                check_sql = hos.check_chul_list(rcdate_list_v, trno_i, meet_v, tbl_type)
                check_yn, check_db = check_recode.select_sql(check_sql, 'tuple')
                sys.stdout.write(
                    '\r' + str(rcdate_list_v) + '-' + str(trno_i) + ' 작업할 목록 체크 : ' + str(
                        (rcdate_list_i * 17) + trno_i) + '/' + str(len(rcDate_list) * 17) + '[' + str(
                        100 * ((rcdate_list_i * 17) + trno_i) // (len(rcDate_list) * 17)) + '%]')
                sys.stdout.flush()
                if check_yn or len(check_db) > 0:
                    break
                else:
                    links_chul_params.append(params)
        chul_msg = ' >>> 기간모드 ON'
    else:
        chul_list = hgp.ChulLink()
        chul_err, chul_msg, links_chul_params = chul_list.chul_get_link(meet=meet_v, tbl_type=tbl_type)
        chul_msg = ' >>> 기간모드 OFF'
    chul_msg += chul_msg
    ################################################################
    # step 2
    # 저장된 상세결과 링크페이지에서 각 항목들 DB에 저장하기
    ################################################################
    if test_mode:
        print(links_chul_params)
        chul_msg = ' >>> 테스트 모드'
        return chul_msg
    else:
        pass

    if len(links_chul_params) > 0:
        ho_chulinfo = OrderedDict()
        ho_chuldetail = OrderedDict()
        chul_page = hgp.ChulPage()
        for links_arranged_i, links_arranged_v in enumerate(links_chul_params):
            sys.stdout.write(
                '\r' + str(links_arranged_i + 1) + '/' + str(len(links_chul_params)) + ' 링크 페이지 작업 : ')
            sys.stdout.flush()
            chul_err, chul_msg, chul_day, chul_detail = chul_page.chul_get_page(meet=meet_v, span=span['span_mode'],
                                                                                links_arranged_v=links_arranged_v)

            if chul_err:
                print('error : ', chul_msg, links_arranged_v)
                continue
            else:
                ho_chulinfo.update(chul_day)
                ho_chuldetail.update(chul_detail)

        if len(ho_chulinfo) > 0 and len(ho_chuldetail) > 0:
            print(' > 기본정보 : [' + str(len(ho_chulinfo)) + '], 상세정보 : [' + str(len(ho_chuldetail)) + ']')

            chul_msg = chul_msg + ' >>> chul pasge 잘 읽음 > '

            insert_chulma_base = hoeq.InsertResult()
            insert_chul_base_error = insert_chulma_base.insert_chul_base(ho_chulinfo, tblnm)
            if not insert_chul_base_error:
                chul_msg = chul_msg + ' >>> chul 기본정보 Inserted...'
                insert_chulma_detail = hoeq.InsertResult()
                insert_chul_detail_error = insert_chulma_detail.insert_chul_detail(ho_chuldetail, tblnmsub)
                if not insert_chul_detail_error:
                    chul_msg = chul_msg + ' >>> chul 상세정보 Inserted...'
                else:
                    delete_chulma = hoeq.DeleteResult()
                    chulma_base_del_error = delete_chulma.delete_exist_data(ho_chulinfo, tblnm)
                    if chulma_base_del_error:
                        chul_msg = chul_msg + ' >>> 상세정보 error >>>  기본정보 삭제 error... '
                    else:
                        chul_msg = chul_msg + ' >>> 상세정보 error >>>  기본정보 삭제 완료... '
            else:
                chul_msg = chul_msg + ' >>> chul 기본정보 Inserted error... >'
        else:
            chul_msg = " >>> 페이지 읽기 에러...."
    else:
        chul_msg = chul_msg + " 입력할 정보 없음"
    return chul_msg


def rst_work(test_mode, meet_v, tbl_type, span):
    ################################################################
    # step 1
    # 저장된 상세결과 링크페이지에서 각 항목들 DB에 저장하기
    ################################################################
    rst_msg = 'get_result '
    links_list_arranged = []
    update_list_arranged = []

    if span['span_mode']:
        rst_msg = rst_msg + ' >>> 기간모드 ON'
    else:
        rst_msg = rst_msg + ' >>> 기간모드 OFF'

    if meet_v == '1':
        meet_nm = '서울'
        tblnm = tbl_type + '_tblsehorang'
        tblnmsub = tbl_type + '_tblsehorangsub'
    elif meet_v == '2':
        meet_nm = '제주'
        tblnm = tbl_type + '_tbljehorang'
        tblnmsub = tbl_type + '_tbljehorangsub'
    elif meet_v == '3':
        meet_nm = '부산'
        tblnm = tbl_type + '_tblbshorang'
        tblnmsub = tbl_type + '_tblbshorangsub'
    else:
        rst_msg = rst_msg + ' >>> 작업할 meet이 없음'
        return rst_msg

    print('\n' + '-' * 100)
    print('[' + meet_nm + '][' + rst_msg + ']')
    print('-' * 100)

    # 작업(결과 페이지 스크래핑)할 리스트 획득
    rst_link = hgp.ResultLink()
    for page_num in range(span['page_from_to'][0], span['page_from_to'][1]):

        sys.stdout.write('\r' + str(page_num) + '/' + ' Getting Link Page... ')
        sys.stdout.flush()

        rst_err, rst_list, update_list = rst_link.rst_get_link(meet=meet_v, tbl_type=tbl_type, span=span,
                                                               page_num=page_num)

        if rst_err:
            break;
        else:
            links_list_arranged.extend(rst_list)
            update_list_arranged.extend(update_list)

    if test_mode:
        print('Link_list : ', links_list_arranged)
        print('Update_list : ', update_list_arranged)
        rst_msg = rst_msg + ' >>> 테스트 모드'
        return rst_msg
    else:
        print('Link_list : ', len(links_list_arranged))
        print('Update_list : ', len(update_list_arranged))
        pass
    ################################################################
    # step 2
    # 저장된 상세결과 링크페이지에서 각 항목들 DB에 저장하기
    ################################################################
    print(update_list_arranged)
    if len(update_list_arranged) > 0:

        # 결과 페이지 스크래핑 class 인스턴스
        rst_page = hgp.ResultPage()

        data_update_day = OrderedDict()
        data_update_detail = OrderedDict()

        for update_list_arranged_i, update_list_arranged_v in enumerate(update_list_arranged):

            sys.stdout.write(
                '\r' + str(update_list_arranged_i + 1) + '/' + str(
                    len(update_list_arranged)) + ' 링크 페이지 작업 : ' + update_list_arranged_v[1] + update_list_arranged_v[
                    2])
            sys.stdout.flush()

            # 결과 페이지 스크래핑 시작
            rst_err, data_day, data_detail = rst_page.rst_update_page(meet=meet_v, rcDatercNo=update_list_arranged_v)

            if rst_err:
                print('error : ', rst_msg, update_list_arranged_v)
                return rst_msg
            else:
                data_update_day.update(data_day)
                data_update_detail.update(data_detail)

        if len(data_update_day) > 0 and len(data_update_detail) > 0:

            # 결과 입력 class 인스턴스
            update_result = hoeq.UpdateResult()
            print(' 기본정보 : [' + str(len(data_update_day)) + '], 상세정보 : [' + str(
                len(data_update_detail)) + ']')

            # 결과 입력
            # print(data_update_day)
            daydb_error = update_result.update_daydb(data_update_day, tblnm)

            if not daydb_error:
                rst_msg = rst_msg + ' >>> 기본정보 업데이트'
                detaildb_error = update_result.update_detaildb(data_update_detail, tblnmsub)
                if not detaildb_error:
                    rst_msg = rst_msg + ' >>> 상세정보 업데이트'
                else:
                    rst_update_err = update_result.update_rst_daydb(data_update_day, tblnm)
                    if rst_update_err:
                        rst_msg = rst_msg + ' >>> 상세정보 에러'
                    else:
                        rst_msg = rst_msg + ' >>> 상세정보 에러 Culma 로 변경'
            else:
                rst_msg = rst_msg + ' >>> 기본정보 업데이트 에러'
        else:
            print(len(data_update_day), len(data_update_detail))
            rst_msg = rst_msg + ' >>> 결과 페이지 읽기 완료'
    else:
        rst_msg = rst_msg + ' 입력할 정보 없음'
        print(len(links_list_arranged), len(update_list_arranged))

    return rst_msg


def bedang_work(test_mode, meet_v, fromdate, todate, tbl_type):
    ################################################################
    # step 1
    # 저장된 상세결과 링크페이지에서 각 항목들 DB에 저장하기
    ################################################################
    bedang_msg = 'get_bedang'

    if meet_v == '1':
        meet_nm = '서울'
        tblnm = tbl_type + '_tblsebedang'

    elif meet_v == '2':
        meet_nm = '제주'
        tblnm = tbl_type + '_tbljebedang'

    elif meet_v == '3':
        meet_nm = '부산'
        tblnm = tbl_type + '_tblbsbedang'

    else:
        bedang_msg = bedang_msg + ' >>> 작업할 meet이 없음'
        return bedang_msg

    print('\n' + '-' * 100)
    print('[' + meet_nm + '][' + bedang_msg + ']')
    print('-' * 100)

    check_recode = hoeq.SelectResult()
    check_sql = hos.check_bedang(meet_v, fromdate, todate, tbl_type)
    check_yn, check_db = check_recode.select_sql(check_sql, 'dataframe')

    if len(check_db) > 0:
        bedang_msg = bedang_msg + ' >>> 작업 시작'

    else:
        return bedang_msg

    if test_mode:
        print('bedang_page_list : ', check_db)
        bedang_msg = bedang_msg + ' >>> 테스트 모드'
        return bedang_msg
    else:
        print('FromDate : ', fromdate, 'ToDate : ', todate, '[', len(check_db), ']')
        pass

    ################################################################
    # step 2
    # 저장된 상세결과 링크페이지에서 각 항목들 DB에 저장하기
    ################################################################
    bedang_page = hgp.AboutOddsInPage()
    bedang_insert = OrderedDict()

    for index, row in check_db.iterrows():
        sys.stdout.write(
            '\r' + str(index + 1) + '/' + str(
                len(check_db)) + ' 링크 페이지 작업 : ' + row['YYMMDD_L_NO'])
        sys.stdout.flush()

        bedang_odd = bedang_page.odds_page_read(row['YYMMDD_L_NO'], row['NUM_HO'])
        bedang_insert.update(bedang_odd)

    if len(bedang_insert) > 0:
        insert_bedang = hoeq.InsertResult()
        print('배당 정보 : [' + str(len(bedang_insert)) + ']')

        daydb_error = insert_bedang.insert_bedang(bedang_insert, tblnm)
        if not daydb_error:
            bedang_msg = bedang_msg + ' >>> 배당 정보 Insert'

        else:
            bedang_msg = bedang_msg + ' >>> 배당 정보 Insert Error'
    else:
        bedang_msg = bedang_msg + ' >>> 입력 없음'

    return bedang_msg
