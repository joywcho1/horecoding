def check_info_link(exist_link, meet, tbl_type):
    if meet == '1':
        rsttbl = tbl_type + '_tblsehorang'
    elif meet == '2':
        rsttbl = tbl_type + '_tbljehorang'
    elif meet == '3':
        rsttbl = tbl_type + '_tblbshorang'
    else:
        print('뭐야')

    check_sql = "select * from " + rsttbl + " where YYMMDD_L_NO = '" + exist_link + "'"
    # print(check_sql)
    return check_sql


def check_chul_list(rcDate, rcNo, meet, tbl_type):
    if meet == '1':
        rsttbl = tbl_type + '_tblsehorangsub'
        yymmdd_l_no = str(int(rcDate)) + str(100 + int(rcNo)) + 'S'
    elif meet == '2':
        rsttbl = tbl_type + '_tbljehorangsub'
        yymmdd_l_no = str(int(rcDate)) + str(100 + int(rcNo)) + 'J'
    elif meet == '3':
        rsttbl = tbl_type + '_tblbshorangsub'
        yymmdd_l_no = str(int(rcDate)) + str(100 + int(rcNo)) + 'B'

    check_sql = "select * from " + rsttbl + " where YYMMDD_L_NO = '" + yymmdd_l_no + "'"

    return check_sql


def check_rst_link(yymmdd_l_no, meet, tbl_type):
    if meet == '1':
        chul_info = tbl_type + '_tblsehorang'
        chul_detail = tbl_type + '_tblsehorangsub'
    elif meet == '2':
        chul_info = tbl_type + '_tbljehorang'
        chul_detail = tbl_type + '_tbljehorangsub'
    elif meet == '3':
        chul_info = tbl_type + '_tblbshorang'
        chul_detail = tbl_type + '_tblbshorangsub'
    else:
        print('뭐야')

    check_sql = "SELECT  b.YYMMDD_L_NO FROM " + chul_info + " a right  OUTER  JOIN " + chul_detail
    check_sql += " b  ON   a.YYMMDD_L_NO = b.YYMMDD_L_NO and a.STAGE = b.STAGE   WHERE  a.YYMMDD_L_NO = '" + yymmdd_l_no + "' and a.STAGE = 'CHULMA'"
    check_sql += " and b.YYMMDD_L_NO = '" + yymmdd_l_no + "' and b.STAGE = 'CHULMA'"

    # print(check_sql)
    return check_sql


def check_update_link(yymmdd_l_no, meet, tbl_type):
    if meet == '1':
        chul_info = tbl_type + '_tblsehorang'
        chul_detail = tbl_type + '_tblsehorangsub'
    elif meet == '2':
        chul_info = tbl_type + '_tbljehorang'
        chul_detail = tbl_type + '_tbljehorangsub'
    elif meet == '3':
        chul_info = tbl_type + '_tblbshorang'
        chul_detail = tbl_type + '_tblbshorangsub'
    else:
        print('뭐야')

    check_sql = "SELECT  b.YYMMDD_L_NO FROM " + chul_info + " a right  OUTER  JOIN " + chul_detail
    check_sql += " b  ON   a.YYMMDD_L_NO = b.YYMMDD_L_NO and a.STAGE = b.STAGE   WHERE  a.YYMMDD_L_NO = '" + yymmdd_l_no + "' and a.STAGE = 'RESULT'"
    check_sql += " and b.YYMMDD_L_NO = '" + yymmdd_l_no + "' and b.STAGE = 'RESULT'"

    return check_sql


def check_simsa_link(exist_link_yn, meet, tbl_type):
    if meet == '1':
        rsttbl = tbl_type + '_tblsehorang'
    elif meet == '2':
        rsttbl = tbl_type + '_tbljehorang'
    elif meet == '3':
        rsttbl = tbl_type + '_tblbshorang'
    else:
        print('뭐야')

    check_sql = "select * from " + rsttbl + " where YYMMDD_L_NO like '%" + exist_link_yn + "%'"
    # print(check_sql)
    return check_sql


def check_bedang(meet, fromdate, todate, tbl_type):
    if meet == '1':
        rsttbl = tbl_type + '_tblsehorangsub'
        bedtbl = tbl_type + '_tblsebedang'

    elif meet == '2':
        rsttbl = tbl_type + '_tbljehorangsub'
        bedtbl = tbl_type + '_tbljebedang'

    elif meet == '3':
        rsttbl = tbl_type + '_tblbshorangsub'
        bedtbl = tbl_type + '_tblbsbedang'

    bedang_check = "select a.yymmdd_l_no as YYMMDD_L_NO, count(a.YYMMDD_L_NO) as NUM_HO  from " + rsttbl
    bedang_check += " a left outer join " + bedtbl
    bedang_check += " b on a.YYMMDD_L_NO = b.YYMMDD_L_NO and a.STAGE =b.STAGE where a.YYMMDD_L_NO between '" + fromdate + "'"
    bedang_check += " and '" + todate + "' and a.STAGE = 'RESULT'"
    bedang_check += " and b.STAGE is null group by a.YYMMDD_L_NO"
    # print(bedang_check)
    return bedang_check


def check_n_bedang(meet, tbl_type, check_db):
    if meet == '1':
        rsttbl = tbl_type + '_tblsehorangsub'
    elif meet == '2':
        rsttbl = tbl_type + '_tbljehorangsub'
    elif meet == '3':
        rsttbl = tbl_type + '_tblbshorangsub'

    bedang_n_check = "select yymmdd_l_no, count(YYMMDD_L_NO) as NUM_HO from " + rsttbl
    bedang_n_check += " where YYMMDD_L_NO in ('"
    for check_db_v in check_db['YYMMDD_L_NO']:
        bedang_n_check += check_db_v + "',"

    bedang_n_check = bedang_n_check[0:-1] + ") groupby YYMMDD_L_NO"

    return bedang_n_check


def sel_rank(meet, rcDate, data, colnm, tbl_type):
    if meet == '1':
        rsttbl = tbl_type + '_tblsehorangsub'
    elif meet == '2':
        rsttbl = tbl_type + '_tbljehorangsub'
    elif meet == '3':
        rsttbl = tbl_type + '_tblbshorangsub'
    yymmdd = '20170101'

    rank_sql = "select " + colnm + ", SUBSTRING(YYMMDD_L_NO,1, 11) AS YYMMDD_L_NO , RST_RANK from " + rsttbl
    rank_sql += " where RST_TIME > 10 and RST_RANK < 30 and YYMMDD_L_NO > '" + yymmdd
    rank_sql += "' and YYMMDD_L_NO < '" + rcDate
    rank_sql += "' and " + colnm + " in ("

    for i in range(len(data)):
        rank_sql = rank_sql + "'" + data[i] + "',"
    rank_sql = rank_sql[0:-1] + ") order by " + colnm + ", YYMMDD_L_NO DESC"

    return rank_sql


def check_horse(horse_name, horse_num, tbl_type):
    hoinfo_sql = "select * from " + tbl_type + "_tblhoranginfo where "
    hoinfo_sql += "RH_NM = '" + horse_name + "' and RH_ID_NO = '" + horse_num + "'"
    return hoinfo_sql


def checkup_link(exist_link, meet, tbl_type):
    if meet == '1':
        rsttbl = tbl_type + '_tblsehorangsub'
    elif meet == '2':
        rsttbl = tbl_type + '_tbljehorangsub'
    elif meet == '3':
        rsttbl = tbl_type + '_tblbshorangsub'
    else:
        print('뭐야')
    check_sql = "select * from " + rsttbl + " where YYMMDD_L_NO = '" + exist_link + "'"
    # print(check_sql)
    return check_sql


def check_simsa(rcDate, rcNo, meet, tbl_type):
    if meet == '1':
        rsttbl = tbl_type + '_tblsehorang'
        exist_link = str(int(rcDate)) + str(100 + int(rcNo)) + 'S'
    elif meet == '2':
        rsttbl = tbl_type + '_tbljehorang'
        exist_link = str(int(rcDate)) + str(100 + int(rcNo)) + 'J'
    elif meet == '3':
        rsttbl = tbl_type + '_tblbshorang'
        exist_link = str(int(rcDate)) + str(100 + int(rcNo)) + 'B'
    else:
        print("뭥미")

    check_sql = "select * from " + rsttbl + " where STAGE = 'SIMSA' and YYMMDD_L_NO = '" + exist_link + "'"
    # print(check_sql)
    return check_sql


def horse_list(meet, tbl_type, rh_nm):
    if meet == '1':
        rsttbl = tbl_type + '_tblsehorangsub'
    elif meet == '2':
        rsttbl = tbl_type + '_tbljehorangsub'
    elif meet == '3':
        rsttbl = tbl_type + '_tblbshorangsub'
    else:
        print('뭐야')

    horse_list_sql = "select RH_NM, RH_ID_NO from " + rsttbl + " where RH_NM >= '" + rh_nm[0] + "' and RH_NM < '" + \
                     rh_nm[1] + "' group by RH_NM, RH_ID_NO"
    # print(check_sql)
    return horse_list_sql
