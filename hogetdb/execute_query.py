import sys
import pandas as pd
import hocmon.cmfindpage as hocm

class SelectResult:
    """MySql"""

    def __init__(self):
        self.get_connection= hocm.ConnectMySQL()
        # if self.get_connection.conn.open:
        #     print('[Select] db connection has begun to open')
        # else:
        #     print('[Select] db connection Error')

    def select_sql(self, sql, rst_type):

        try:
            db_error = False
            with self.get_connection.conn.cursor() as curs:
                curs.execute(sql)
                if rst_type == 'tuple':
                    rst_df = curs.fetchall()
                elif rst_type == 'dataframe':
                    rst_df = pd.read_sql_query(sql, self.get_connection.conn)
                else:
                    return True, None
            # print('[Select] ' + str(len(rst_df)) + ' rows selected...')
        except Exception as error:
            print('error execting select query "{}", error: {}'.format(sql, error))
            return True, None
        finally:
            curs.close()
            # self.get_connection.conn.close()
            # if self.get_connection.conn.open:
            #     print('[Select] db connection is opened')
            # else:
            #     print('[Select] db connection is finally closed')
        return db_error, rst_df

class InsertResult:
    """InsertResult"""
    def __init__(self):
        self.get_connection= hocm.ConnectMySQL()
        if self.get_connection.conn.open:
            print('[Insert] db connection has begun to open')
        else:
            print('[Insert] db connection Error')

    def insert_hoinfodb(self, ho_infoorder, tbl_type):

        try:
            db_error = False
            self.inserted = 0
            self.tblnmsub = tbl_type + '_tblhoranginfo'

            for i, (horse_name, horse_num) in enumerate(ho_infoorder):
                sys.stdout.write('\r' + str(i + 1) + '/' + str(len(ho_infoorder)) + ' 경주마 Insert ... ')
                sys.stdout.flush()
                place_holder =[]
                self.inserted +=1
                sql = ''
                sql = 'insert into '+ self.tblnmsub +' ('
                for column in ho_infoorder[(horse_name, horse_num)].col:
                    # 데이터 있는 경우만 추가
                    if ho_infoorder[(horse_name, horse_num)].col[column] != '':
                        sql = sql + column + ','
                sql = sql[0:-1]  # 마지막 ',' 제거
                sql = sql + ') values('

                for column in ho_infoorder[(horse_name, horse_num)].col:
                    # 데이터 있는 경우만 추가
                    if ho_infoorder[(horse_name, horse_num)].col[column] != '':
                        sql = sql + '%s,'
                        place_holder.append(ho_infoorder[(horse_name, horse_num)].col[column])
                sql = sql[0:-1] + ')'  # 마지막 ',' 제거
                # print(sql)
                # 등록 or 수정
                with self.get_connection.conn.cursor() as curs:
                    curs.execute(sql, place_holder)
            self.get_connection.conn.commit()
            print('\n[Insert] ' + self.tblnmsub + ' table ->' + str(self.inserted) + '] inserted...')

        except Exception as e:

            print('db(' + tbl_type + '_tblhoranginfo) error')
            print('error : "{}", place_holder : {}'.format(e, place_holder))
            print('sql = ' + sql)
            self.get_connection.conn.rollback()
            return True
        finally:
            curs.close()
            self.get_connection.conn.close()
            if self.get_connection.conn.open:
                print('[Insert] db connection is still opened')
            else:
                print('[Insert] db connection is finally closed')
        return db_error


    def insert_simsa_base(self, simsainfo, tbl_name):

        try:
            db_error = False
            self.inserted = 0

            for i, yymmdd_l_no in enumerate(simsainfo):
                self.inserted += 1
                sql = ''
                place_holder = []
                sql = 'insert into ' + tbl_name + ' ('
                for column in simsainfo[yymmdd_l_no].col:
                    if simsainfo[yymmdd_l_no].col[column] != '':
                        sql = sql + column + ','
                sql = sql[0:-1]  # 마지막 ',' 제거
                sql = sql + ') values('

                for column in simsainfo[yymmdd_l_no].col:
                    if simsainfo[yymmdd_l_no].col[column] != '':
                        sql = sql + '%s,'
                        place_holder.append(simsainfo[yymmdd_l_no].col[column])
                sql = sql[0:-1] + ')'  # 마지막 ',' 제거
                with self.get_connection.conn.cursor() as curs:
                    curs.execute(sql, place_holder)
            self.get_connection.conn.commit()
            print('\n[Insert] ' + tbl_name + ' table ->' + str(self.inserted) + '] inserted...')
        except Exception as e:
            print('db(' + tbl_name + ') error')
            print('error : "{}", place_holder : {}'.format(e, place_holder))
            print('sql = ' + sql)
            self.get_connection.conn.rollback()
            return True
        finally:

            curs.close()
            self.get_connection.conn.close()
            if self.get_connection.conn.open:
                print('[Insert] db connection is still opened')
            else:
                print('[Insert] db connection is finally closed')
        return db_error

    def insert_simsa_detail(self, simsa_detail, tbl_name):

        try:
            db_error = False
            self.inserted = 0
            for i, (yymmdd_l_no, horse_lane) in enumerate(simsa_detail):
                sys.stdout.write('\r' + str(i + 1) + '/' + str(len(simsa_detail)) + ' 주행심사 상세정보 Insert 작업')
                sys.stdout.flush()
                self.inserted += 1
                place_holder = []
                sql = ''
                sql = 'insert into '+ tbl_name +' ('
                for column in simsa_detail[(yymmdd_l_no, horse_lane)].col:
                    # 데이터 있는 경우만 추가
                    if simsa_detail[(yymmdd_l_no, horse_lane)].col[column] != '':
                        sql = sql + column + ','
                sql = sql[0:-1]  # 마지막 ',' 제거
                sql = sql + ') values('

                for column in simsa_detail[(yymmdd_l_no, horse_lane)].col:
                    # 데이터 있는 경우만 추가
                    if simsa_detail[(yymmdd_l_no, horse_lane)].col[column] != '':
                        sql = sql + '%s,'
                        place_holder.append(simsa_detail[(yymmdd_l_no, horse_lane)].col[column])
                sql = sql[0:-1] + ')'  # 마지막 ',' 제거

                with self.get_connection.conn.cursor() as curs:
                    curs.execute(sql, place_holder)
            self.get_connection.conn.commit()
            print('\n[Insert] ' + tbl_name + ' table ->' + str(self.inserted) + '] inserted...')
        except Exception as e:
            print('db(' + tbl_name + ') error')
            print('error : "{}", place_holder : {}'.format(e, place_holder))
            print('sql = ' + sql)
            self.get_connection.conn.rollback()
            return True
        finally:
            curs.close()
            self.get_connection.conn.close()
            if self.get_connection.conn.open:
                print('[Insert] db connection is still opened')
            else:
                print('[Insert] db connection is finally closed')
        return db_error

    def insert_chul_base(self, data_day, tbl_name):
        try:
            db_error = False
            self.inserted = 0
            for i, yymmdd_l_no in enumerate(data_day):
                sys.stdout.write('\r' + str(i + 1) + '/' + str(len(data_day)) + ' 출마표 info insert : ' + yymmdd_l_no)
                sys.stdout.flush()
                with self.get_connection.conn.cursor() as curs:

                    check_sql = "select * from " + tbl_name + " where YYMMDD_L_NO = %s"
                    curs.execute(check_sql, yymmdd_l_no)
                    self.result = curs.fetchall()

                    if len(self.result) > 0:
                        continue
                    else:
                        self.inserted += 1
                        sql = ''
                        place_holder = []
                        sql = 'insert into ' + tbl_name + ' ('
                        for column in data_day[yymmdd_l_no].col:
                            if data_day[yymmdd_l_no].col[column] != '':
                                sql = sql + column + ','
                        sql = sql[0:-1]  # 마지막 ',' 제거
                        sql = sql + ') values('

                        for column in data_day[yymmdd_l_no].col:
                            if data_day[yymmdd_l_no].col[column] != '':
                                sql = sql + '%s,'
                                place_holder.append(data_day[yymmdd_l_no].col[column])
                        sql = sql[0:-1] + ')'  # 마지막 ',' 제거
                        with self.get_connection.conn.cursor() as curs:
                            curs.execute(sql, place_holder)
            self.get_connection.conn.commit()
            print('\n[Insert] ' + tbl_name + ' table ->' + str(self.inserted) + '] inserted...')
        except Exception as e:
            print('db(' + tbl_name + ') error')
            print('error : "{}", place_holder : {}'.format(e, place_holder))
            print('sql = ' + sql)
            self.get_connection.conn.rollback()
            return True
        finally:
            curs.close()
            self.get_connection.conn.close()
            if self.get_connection.conn.open:
                print('[Insert] db connection is still opened')
            else:
                print('[Insert] db connection is finally closed')
        return db_error


    def insert_chul_detail(self, data_detail, tbl_name):

        try:
            db_error = False
            self.inserted = 0
            for i, (yymmdd_l_no, lane) in enumerate(data_detail):
                sys.stdout.write('\r' + str(i + 1) + '/' + str(len(data_detail)) + ' 출마표 테이블 Insert')
                sys.stdout.flush()
                place_holder = []
                self.inserted += 1
                sql = 'insert into ' + tbl_name + ' ('
                for column in data_detail[(yymmdd_l_no, lane)].col:
                    # 데이터 있는 경우만 추가
                    if data_detail[(yymmdd_l_no, lane)].col[column] != '':
                        sql = sql + column + ','
                sql = sql[0:-1]  # 마지막 ',' 제거
                sql = sql + ') values('

                for column in data_detail[(yymmdd_l_no, lane)].col:
                    # 데이터 있는 경우만 추가
                    if data_detail[(yymmdd_l_no, lane)].col[column] != '':
                        sql = sql + '%s,'
                        place_holder.append(data_detail[(yymmdd_l_no, lane)].col[column])
                sql = sql[0:-1] + ')'  # 마지막 ',' 제거

                # 등록 or 수정
                with self.get_connection.conn.cursor() as curs:
                    curs.execute(sql, place_holder)
            self.get_connection.conn.commit()
            print('\n[Insert] ' + tbl_name + ' table ->' + str(self.inserted) + '] inserted...')

        except Exception as e:
            print('db(' + tbl_name + ') error')
            print('error : "{}", place_holder : {}'.format(e, place_holder))
            print('sql = ' + sql)
            self.get_connection.conn.rollback()
            return True
        finally:
            curs.close()
            self.get_connection.conn.close()
            if self.get_connection.conn.open:
                print('[Insert] db connection is still opened')
            else:
                print('[Insert] db connection is finally closed')
        return db_error


    def insert_bedang(self, bedang_insert, tbl_name):
        try:
            db_error = False
            self.inserted = 0
            for i, (yymmdd_l_no, num_ho) in enumerate(bedang_insert):
                sys.stdout.write('\r' + str(i + 1) + '/' + str(len(bedang_insert)) + ' 배당 테이블 Insert')
                sys.stdout.flush()
                place_holder = []
                self.inserted += 1
                sql = 'insert into ' + tbl_name + ' ('
                for column in bedang_insert[(yymmdd_l_no, num_ho)].col:
                    # 데이터 있는 경우만 추가
                    if bedang_insert[(yymmdd_l_no, num_ho)].col[column] != '' or bedang_insert[(yymmdd_l_no, num_ho)].col[column] != 0:
                        sql = sql + column + ','
                sql = sql[0:-1]  # 마지막 ',' 제거
                sql = sql + ') values('

                for column in bedang_insert[(yymmdd_l_no, num_ho)].col:
                    # 데이터 있는 경우만 추가
                    if bedang_insert[(yymmdd_l_no, num_ho)].col[column] != '':
                        sql = sql + '%s,'
                        place_holder.append(bedang_insert[(yymmdd_l_no, num_ho)].col[column])
                sql = sql[0:-1] + ')'  # 마지막 ',' 제거

                # 등록 or 수정
                with self.get_connection.conn.cursor() as curs:
                    curs.execute(sql, place_holder)
            self.get_connection.conn.commit()
            print('\n[Insert] ' + tbl_name + ' table ->' + str(self.inserted) + '] inserted...')

        except Exception as e:
            print('db(' + tbl_name + ') error')
            print('error : "{}", place_holder : {}'.format(e, place_holder))
            print('sql = ' + sql)
            self.get_connection.conn.rollback()
            return True
        finally:
            curs.close()
            self.get_connection.conn.close()
            if self.get_connection.conn.open:
                print('[Insert] db connection is opened')
            else:
                print('[Insert] db connection is finally closed')
        return db_error


class UpdateResult:

    def __init__(self):
        self.get_connection= hocm.ConnectMySQL()
        if self.get_connection.conn.open:
            print('[Update] db connection has begun to open')
        else:
            print('[Update] db connection Error')

    def update_daydb(self, data_day, tbl_name):

        try:
            db_error = False
            self.updated = 0
            for i, yymmdd_l_no in enumerate(data_day):
                sys.stdout.write('\r' + str(i + 1) + '/' + str(len(data_day)) + ' 경마일자별 기본 Update 작업 ')
                sys.stdout.flush()
                self.updated += 1
                place_holder = []

                sql = "update " + tbl_name + " set "
                for column in data_day[yymmdd_l_no].col:
                    # 데이터 있는 경우만 추가
                    if data_day[yymmdd_l_no].col[column] != '':
                        sql = sql + column + '='
                        sql = sql + "%s,"
                        place_holder.append((data_day[yymmdd_l_no].col[column]))

                sql = sql[0:-1]  # 마지막 ',' 제거
                sql = sql + " where YYMMDD_L_NO = '" + yymmdd_l_no + "'"

                with self.get_connection.conn.cursor() as curs:
                    curs.execute(sql, place_holder)
            self.get_connection.conn.commit()
            print('\n[Update] ' + tbl_name + ' table ->' + str(self.updated) + '] updated...')
        except Exception as e:
            print(' db(' + tbl_name + ')  update error')
            print(' error : "{}", place_holder : {}'.format(e, place_holder))
            print(' sql = ' + sql)
            self.get_connection.conn.rollback()
            return True
        finally:
            curs.close()
            if self.get_connection.conn.open:
                print('[Update] db connection is still opened')
            else:
                print('[Update] db connection is finally closed')
        return db_error

    def update_detaildb(self, data_detail, tbl_name):
        try:
            db_error = False
            self.updated = 0
            for i, (yymmdd_l_no, lane) in enumerate(data_detail):
                sys.stdout.write('\r' + str(i + 1) + '/' + str(len(data_detail)) + ' 경마일자별 상세 Update 작업')
                sys.stdout.flush()
                self.updated += 1
                place_holder = []

                sql = "update " + tbl_name + " set "
                for column in data_detail[(yymmdd_l_no, lane)].col:
                    # 데이터 있는 경우만 추가
                    if data_detail[(yymmdd_l_no, lane)].col[column] != '':
                        sql = sql + column + '='
                        sql = sql + "%s,"
                        place_holder.append((data_detail[(yymmdd_l_no, lane)].col[column]))
                sql = sql[0:-1]  # 마지막 ',' 제거
                sql = sql + " where YYMMDD_L_NO = '" + yymmdd_l_no + "' and RHT_START_LANE_NO = " + str(lane)

                with self.get_connection.conn.cursor() as curs:
                    curs.execute(sql, place_holder)
            self.get_connection.conn.commit()
            print('\n[Update] ' + tbl_name + ' table ->' + str(self.updated) + '] updated...')
        except Exception as e:
            print(' db(' + tbl_name + ')  update error')
            print(' error : "{}", place_holder : {}'.format(e, place_holder))
            print(' sql = ' + sql)
            self.get_connection.conn.rollback()
            return True
        finally:
            curs.close()
            if self.get_connection.conn.open:
                print('[Update] db connection is still opened')
            else:
                print('[Update] db connection is finally closed')
        return db_error

    def update_rst_daydb(self, data_day, tbl_name):


        try:
            self.updated = 0
            db_error = False
            for i, yymmdd_l_no in enumerate(data_day):
                sys.stdout.write('\r' + str(i + 1) + '/' + str(len(data_day)) + ' 경마일자별 기본 Update 작업')
                sys.stdout.flush()

                place_holder = []
                sql = "update " + tbl_name + " set STAGE = 'CHULMA' "
                sql = sql + " where YYMMDD_L_NO = '" + yymmdd_l_no + "'"
                with self.get_connection.conn.cursor() as curs:
                    curs.execute(sql, place_holder)
            self.get_connection.conn.commit()
            print('\n[Update] ' + tbl_name + ' table ->' + str(self.updated) + '] updated...')
        except Exception as e:
            print(' db(' + tbl_name + ')  update error')
            print(' error : "{}", place_holder : {}'.format(e, place_holder))
            print(' sql = ' + sql)
            self.get_connection.conn.rollback()
            return True
        finally:
            curs.close()
            # self.get_connection.conn.close()
            if self.get_connection.conn.open:
                print('[Update] db connection is still opened')
            else:
                print('[Update] db connection is finally closed')
        return db_error

class DeleteResult:
    def __init__(self):
        self.get_connection= hocm.ConnectMySQL()
        if self.get_connection.conn.open:
            print('[Delete] db connection has begun to open')
        else:
            print('[Delete] db connection Error')

    def delete_exist_data(self, delinfo, tbl_name):

        try:
            db_error = False
            self.deleted = 0
            for i, yymmdd_l_no in enumerate(delinfo):
                self.deleted += 1
                sys.stdout.write('\r' + str(i + 1) + '/' + str(len(delinfo)) + ' 상세 작업 에러로 인한 delete 작업')
                sys.stdout.flush()
                with self.get_connection.conn.cursor() as curs:
                    sql = ''
                    sql = 'delete from ' + tbl_name + ' where YYMMDD_L_NO =%s'
                    curs.execute(sql, yymmdd_l_no)
            self.get_connection.conn.commit()
            print('\n[Delete] ' + tbl_name + ' table ->' + str(self.deleted) + '] deleted...')
        except Exception as e:
            print(' db(' + tbl_name + ')  update error')
            print(' error : "{}", place_holder : {}'.format(e))
            print(' sql = ' + sql)
            self.get_connection.conn.rollback()
            return True
        finally:
            curs.close()
            self.get_connection.conn.close()
            if self.get_connection.conn.open:
                print('[Delete] db connection is opened')
            else:
                print('[Delete] db connection is finally closed')
        return db_error
