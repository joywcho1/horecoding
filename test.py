import unittest
import warnings
import hogetpage.get_page as hgp
import hogetdb.execute_query as hoe
import collections
import sys


class A:
    def __init__(self):
        return

    def print_c(self, a):
        print(a)

    def amethod(self):
        self.a = 'A class attribute'
        self.print_c(self.a)
        print('a method')

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


class B():
    def __init__(self):
        return

    def print_c(self, a):
        print(a)

    def bmethod(self):
        self.a = 'B class attribute'
        self.print_c(self.a)
        print('b method')

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


class Odds:
    def __init__(self):
        self.col = collections.OrderedDict()
        self.col['RST_QUINELLA_TREBLES_DRT'] = ''


def test():
    meet_list = ['1', '2', '3']

    for meet in meet_list:
        meet_odds = collections.OrderedDict()
        if meet == '1':
            tblname = 'sa_tblsebedang'
        elif meet == '2':
            tblname = 'sa_tbljebedang'
        elif meet == '3':
            tblname = 'sa_tblbsbedang'
        else:
            return
        get_to_work = hoe.SelectResult()
        get_yymmdd_sql = 'select YYMMDD_L_NO, NUM_HO from ' + tblname
        err, to_work = get_to_work.select_sql(get_yymmdd_sql, 'dataframe')

        get_odds = hgp.AboutOddsInPage()
        for index, row in to_work.iterrows():
            yymmdd_l_no = row['YYMMDD_L_NO']
            num_ho = row['NUM_HO']
            sys.stdout.write('\r' + str(index + 1) + '/' + str(len(to_work)) + ' page 작업 :' + yymmdd_l_no)
            sys.stdout.flush()
            meet_odds[yymmdd_l_no] = Odds()
            rsterr, rst_str = get_odds.odds_trebles_page_read(yymmdd_l_no, num_ho)

            if rsterr:
                continue
            else:
                meet_odds[yymmdd_l_no].col['RST_QUINELLA_TREBLES_DRT'] = rst_str

        update_odds = hoe.UpdateResult()
        db_error = update_odds.update_daydb(meet_odds, tblname)
        if db_error:
            break
    print("완료")


class TestCless(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter('ignore', category=ImportWarning)

    def test_runs(self):
        test()


if __name__ == '__main__':
    unittest.main()
