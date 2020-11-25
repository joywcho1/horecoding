def main():
    import recode_insert as insert_work

    test_mode = False

    fromdate = '20201001'  # 1003, 0803 목
    todate = '20201124'
    ####################################################################################################################
    # Test
    ####################################################################################################################
    meet = ['1', '2', '3']
    # meet = ['2']
    step_mode = {'step_simsa': False, 'step_cul': True, 'step_rst': True, 'step_bedang': True, 'step_info': True}
    tbl_type = 'sa'
    ####################################################################################################################
    # 기간
    #######################################################################################################################
    span = {'span_mode': True, 'span_from_to': (fromdate, todate), 'page_from_to': (1, 5)}
    # span = {'span_mode': False, 'span_from_to' : ('', ''), 'page_from_to': (1, 3)}

    for i, v in step_mode.items():
        if i == 'step_info' and v == True:
            print('#' * 100)
            print('#' + i + ' is Started' + ' ' * 30)

            for meet_v in meet:
                msg = insert_work.get_horse_info(test_mode, meet_v, tbl_type)
                print('\n' + '#' + ' ' * 10 + i + ' : ' + msg + ' ' * 30)
            print('#' * 100)

        elif i == 'step_simsa' and v == True:
            print('#' * 100)
            print('#' + i + ' is Started' + ' ' * 30)

            for meet_v in meet:
                msg = insert_work.simsa_work(test_mode, meet_v, tbl_type, span)
                print('\n' + '# ' + i + ' : ' + msg + ' ' * 30)
            print('#' * 100)
        elif i == 'step_cul' and v == True:
            print('#' * 100)
            print('#' + i + ' is Started' + ' ' * 30)

            for meet_v in meet:
                msg = insert_work.chul_work(test_mode, meet_v, tbl_type, span)
                print('\n' + '# ' + i + ' : ' + msg + ' ' * 30)
            print('#' * 100)
        elif i == 'step_bedang' and v == True:
            print('#' * 100)
            print('#' + i + ' is Started' + ' ' * 30)
            for meet_v in meet:
                msg = insert_work.bedang_work(test_mode, meet_v, fromdate, todate, tbl_type)
                print('\n' + '#' + i + ' : ' + msg + ' ' * 30)
                print('#' * 100)
        elif i == 'step_rst' and v == True:
            print('#' * 100)
            print('#' + i + ' is Started' + ' ' * 30)

            for meet_v in meet:
                msg = insert_work.rst_work(test_mode, meet_v, tbl_type, span)
                print('\n' + '#' + i + ' : ' + msg + ' ' * 30)
                print('#' * 100)


if __name__ == '__main__':
    main()
