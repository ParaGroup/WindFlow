# Script to run regression tests of the WindFlow library

# GRAPH TESTS
./build/bin/graph_tests/test_graph_1 -r 100 -l 13335 -k 9;
./build/bin/graph_tests/test_graph_2 -r 100 -l 13335 -k 9;
./build/bin/graph_tests/test_graph_3 -r 100 -l 13335 -k 9;
./build/bin/graph_tests/test_graph_4 -r 100 -l 13335 -k 9;
./build/bin/graph_tests/test_graph_5 -r 100 -l 13335 -k 9;
./build/bin/graph_tests/test_graph_6 -r 100 -l 13335 -k 9;
./build/bin/graph_tests/test_graph_7 -r 100 -l 13335 -k 9;
./build/bin/graph_tests/test_graph_8 -r 100 -l 13335 -k 9;
./build/bin/graph_tests/test_graph_9 -r 100 -l 13335 -k 9;
./build/bin/graph_tests/test_graph_10 -r 100 -l 13335 -k 9;
./build/bin/graph_tests/test_graph_11 -r 100 -l 13335 -k 9;
./build/bin/graph_tests/test_graph_12 -r 100 -l 13335 -k 9;

# MERGE TESTS
./build/bin/merge_tests/test_merge_1 -r 100 -l 13335 -k 9;
./build/bin/merge_tests/test_merge_2 -r 100 -l 13335 -k 9;
./build/bin/merge_tests/test_merge_3 -r 100 -l 13335 -k 9;

# SPLIT TESTS
./build/bin/split_tests/test_split_1 -r 100 -l 13335 -k 9;
./build/bin/split_tests/test_split_2 -r 100 -l 13335 -k 9;
./build/bin/split_tests/test_split_3 -r 100 -l 13335 -k 9;

# WINDOWED TESTS (time-based Windows)
./build/bin/win_tests/test_win_kw_tb -r 100 -l 13335 -k 9 -w 500000 -s 17000;
./build/bin/win_tests/test_win_pw_tb -r 100 -l 13335 -k 9 -w 500000 -s 17000;
./build/bin/win_tests/test_win_paw_tb -r 100 -l 13335 -k 9 -w 500000 -s 17000;
./build/bin/win_tests/test_win_mrw_tb -r 100 -l 13335 -k 9 -w 500000 -s 17000;
./build/bin/win_tests/test_win_fat_tb -r 100 -l 13335 -k 9 -w 500000 -s 17000;

# WINDOWED TESTS (count-based Windows)
./build/bin/win_tests/test_win_kw_cb -r 100 -l 13335 -k 9 -w 335 -s 22;
./build/bin/win_tests/test_win_pw_cb -r 100 -l 13335 -k 9 -w 335 -s 22;
./build/bin/win_tests/test_win_paw_cb -r 100 -l 13335 -k 9 -w 335 -s 22;
./build/bin/win_tests/test_win_mrw_cb -r 100 -l 13335 -k 9 -w 335 -s 22;
./build/bin/win_tests/test_win_fat_cb -r 100 -l 13335 -k 9 -w 335 -s 22;

# GRAPH TESTS (one key)
./build/bin/graph_tests/test_graph_1 -r 100 -l 13335 -k 1;
./build/bin/graph_tests/test_graph_2 -r 100 -l 13335 -k 1;
./build/bin/graph_tests/test_graph_3 -r 100 -l 13335 -k 1;
./build/bin/graph_tests/test_graph_4 -r 100 -l 13335 -k 1;
./build/bin/graph_tests/test_graph_5 -r 100 -l 13335 -k 1;
./build/bin/graph_tests/test_graph_6 -r 100 -l 13335 -k 1;
./build/bin/graph_tests/test_graph_7 -r 100 -l 13335 -k 1;
./build/bin/graph_tests/test_graph_8 -r 100 -l 13335 -k 1;
./build/bin/graph_tests/test_graph_9 -r 100 -l 13335 -k 1;
./build/bin/graph_tests/test_graph_10 -r 100 -l 13335 -k 1;
./build/bin/graph_tests/test_graph_11 -r 100 -l 13335 -k 1;
./build/bin/graph_tests/test_graph_12 -r 100 -l 13335 -k 1;

# MERGE TESTS (one key)
./build/bin/merge_tests/test_merge_1 -r 100 -l 13335 -k 1;
./build/bin/merge_tests/test_merge_2 -r 100 -l 13335 -k 1;
./build/bin/merge_tests/test_merge_3 -r 100 -l 13335 -k 1;

# SPLIT TESTS (one key)
./build/bin/split_tests/test_split_1 -r 100 -l 13335 -k 1;
./build/bin/split_tests/test_split_2 -r 100 -l 13335 -k 1;
./build/bin/split_tests/test_split_3 -r 100 -l 13335 -k 1;

# WINDOWED TESTS (time-based Windows and one key)
./build/bin/win_tests/test_win_kw_tb -r 100 -l 13335 -k 1 -w 500000 -s 17000;
./build/bin/win_tests/test_win_pw_tb -r 100 -l 13335 -k 1 -w 500000 -s 17000;
./build/bin/win_tests/test_win_paw_tb -r 100 -l 13335 -k 1 -w 500000 -s 17000;
./build/bin/win_tests/test_win_mrw_tb -r 100 -l 13335 -k 1 -w 500000 -s 17000;
./build/bin/win_tests/test_win_fat_tb -r 100 -l 13335 -k 1 -w 500000 -s 17000;

# WINDOWED TESTS (count-based Windows and one key)
./build/bin/win_tests/test_win_kw_cb -r 100 -l 13335 -k 1 -w 335 -s 22;
./build/bin/win_tests/test_win_pw_cb -r 100 -l 13335 -k 1 -w 335 -s 22;
./build/bin/win_tests/test_win_paw_cb -r 100 -l 13335 -k 1 -w 335 -s 22;
./build/bin/win_tests/test_win_mrw_cb -r 100 -l 13335 -k 1 -w 335 -s 22;
./build/bin/win_tests/test_win_fat_cb -r 100 -l 13335 -k 1 -w 335 -s 22;

# INTERVAL JOIN TESTS
./build/bin/join_tests/test_join_1 -r 100 -l 13335 -k 9 -L -250 -U 250;
./build/bin/join_tests/test_join_2 -r 100 -l 13335 -k 9 -L -250 -U 250;
./build/bin/join_tests/test_join_3 -r 100 -l 13335 -k 9 -L -250 -U 250;
./build/bin/join_tests/test_join_4 -r 100 -l 13335 -k 9 -L -250 -U 250;
./build/bin/join_tests/test_join_5 -r 100 -l 13335 -k 9 -L -250 -U 250;

# sh run_regression_tests.sh 2>&1 | tee output.log
