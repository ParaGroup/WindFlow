# Script to run regression tests (with GPU operators) of the WindFlow library

# GRAPH TESTS
./build/bin/graph_tests_gpu/test_graph_gpu_1 -r 100 -l 11220 -k 7;
./build/bin/graph_tests_gpu/test_graph_gpu_2 -r 100 -l 11220 -k 7;
./build/bin/graph_tests_gpu/test_graph_gpu_3 -r 100 -l 11220 -k 7;
./build/bin/graph_tests_gpu/test_graph_gpu_4 -r 100 -l 11220 -k 7;
./build/bin/graph_tests_gpu/test_graph_gpu_5 -r 100 -l 11220 -k 7;

# MERGE TESTS
./build/bin/merge_tests_gpu/test_merge_gpu_1 -r 100 -l 11220 -k 7;
./build/bin/merge_tests_gpu/test_merge_gpu_2 -r 100 -l 11220 -k 7;
./build/bin/merge_tests_gpu/test_merge_gpu_3 -r 100 -l 11220 -k 7;
./build/bin/merge_tests_gpu/test_merge_gpu_4 -r 100 -l 11220 -k 7;
./build/bin/merge_tests_gpu/test_merge_gpu_5 -r 100 -l 11220 -k 7;
./build/bin/merge_tests_gpu/test_merge_gpu_kb_1 -r 100 -l 11220;
./build/bin/merge_tests_gpu/test_merge_gpu_kb_2 -r 100 -l 11220;
./build/bin/merge_tests_gpu/test_merge_gpu_kb_3 -r 100 -l 11220;
./build/bin/merge_tests_gpu/test_merge_gpu_kb_4 -r 100 -l 11220;
./build/bin/merge_tests_gpu/test_merge_gpu_kb_5 -r 100 -l 11220;

# SPLIT TESTS
./build/bin/split_tests_gpu/test_split_gpu_1 -r 100 -l 12000 -k 7;
./build/bin/split_tests_gpu/test_split_gpu_2 -r 100 -l 12000 -k 7;
./build/bin/split_tests_gpu/test_split_gpu_3 -r 100 -l 12000 -k 7;
./build/bin/split_tests_gpu/test_split_gpu_4 -r 100 -l 12000 -k 7;
./build/bin/split_tests_gpu/test_split_gpu_5 -r 100 -l 12000 -k 7;
./build/bin/split_tests_gpu/test_split_gpu_kb_1 -r 100 -l 12000;
./build/bin/split_tests_gpu/test_split_gpu_kb_2 -r 100 -l 12000;
./build/bin/split_tests_gpu/test_split_gpu_kb_3 -r 100 -l 12000;
./build/bin/split_tests_gpu/test_split_gpu_kb_4 -r 100 -l 12000;
./build/bin/split_tests_gpu/test_split_gpu_kb_5 -r 100 -l 12000;

# WINDOWED TESTS (time-based Windows)
./build/bin/win_tests_gpu/test_win_kw_gpu_tb -r 100 -l 11220 -k 7 -w 200000 -s 12000;
./build/bin/win_tests_gpu/test_win_pw_gpu_tb -r 100 -l 11220 -k 7 -w 200000 -s 12000;
./build/bin/win_tests_gpu/test_win_paw_gpu_tb -r 100 -l 11220 -k 7 -w 200000 -s 12000;
./build/bin/win_tests_gpu/test_win_mrw_gpu_tb -r 100 -l 11220 -k 7 -w 200000 -s 12000;
./build/bin/win_tests_gpu/test_win_fat_gpu_tb -r 100 -l 11220 -k 7 -w 200000 -s 12000;

# GRAPH TESTS (one key)
./build/bin/graph_tests_gpu/test_graph_gpu_1 -r 100 -l 11220 -k 1;
./build/bin/graph_tests_gpu/test_graph_gpu_2 -r 100 -l 11220 -k 1;
./build/bin/graph_tests_gpu/test_graph_gpu_3 -r 100 -l 11220 -k 1;
./build/bin/graph_tests_gpu/test_graph_gpu_4 -r 100 -l 11220 -k 1;
./build/bin/graph_tests_gpu/test_graph_gpu_5 -r 100 -l 11220 -k 1;

# MERGE TESTS (one key)
./build/bin/merge_tests_gpu/test_merge_gpu_1 -r 100 -l 11220 -k 1;
./build/bin/merge_tests_gpu/test_merge_gpu_2 -r 100 -l 11220 -k 1;
./build/bin/merge_tests_gpu/test_merge_gpu_3 -r 100 -l 11220 -k 1;
./build/bin/merge_tests_gpu/test_merge_gpu_4 -r 100 -l 11220 -k 1;
./build/bin/merge_tests_gpu/test_merge_gpu_5 -r 100 -l 11220 -k 1;
./build/bin/merge_tests_gpu/test_merge_gpu_kb_1 -r 100 -l 11220;
./build/bin/merge_tests_gpu/test_merge_gpu_kb_2 -r 100 -l 11220;
./build/bin/merge_tests_gpu/test_merge_gpu_kb_3 -r 100 -l 11220;
./build/bin/merge_tests_gpu/test_merge_gpu_kb_4 -r 100 -l 11220;
./build/bin/merge_tests_gpu/test_merge_gpu_kb_5 -r 100 -l 11220;

# SPLIT TESTS (one key)
./build/bin/split_tests_gpu/test_split_gpu_1 -r 100 -l 12000 -k 1;
./build/bin/split_tests_gpu/test_split_gpu_2 -r 100 -l 12000 -k 1;
./build/bin/split_tests_gpu/test_split_gpu_3 -r 100 -l 12000 -k 1;
./build/bin/split_tests_gpu/test_split_gpu_4 -r 100 -l 12000 -k 1;
./build/bin/split_tests_gpu/test_split_gpu_5 -r 100 -l 12000 -k 1;
./build/bin/split_tests_gpu/test_split_gpu_kb_1 -r 100 -l 12000;
./build/bin/split_tests_gpu/test_split_gpu_kb_2 -r 100 -l 12000;
./build/bin/split_tests_gpu/test_split_gpu_kb_3 -r 100 -l 12000;
./build/bin/split_tests_gpu/test_split_gpu_kb_4 -r 100 -l 12000;
./build/bin/split_tests_gpu/test_split_gpu_kb_5 -r 100 -l 12000;

# WINDOWED TESTS (time-based Windows and one key)
./build/bin/win_tests_gpu/test_win_kw_gpu_tb -r 100 -l 11220 -k 1 -w 200000 -s 12000;
./build/bin/win_tests_gpu/test_win_pw_gpu_tb -r 100 -l 11220 -k 1 -w 200000 -s 12000;
./build/bin/win_tests_gpu/test_win_paw_gpu_tb -r 100 -l 11220 -k 1 -w 200000 -s 12000;
./build/bin/win_tests_gpu/test_win_mrw_gpu_tb -r 100 -l 11220 -k 1 -w 200000 -s 12000;
./build/bin/win_tests_gpu/test_win_fat_gpu_tb -r 100 -l 11220 -k 1 -w 200000 -s 12000;

# sh run_regression_gpu_tests.sh 2>&1 | tee output_gpu.log
