# Long script to test correctness

# SUM TESTS with CB windows on CPU
./build/bin/sum_tests_cpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -r 3 -n 5 -m 4 -o 0;
./build/bin/sum_tests_cpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -r 3 -n 5 -m 4 -o 1;
./build/bin/sum_tests_cpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -r 3 -n 5 -m 4 -o 2;
./build/bin/sum_tests_cpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -r 3 -n 1 -m 4 -o 0;
./build/bin/sum_tests_cpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -r 3 -n 1 -m 4 -o 1;
./build/bin/sum_tests_cpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -r 3 -n 1 -m 4 -o 2;
./build/bin/sum_tests_cpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -r 3 -n 5 -m 1 -o 0;
./build/bin/sum_tests_cpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -r 3 -n 5 -m 1 -o 1;
./build/bin/sum_tests_cpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -r 3 -n 5 -m 1 -o 2;
./build/bin/sum_tests_cpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -r 3 -n 1 -m 1 -o 0;
./build/bin/sum_tests_cpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -r 3 -n 1 -m 1 -o 1;
./build/bin/sum_tests_cpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -r 3 -n 1 -m 1 -o 2;

# SUM TESTS with TB windows on CPU
./build/bin/sum_tests_cpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -r 3 -n 5 -m 4 -o 0;
./build/bin/sum_tests_cpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -r 3 -n 5 -m 4 -o 1;
./build/bin/sum_tests_cpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -r 3 -n 5 -m 4 -o 2;
./build/bin/sum_tests_cpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -r 3 -n 1 -m 4 -o 0;
./build/bin/sum_tests_cpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -r 3 -n 1 -m 4 -o 1;
./build/bin/sum_tests_cpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -r 3 -n 1 -m 4 -o 2;
./build/bin/sum_tests_cpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -r 3 -n 5 -m 1 -o 0;
./build/bin/sum_tests_cpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -r 3 -n 5 -m 1 -o 1;
./build/bin/sum_tests_cpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -r 3 -n 5 -m 1 -o 2;
./build/bin/sum_tests_cpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -r 3 -n 1 -m 1 -o 0;
./build/bin/sum_tests_cpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -r 3 -n 1 -m 1 -o 1;
./build/bin/sum_tests_cpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -r 3 -n 1 -m 1 -o 2;

# MULTIPIPE TESTS with CB windows on CPU
./build/bin/mp_tests_cpu/test_mp_kf_cb -r 50 -l 100000 -k 3 -w 330 -s 8;
./build/bin/mp_tests_cpu/test_mp_kf_cb_string -r 50 -l 100000 -k 3 -w 330 -s 8;
./build/bin/mp_tests_cpu/test_mp_kff_cb -r 50 -l 100000 -k 3 -w 330 -s 8;
./build/bin/mp_tests_cpu/test_mp_wf_cb -r 50 -l 100000 -k 3 -w 330 -s 8;
./build/bin/mp_tests_cpu/test_mp_wf_cb_string -r 50 -l 100000 -k 3 -w 330 -s 8;
./build/bin/mp_tests_cpu/test_mp_pf_cb -r 50 -l 100000 -k 3 -w 330 -s 8;
./build/bin/mp_tests_cpu/test_mp_pf_cb_string -r 50 -l 100000 -k 3 -w 330 -s 8;
./build/bin/mp_tests_cpu/test_mp_wmr_cb -r 50 -l 100000 -k 3 -w 330 -s 8;
./build/bin/mp_tests_cpu/test_mp_wmr_cb_string -r 50 -l 100000 -k 3 -w 330 -s 8;
./build/bin/mp_tests_cpu/test_mp_kf+pf_cb -r 50 -l 100000 -k 3 -w 330 -s 8;
./build/bin/mp_tests_cpu/test_mp_kf+wmr_cb -r 50 -l 100000 -k 3 -w 330 -s 8;
./build/bin/mp_tests_cpu/test_mp_wf+pf_cb -r 50 -l 100000 -k 3 -w 330 -s 8;
./build/bin/mp_tests_cpu/test_mp_wf+wmr_cb -r 50 -l 100000 -k 3 -w 330 -s 8;

# MULTIPIPE TESTS with TB windows on CPU (DETERMINISTIC MODE)
./build/bin/mp_tests_cpu/test_mp_kf_tb -r 50 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_kff_tb -r 50 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_wf_tb -r 50 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_pf_tb -r 50 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_wmr_tb -r 50 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_kf+pf_tb -r 50 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_kf+wmr_tb -r 50 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_wf+pf_tb -r 50 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_wf+wmr_tb -r 50 -l 100000 -k 3 -w 35000 -s 2000;

# MULTIPIPE TESTS with TB windows on CPU (DEFAULT MODE with large delay)
./build/bin/mp_tests_cpu/test_mp_kf_tb_oop -r 10 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_kff_tb_oop -r 10 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_wf_tb_oop -r 10 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_pf_tb_oop -r 10 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_wmr_tb_oop -r 10 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_kf+pf_tb_oop -r 10 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_kf+wmr_tb_oop -r 10 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_wf+pf_tb_oop -r 10 -l 100000 -k 3 -w 35000 -s 2000;
./build/bin/mp_tests_cpu/test_mp_wf+wmr_tb_oop -r 10 -l 100000 -k 3 -w 35000 -s 2000;

# SUM TESTS with CB windows on GPU
./build/bin/sum_tests_gpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -b 47 -r 3 -n 5 -m 4 -o 0;
./build/bin/sum_tests_gpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -b 47 -r 3 -n 5 -m 4 -o 1;
./build/bin/sum_tests_gpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -b 47 -r 3 -n 5 -m 4 -o 2;
./build/bin/sum_tests_gpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -b 47 -r 3 -n 1 -m 4 -o 0;
./build/bin/sum_tests_gpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -b 47 -r 3 -n 1 -m 4 -o 1;
./build/bin/sum_tests_gpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -b 47 -r 3 -n 1 -m 4 -o 2;
./build/bin/sum_tests_gpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -b 47 -r 3 -n 5 -m 1 -o 0;
./build/bin/sum_tests_gpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -b 47 -r 3 -n 5 -m 1 -o 1;
./build/bin/sum_tests_gpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -b 47 -r 3 -n 5 -m 1 -o 2;
./build/bin/sum_tests_gpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -b 47 -r 3 -n 1 -m 1 -o 0;
./build/bin/sum_tests_gpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -b 47 -r 3 -n 1 -m 1 -o 1;
./build/bin/sum_tests_gpu/test_all_cb -l 100000 -k 7 -w 330 -s 15 -b 47 -r 3 -n 1 -m 1 -o 2;

# SUM TESTS with TB windows on GPU
./build/bin/sum_tests_gpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -b 500 -r 3 -n 5 -m 4 -o 0;
./build/bin/sum_tests_gpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -b 500 -r 3 -n 5 -m 4 -o 1;
./build/bin/sum_tests_gpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -b 500 -r 3 -n 5 -m 4 -o 2;
./build/bin/sum_tests_gpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -b 500 -r 3 -n 1 -m 4 -o 0;
./build/bin/sum_tests_gpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -b 500 -r 3 -n 1 -m 4 -o 1;
./build/bin/sum_tests_gpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -b 500 -r 3 -n 1 -m 4 -o 2;
./build/bin/sum_tests_gpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -b 500 -r 3 -n 5 -m 1 -o 0;
./build/bin/sum_tests_gpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -b 500 -r 3 -n 5 -m 1 -o 1;
./build/bin/sum_tests_gpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -b 500 -r 3 -n 5 -m 1 -o 2;
./build/bin/sum_tests_gpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -b 500 -r 3 -n 1 -m 1 -o 0;
./build/bin/sum_tests_gpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -b 500 -r 3 -n 1 -m 1 -o 1;
./build/bin/sum_tests_gpu/test_all_tb -l 100000 -k 7 -w 35000 -s 2000 -b 500 -r 3 -n 1 -m 1 -o 2;

# MULTIPIPE TESTS with CB windows on GPU
./build/bin/mp_tests_gpu/test_mp_gpu_kf_cb -r 50 -l 100000 -k 3 -b 47 -w 330 -s 8;
./build/bin/mp_tests_gpu/test_mp_gpu_kff_cb -r 50 -l 100000 -k 3 -b 47 -w 330 -s 8;
./build/bin/mp_tests_gpu/test_mp_gpu_kf_cb_string -r 50 -l 100000 -k 3 -b 47 -w 330 -s 8;
./build/bin/mp_tests_gpu/test_mp_gpu_wf_cb -r 50 -l 100000 -k 3 -b 47 -w 330 -s 8;
./build/bin/mp_tests_gpu/test_mp_gpu_wf_cb_string -r 50 -l 100000 -k 3 -b 47 -w 330 -s 8;
./build/bin/mp_tests_gpu/test_mp_gpu_pf_cb -r 50 -l 100000 -k 3 -b 47 -w 330 -s 8;
./build/bin/mp_tests_gpu/test_mp_gpu_pf_cb_string -r 50 -l 100000 -k 3 -b 47 -w 330 -s 8;
./build/bin/mp_tests_gpu/test_mp_gpu_wmr_cb -r 50 -l 100000 -k 3 -b 47 -w 330 -s 8;
./build/bin/mp_tests_gpu/test_mp_gpu_wmr_cb_string -r 50 -l 100000 -k 3 -b 47 -w 330 -s 8;
./build/bin/mp_tests_gpu/test_mp_gpu_kf+pf_cb -r 50 -l 100000 -k 3 -b 47 -w 330 -s 8;
./build/bin/mp_tests_gpu/test_mp_gpu_kf+wmr_cb -r 50 -l 100000 -k 3 -b 47 -w 330 -s 8;
./build/bin/mp_tests_gpu/test_mp_gpu_wf+pf_cb -r 50 -l 100000 -k 3 -b 47 -w 330 -s 8;
./build/bin/mp_tests_gpu/test_mp_gpu_wf+wmr_cb -r 50 -l 100000 -k 3 -b 47  -w 330 -s 8;

# MULTIPIPE TESTS with TB windows on GPU (DETERMINISTIC MODE)
./build/bin/mp_tests_gpu/test_mp_gpu_kf_tb -r 50 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_kff_tb -r 50 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_wf_tb -r 50 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_pf_tb -r 50 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_wmr_tb -r 50 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_kf+pf_tb -r 50 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_kf+wmr_tb -r 50 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_wf+pf_tb -r 50 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_wf+wmr_tb -r 50 -l 100000 -k 3 -b 500 -w 35000 -s 2000;

# MULTIPIPE TESTS with TB windows on GPU (DEFAULT MODE with large delay)
./build/bin/mp_tests_gpu/test_mp_gpu_kf_tb_oop -r 10 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_kff_tb_oop -r 10 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_wf_tb_oop -r 10 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_pf_tb_oop -r 10 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_wmr_tb_oop -r 10 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_kf+pf_tb_oop -r 10 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_kf+wmr_tb_oop -r 10 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_wf+pf_tb_oop -r 10 -l 100000 -k 3 -b 500 -w 35000 -s 2000;
./build/bin/mp_tests_gpu/test_mp_gpu_wf+wmr_tb_oop -r 10 -l 100000 -k 3 -b 500 -w 35000 -s 2000;

# MERGE TESTS
./build/bin/merge_tests/test_merge_1 -r 50 -l 10000 -k 7;
./build/bin/merge_tests/test_merge_2 -r 50 -l 10000 -k 7;
./build/bin/merge_tests/test_merge_3 -r 50 -l 10000 -k 7;
./build/bin/merge_tests/test_merge_4 -r 50 -l 10000 -k 7;

# SPLIT TESTS
./build/bin/split_tests/test_split_1 -r 50 -l 10000 -k 7;
./build/bin/split_tests/test_split_2 -r 50 -l 10000 -k 7;
./build/bin/split_tests/test_split_3 -r 50 -l 10000 -k 7;
./build/bin/split_tests/test_split_4 -r 50 -l 10000 -k 7;
./build/bin/split_tests/test_split_5 -r 50 -l 10000 -k 7;

# GRAPH TESTS
./build/bin/graph_tests/test_graph_1 -r 50 -l 10000 -k 7;
./build/bin/graph_tests/test_graph_2 -r 50 -l 10000 -k 7;
./build/bin/graph_tests/test_graph_3 -r 50 -l 10000 -k 7;
./build/bin/graph_tests/test_graph_4 -r 50 -l 10000 -k 7;
./build/bin/graph_tests/test_graph_5 -r 50 -l 10000 -k 7;
./build/bin/graph_tests/test_graph_6 -r 50 -l 10000 -k 7;
./build/bin/graph_tests/test_graph_7 -r 50 -l 10000 -k 7;
./build/bin/graph_tests/test_graph_8 -r 50 -l 10000 -k 7;
./build/bin/graph_tests/test_graph_9 -r 50 -l 10000 -k 7;

# MISCELLANEA TESTS
./build/bin/miscellanea/test_diagram -l 10000 -k 7;
