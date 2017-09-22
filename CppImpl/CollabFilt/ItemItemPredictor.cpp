// Author: Senthil Kumar Thangavelu kingjuliyen@gmail.com

/*
  Compile:
  g++  ItemItemPredictor.cpp \
       -I/opt/boost/1_61_0/include/ \
       -L/opt/boost/1_61_0/lib \
       -lboost_program_options \
       -lpthread \
       -O3 \
       -o /tmp/itemitem-predictor \
       -std=c++11

  Usage:
  export DYLD_LIBRARY_PATH=/opt/boost/1_61_0/lib:$DYLD_LIBRARY_PATH

  date && \
  time /tmp/itemitem-predictor \
        --top-K-neighbours 17000 \
        --similarity-cutoff-value 0.20 \
        --loop-mode-count 20 \
        --max-threads-count 6000 \
        --input-csv-file-path /tmp/sf41.csv \
        --sim-mtx-file-path /tmp/sim-vals.mtx \
        --item-index-table-path /tmp/itm.idx \
        --user-index-table-path /tmp/usr.idx && \
  date

*/

#include <iostream>
#include <string>
#include <algorithm>
#include <boost/program_options.hpp>
#include "ItemItemPredictor.hpp"

namespace ProgOpts = boost::program_options;

STRPTR(input_csv_str, "Path of input csv file to read rating entries from ");
STRPTR(verbose_mode_level_str, "Show debug info about inner workings ");
STRPTR(loop_mode_count_str, "Run repeatedly in loop mode for same input ");
STRPTR(top_K_neighbours_str, "Top K neighbours to consider ");
STRPTR(sim_mtx_file_path_str, "Path of similarity matrix to load from ");
STRPTR(item_index_table_path_str, "Item's index lookup table path to load from ");
STRPTR(user_index_table_path_str, "User's index lookup table path to load from ");
STRPTR(wait_for_debugger_str, "Wait for debugger to connect in a while(1) loop ");
STRPTR(similarity_cutoff_str, "Select items only if greater than this cutoff ");
STRPTR(training_sample_percentage_str,
  "Percentage of data set to be used for training ");
STRPTR(num_threads_str, "Number of threads to be used ");

volatile bool use_debugger = false;

bool processInputArgs(int argc, char * argv[], ProgOpts::variables_map &varMap,
                      ItemItemPredictorParams &params)
{
    try {
        ProgOpts::options_description desc("Allowed Options");

        desc.add_options()
                ("help", "produce help message")
                ("training-sample-percentage", ProgOpts::value<FLT_T>(), training_sample_percentage_str)
                ("input-csv-file-path", ProgOpts::value<STRING_T>(), input_csv_str)
                ("verbose-mode-level", ProgOpts::value<INT_T>(), verbose_mode_level_str)
                ("loop-mode-count", ProgOpts::value<INT_T>(), loop_mode_count_str)
                ("top-K-neighbours", ProgOpts::value<INT_T>(), top_K_neighbours_str)
                ("sim-mtx-file-path", ProgOpts::value<STRING_T>(),sim_mtx_file_path_str)
                ("item-index-table-path", ProgOpts::value<STRING_T>(), item_index_table_path_str)
                ("user-index-table-path", ProgOpts::value<STRING_T>(), user_index_table_path_str)
                ("wait-for-debugger", ProgOpts::value<INT_T>(), wait_for_debugger_str)
                ("similarity-cutoff-value", ProgOpts::value<FLT_T>(), similarity_cutoff_str)
                ("max-threads-count", ProgOpts::value<INT_T>(), num_threads_str)
                ; // leave this semi colon at end don't move this

        ProgOpts::store(ProgOpts::parse_command_line(argc, argv, desc), varMap);
        ProgOpts::notify(varMap);

        if (varMap.count("help")) {
            cout << desc << "\n";
            return false;
        }

        // attach to process -->  process attach --pid 99130
        // then --> expression use_debugger = false
        if(varMap.count("wait-for-debugger")) {
          use_debugger = varMap["wait-for-debugger"].as<INT_T>() == 0? false : true;
          while(use_debugger) { }
        }

        OPT(training_sample_percentage, "training-sample-percentage", FLT_T, 0.70);
        OPT(similarity_cutoff_value, "similarity-cutoff-value", FLT_T, 0.90);
        OPT(verbose_mode_level, "verbose-mode-level", INT_T, 0);
        OPT(loop_mode_count, "loop-mode-count", INT_T, 1);
        OPT(top_K_neighbours, "top-K-neighbours", INT_T, 1);
        OPT(csv_input_file_path, "input-csv-file-path",
          STRING_T, STRING_T("unknown_bad.csv"));
        OPT(sim_mtx_file_path, "sim-mtx-file-path",
          STRING_T, STRING_T("/tmp/sim-mtx-file.mtx"));
        OPT(item_index_table_path, "item-index-table-path",
          STRING_T, STRING_T("/tmp/item-index-table.idx"));
        OPT(user_index_table_path, "user-index-table-path",
          STRING_T, STRING_T("/tmp/user-index-table.idx"));
        OPT(max_threads_count, "max-threads-count", INT_T, 60);
    }
    catch(exception &e)
    {
        cerr << "\n processInputArgs error: " << e.what() << "\n";
        return false;
    }
    return true;
}

void printHeader() {
    cout << "\n\n\n =================================================\n";
    cout << " ============== Starting predictions ===============\n";
    cout << " =================================================\n\n";
}

int startPredictions(int argc, char * argv[])
{
    ProgOpts::variables_map vmp; // varmap
    ItemItemPredictorParams params;

    if(!processInputArgs(argc, argv, vmp, params))
       return 3;

    printHeader();

    FLT_T rmse = 0, numTrials = 0;

    ItemItemPredictor ipred(params);
    ipred.prepareForPrediction();
    ipred.generateRecommendationsForAllUsers(STRING_T("/tmp/recos/"), 20,
                                              params.max_threads_count);

    cout << "ipred.prepareForPrediction() done " << endl;
#if 0
    for(int i=0; i<params.loop_mode_count; i++) {
        try {
          ItemItemPredictor ipred(params);
          ipred.prepareForPrediction();
          cout << " ipred.recommendProducts\n";
          system("date");
          rmse += ipred.checkCrossValidation();
          numTrials++;
        } catch (const char * s) {
          cout << "Exception:: " << s << "\n";
        }
    }
    cout << " rmse " << (rmse/numTrials) << " found using "
      << numTrials << " trials " << endl;
#endif
    return 0;
}

int main(int argc, char * argv[])
{
    return startPredictions(argc, argv);
}
