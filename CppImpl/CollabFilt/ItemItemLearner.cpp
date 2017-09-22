// Author: Senthil Kumar Thangavelu kingjuliyen@gmail.com

/*
  Compile:
  g++  ItemItemLearner.cpp \
       -I/opt/boost/1_61_0/include/ \
       -L/opt/boost/1_61_0/lib \
       -lboost_program_options \
       -lpthread \
       -O3 \
       -o /tmp/itemitem-learner \
       -std=c++11

  Usage:
  export DYLD_LIBRARY_PATH=/opt/boost/1_61_0/lib:$DYLD_LIBRARY_PATH
  export LD_LIBRARY_PATH=/opt/boost/1_61_0/lib:$LD_LIBRARY_PATH

  date && \
  time DYLD_LIBRARY_PATH=/opt/boost/1_61_0/lib:$DYLD_LIBRARY_PATH  \
  /tmp/itemitem-learner  \
         --training-sample-percentage 0.7 \
         --max-row-dimension 2700000 \
         --max-column-dimension 25000 \
         --verbose-mode-level 1 \
         --loop-mode-count 1 \
         --recommendation-type item-item \
         --max-threads-count 60 \
         --input-csv-file-path /tmp/sf41.csv \
         --sim-mtx-file-save-path /tmp/sim-vals.mtx \
         --item-index-table-path /tmp/itm.idx \
         --user-index-table-path /tmp/usr.idx \
         --wait-for-debugger 0 \
      && \
  date

e.g csv file (remove header)
usrid  itemid rating
785166 6066 3
521295 4972 3
1665652 11701 5
1963419 15887 3

Linux:

  date && \
  time LD_LIBRARY_PATH=/opt/boost/1_61_0/lib:$LD_LIBRARY_PATH  \
  /tmp/itemitem-learner  \
         --training-sample-percentage 0.7 \
         --max-row-dimension 2700000 \
         --max-column-dimension 25000 \
         --verbose-mode-level 1 \
         --loop-mode-count 1 \
         --top-K-neighbours 10 \
         --recommendation-type item-item \
         --max-threads-count 1 \
         --input-csv-file-path /tmp/sf41.csv \
         --sim-mtx-file-save-path /tmp/sim-vals.mtx \
         --item-index-table-path /tmp/itm.idx \
         --user-index-table-path /tmp/usr.idx \
         --wait-for-debugger 0 \
      && \
  date

*/

#include <iostream>
#include <string>
#include <algorithm>
#include <boost/program_options.hpp>

using namespace std;
typedef float FLT_T;
typedef int INT_T;
typedef string STRING_T;

#include "ItemItemLearner.hpp"

namespace ProgOpts = boost::program_options;

const char * training_sample_percentage_str =
        "Percentage of data set to be used for training ";
//"remaining percentage will be used for validation ";
const char * max_row_dim_str= "Max dimension for rows in matrix";
const char * max_col_dim_str = "Max dimension for columns in matrix";
const char * input_csv_str = "Path of input csv file to read rating "
        "entries from ";
const char * verbose_mode_level_str = "Show debug info about inner workings ";
const char * loop_mode_count_str = "Run repeatedly in loop mode for same input ";
const char * top_K_neighbours_str = "Top K neighbours to consider ";
const char * recommendation_type_str = "Type of Recommendation "
        "'item-item' or 'user-user' ";
const char * max_thread_count_str = "Maximum number of threads to use " ;
const char * sim_mtx_file_save_path_str = "Path of similarity matrix to save " ;
const char * item_index_table_path_str = "Item's index lookup table path to save ";
const char * user_index_table_path_str = "User's index lookup table path to save ";
const char * wait_for_debugger_str = "Wait for debugger to connect in a while(1) loop ";

volatile bool use_debugger = false;

bool processInputArgs(int argc, char * argv[], ProgOpts::variables_map &varMap,
                      NeighbourHoodRecoParams &params)
{
    try {
        ProgOpts::options_description desc("Allowed Options");

        desc.add_options()
                ("help", "produce help message")
                ("training-sample-percentage", ProgOpts::value<FLT_T>(), training_sample_percentage_str)
                ("max-row-dimension", ProgOpts::value<INT_T>(), max_row_dim_str)
                ("max-column-dimension", ProgOpts::value<INT_T>(), max_col_dim_str)
                ("input-csv-file-path", ProgOpts::value<STRING_T>(), input_csv_str)
                ("verbose-mode-level", ProgOpts::value<INT_T>(), verbose_mode_level_str)
                ("loop-mode-count", ProgOpts::value<INT_T>(), loop_mode_count_str)
                ("top-K-neighbours", ProgOpts::value<INT_T>(), top_K_neighbours_str)
                ("recommendation-type", ProgOpts::value<STRING_T>(),recommendation_type_str)
                ("max-threads-count", ProgOpts::value<INT_T>(), max_thread_count_str)
                ("sim-mtx-file-save-path", ProgOpts::value<STRING_T>(),sim_mtx_file_save_path_str)
                ("item-index-table-path", ProgOpts::value<STRING_T>(), item_index_table_path_str)
                ("user-index-table-path", ProgOpts::value<STRING_T>(), user_index_table_path_str)
                ("wait-for-debugger", ProgOpts::value<INT_T>(), wait_for_debugger_str)
                ; // leave this semi colon at end don't move this

        ProgOpts::store(ProgOpts::parse_command_line(argc, argv, desc), varMap);
        ProgOpts::notify(varMap);

        if (varMap.count("help")) {
            cout << desc << "\n";
            return false;
        }

        if(varMap.count("wait-for-debugger")) {
          // attach to process e.g below
          //  process attach --pid 99130
          // then
          // in lldb do
          // expression use_debugger = false
          use_debugger = varMap["wait-for-debugger"].as<INT_T>() == 0? false : true;
          while(use_debugger) {

          }
        }

        params.training_sample_percentage =
                varMap.count("training-sample-percentage") ?
                varMap["training-sample-percentage"].as<FLT_T>() : 0.70;

        params.max_row_dim = varMap.count("max-row-dimension") ?
                             varMap["max-row-dimension"].as<INT_T>() : 100000000;

        params.max_col_dim = varMap.count("max-column-dimension") ?
                             varMap["max-column-dimension"].as<INT_T>() : 100000000;

        params.csv_input_file_path = varMap.count("input-csv-file-path") ?
                                     varMap["input-csv-file-path"].as<STRING_T>() : STRING_T("unknown_bad.csv");

        params.verbose_mode_level = varMap.count("verbose-mode-level") ?
                                    varMap["verbose-mode-level"].as<INT_T>() : 0;

        params.loop_mode_count = varMap.count("loop-mode-count") ?
                                 varMap["loop-mode-count"].as<INT_T>() : 1;

        params.top_K_neighbours = varMap.count("top-K-neighbours") ?
                                  varMap["top-K-neighbours"].as<INT_T>() : 1;

        params.max_thread_count = varMap.count("max-threads-count") ?
                                  varMap["max-threads-count"].as<INT_T>() : 1;

        params.recommendation_type = varMap.count("recommendation-type") ?
                                     varMap["recommendation-type"].as<STRING_T>() : STRING_T("item-item");

        params.sim_mtx_file_save_path = varMap.count("sim-mtx-file-save-path") ?
                          varMap["sim-mtx-file-save-path"].as<STRING_T>() : STRING_T("/tmp/sim-mtx-file.mtx");

        params.item_index_table_path = varMap.count("item-index-table-path") ?
                  varMap["item-index-table-path"].as<STRING_T>() : STRING_T("/tmp/item-index-table.idx");

        params.user_index_table_path = varMap.count("user-index-table-path") ?
                  varMap["user-index-table-path"].as<STRING_T>() : STRING_T("/tmp/user-index-table.idx");
    }
    catch(exception &e)
    {
        cerr << "\n processInputArgs error: " << e.what() << "\n";
        return false;
    }
    return true;
}

void printHeader() {
    cout << "\n\n\n=================================================\n";
    cout << "============== Finding Neighbours ===============\n";
    cout << "=================================================\n\n";
}

int startRecommender(int argc, char * argv[])
{
    ProgOpts::variables_map vmp; // varmap
    NeighbourHoodRecoParams params;

    if(!processInputArgs(argc, argv, vmp, params))
       return 3;

    printHeader();

    for(int i=0; i<params.loop_mode_count; i++) {
        NeighbourHoodRecommender<AdjustedCosine> recommender(params);

        try {
          bool br = recommender.analyzeInputs();
        } catch (const char * s){
          cout << "Exception:: " << s << "\n";
        }
    }

    return 0;
}

int main(int argc, char * argv[])
{
    return startRecommender(argc, argv);
}
