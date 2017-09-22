// Author: Senthil Kumar Thangavelu kingjuliyen@gmail.com

/*
  Compile:
  g++  HiddenFactorsLearner.cpp \
       -I/opt/boost/1_61_0/include/						\
       -L/opt/boost/1_61_0/lib						\
       -lboost_program_options \
       -O3 -o /tmp/hidden-factor-learner

  Usage:
  export DYLD_LIBRARY_PATH=/opt/boost/1_61_0/lib:$DYLD_LIBRARY_PATH

  DYLD_LIBRARY_PATH=/opt/boost/1_61_0/lib:$DYLD_LIBRARY_PATH \
  time /tmp/hidden-factor-learner --num-factors 5 \
          --default-rating 2.8485 \
          --regularization-param-p 0.002 \
          --regularization-param-q 0.002 \
          --learning-rate-p 0.000002 \
          --learning-rate-q 0.000002 \
          --training-sample-percentage 0.7 \
          --gradient-descent-iteration-count 100 \
          --max-row-dimension 100000000 \
          --max-column-dimension 100000000 \
          --verbose-mode-level 1 \
          --loop-mode-count 3 \
          --input-csv-file-path /tmp/1000.csv \
          --p-q-matrix-output-file-path /tmp/

e.g csv file (remove header)
usrid  itemid rating
785166 6066 3
521295 4972 3
1665652 11701 5
1963419 15887 3

  Linux:

  LD_LIBRARY_PATH=/opt/boost/1_61_0/lib:$LD_LIBRARY_PATH \
  time /tmp/hidden-factor-learner --num-factors 2 \
          --default-rating 2.8485 \
          --regularization-param-p 0.002 \
          --regularization-param-q 0.002 \
          --learning-rate-p 0.000002 \
          --learning-rate-q 0.000002 \
          --training-sample-percentage 0.7 \
          --gradient-descent-iteration-count 100 \
          --max-row-dimension 100000000 \
          --max-column-dimension 100000000 \
          --verbose-mode-level 1 \
          --loop-mode-count 3 \
          --input-csv-file-path /tmp/sf41.csv \
          --p-q-matrix-output-file-path /tmp/

  Install boost:
  ./bootstrap.sh --prefix=/opt/boost/1_61_0
  ./b2 install
*/



#include <iostream>
#include <string>
#include <boost/program_options.hpp>

#include "HiddenFactorsLearner.hpp"

namespace ProgOpts = boost::program_options;

const char * num_factors_str =
  "Number of hidden factors aka K ";
const char * default_rating_str =
  "Default rating used in the matrix factorization"
  " at start of gradient descent ";
const char * regularization_param_p_str =
  "Regularization parameter for matrix P ";
const char * regularization_param_q_str =
  "Regularization parameter for matrix Q ";
const char * learning_rate_p_str =
  "Learning rate for matrix P parameters in gradient descent ";
const char * learning_rate_q_str =
  "Learning rate for matrix Q parameters in gradient descent ";
const char * gradient_descent_iteration_count_str =
  "Number of times gradient descent iterations need to be run ";
const char * training_sample_percentage_str =
  "Percentage of data set to be used for training ";
  //"remaining percentage will be used for validation ";
const char * max_row_dim_str= "Max dimension for rows in matrix";
const char * max_col_dim_str = "Max dimension for columns in matrix";
const char * input_csv_str = "Path of input csv file to read rating "
  "entries from ";
const char * verbose_mode_level_str = "Show debug info about inner workings ";
const char * loop_mode_count_str = "Run repeatedly in loop mode for same input ";
const char * P_Q_matrix_output_file_path_str = "path to store P and Q matrix output";

bool processInputArgs(int argc, char * argv[], ProgOpts::variables_map &varMap,
        MatrixFactorizationParams &params)
{
  try {
    ProgOpts::options_description desc("Allowed Options");

    desc.add_options()
      ("help", "produce help message")
      ("num-factors", ProgOpts::value<INT_T>(), num_factors_str)
      ("default-rating", ProgOpts::value<FLT_T>(), default_rating_str)
      ("regularization-param-p", ProgOpts::value<FLT_T>(), regularization_param_p_str)
      ("regularization-param-q", ProgOpts::value<FLT_T>(), regularization_param_q_str)
      ("learning-rate-p", ProgOpts::value<FLT_T>(), learning_rate_p_str)
      ("learning-rate-q", ProgOpts::value<FLT_T>(), learning_rate_q_str)
      ("gradient-descent-iteration-count", ProgOpts::value<INT_T>(), gradient_descent_iteration_count_str)
      ("training-sample-percentage", ProgOpts::value<FLT_T>(), training_sample_percentage_str)
      ("max-row-dimension", ProgOpts::value<INT_T>(), max_row_dim_str)
      ("max-column-dimension", ProgOpts::value<INT_T>(), max_col_dim_str)
      ("input-csv-file-path", ProgOpts::value<STRING_T>(), input_csv_str)
      ("verbose-mode-level", ProgOpts::value<INT_T>(), verbose_mode_level_str)
      ("loop-mode-count", ProgOpts::value<INT_T>(), loop_mode_count_str)
      ("p-q-matrix-output-file-path", ProgOpts::value<STRING_T>(), P_Q_matrix_output_file_path_str)
      ; // leave this semi colon at end don't move this

    ProgOpts::store(ProgOpts::parse_command_line(argc, argv, desc), varMap);
    ProgOpts::notify(varMap);

    if (varMap.count("help")) {
      cout << desc << "\n";
      return false;
    }

    OPT(num_factors , "num-factors", INT_T,  5);
    OPT(default_rating ,"default-rating" , FLT_T,  2.8485);
    OPT(regularization_param_p ,"regularization-param-p", FLT_T,  0.002);
    OPT(regularization_param_q ,"regularization-param-q", FLT_T,  0.002);
    OPT(learning_rate_p ,"learning-rate-p", FLT_T,  0.000002);
    OPT(learning_rate_q ,"learning-rate-q", FLT_T,  0.000002);
    OPT(gradient_descent_iteration_count ,"gradient-descent-iteration-count", INT_T,  100);
    OPT(training_sample_percentage ,"training-sample-percentage", FLT_T,  0.70);
    OPT(max_row_dim ,"max-row-dimension", INT_T,  100000000);
    OPT(max_col_dim ,"max-column-dimension", INT_T,  100000000);
    OPT(csv_input_file_path ,"input-csv-file-path", STRING_T,  STRING_T("unknown_bad.csv"));
    OPT(verbose_mode_level ,"verbose-mode-level", INT_T,  0);
    OPT(loop_mode_count ,"loop-mode-count", INT_T,  1);
    OPT(p_q_matrix_output_file_path, "p-q-matrix-output-file-path", STRING_T,  STRING_T("P_Q_matrix.mtx"));

  }
  catch(exception &e)
  {
    cerr << "\n processInputArgs error: " << e.what() << "\n";
    return false;
  }
  return true;
}

void Sleep() {
  cout << "\n\n\n\n\n\nSleeping for 10 seconds \n";
  system("sleep 10");
}

void printHeader() {
  cout << "\n\n\n=================================================\n";
  cout << "=============== START TRAINING ==================\n";
  cout << "=================================================\n\n";
}

int startMatrixFactorization_Trainer(int argc, char * argv[])
{
  ProgOpts::variables_map vmp; // varmap
  MatrixFactorizationParams params;

  bool b = processInputArgs(argc, argv, vmp, params);
  if(!b)
    return 3;

  printHeader();
  FLT_T rmseSum = 0;
  FLT_T iterationCount = 0;

  for(int i=0; i<params.loop_mode_count; i++) {
    MatrixFactorization trainer(params);
    if(!trainer.startTraining()) {
      cout << "Training failed check input files and training params\n";
      return 4;
    }
    cout << "Trial " << i << " finalRMSE " << trainer.getFinalRMSE()
      << " num iterations " << trainer.getNumIterations() << "\n";
    rmseSum += trainer.getFinalRMSE();
    iterationCount += 1;
    trainer.printPredictedValues(10);
  }
  cout << " avg RMSE " << (rmseSum/iterationCount)
    << " found using "<< iterationCount << " trials \n";
  return 0;
}

int main(int argc, char * argv[])
{
  return startMatrixFactorization_Trainer(argc, argv);
}
