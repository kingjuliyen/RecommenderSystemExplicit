#ifndef UTILS_HPP
#define UTILS_HPP

#include <iostream>
#include <vector>
#include <limits>
#include <ctime>
#include <algorithm>

using namespace std;

typedef float FLT_T;
typedef int INT_T;
typedef string STRING_T;

#define TIME_POINT std::chrono::steady_clock::time_point
#define NOW std::chrono::steady_clock::now



#define BAD_ARGS_ERR 32561
#define SYSTEM_CMD_FAIL (BAD_ARGS_ERR+1)
#define DB_CONN_OPEN_ERR (SYSTEM_CMD_FAIL+1)

#define DELETE_AR(x) \
{ \
  if(x) { \
    delete[] x; \
    x = 0; \
  } \
}

#define DELETE(x) \
{ \
  if(x) { \
    delete x; \
    x = 0; \
  } \
}

template <typename T>
void printVector(vector<T> vt)
{
  for(int i=0; i<vt.size(); i++) {
    cout << " " << vt[i];
  }
  cout << "\n";
}

template <typename T>
void printVector(vector<T> vt, long long num)
{
  if(num>vt.size())
    throw("num>vt.size()");

  for(int i=0; i<num; i++) {
    cout << " " << vt[i];
  }
  cout << "\n";
}

inline FLT_T MIN_INVAID_RATING() {
  return numeric_limits<FLT_T>::min();
}

inline FLT_T FLT_T_MIN() {
  return numeric_limits<FLT_T>::min();
}

inline FLT_T FLT_T_MAX() {
  return numeric_limits<FLT_T>::max();
}

inline INT_T INT_T_MIN() {
  return numeric_limits<INT_T>::min();
}

inline INT_T INT_T_MAX() {
  return numeric_limits<INT_T>::max();
}

typedef vector<INT_T> INT_T_VEC;

inline int newRandomInt (int i) { return std::rand()%i; }

inline INT_T_VEC getRandomShuffledIndexes(INT_T indexRangeMaxValue, INT_T startIndex,
  INT_T numRandomIndexesRequired) {
  std::srand(std::time(0));

  INT_T_VEC iv;
  for(INT_T i = startIndex; i < startIndex + indexRangeMaxValue; i++) {
    iv.push_back(i);
  }
  random_shuffle(iv.begin(), iv.end(), newRandomInt);

  INT_T_VEC riv;
  for(INT_T ri=0; ri<numRandomIndexesRequired; ri++) {
    riv.push_back(iv[ri]);
  }
  return riv;
}

#define OPT(a, b, c, d) \
  params.a = varMap.count( b ) ? varMap[ b ].as< c > () : d

#define STRPTR(x, y) \
  const char * x = y


// random generator function:
inline int getRandomInt (int i) { return rand()%i; }

#include <string>
#include <fstream>
#include <streambuf>

inline STRING_T readFileasString(const char * filePath) {
  ifstream t(filePath);
  return string((istreambuf_iterator<char>(t)), istreambuf_iterator<char>());
}

inline STRING_T getRecosConfigFileDirPath() {
  return string("/opt/valuemeda/ai-products/RecommenderSystem/config-files");
}

inline STRING_T getRecosSandBoxDirPath() {
  return string("/opt/valuemeda/recos_sandbox");
}

inline void execShellCommand(STRING_T cmd) {
  cout << " execShellCommand : \"" << cmd  << "\"" << endl;

  int status = system(cmd.c_str());
  if (status < 0) {
    std::cout << "Error: " << strerror(errno) << '\n';
  }
  else {
    if (WIFEXITED(status)) {
      cout << " Program returned normally, exit code " << WEXITSTATUS(status) << '\n';
    }
    else {
      cout << " command \"" << cmd << "\" failed" << endl;
      throw SYSTEM_CMD_FAIL;
    }
  }
}

inline void execShellCommandsBatch(vector<STRING_T> cmd)
{
  for(int i=0; i<cmd.size(); i++) {
    cout << cmd[i] << endl;
    execShellCommand(cmd[i]);
  }
}

inline void mkdir(string dir)
{
  string cmd;
  cmd = " rm -rf " + dir;
  execShellCommand(cmd);
  cmd = " mkdir -p " + dir;
  execShellCommand(cmd);
}

#define START_TIME_STAMP0(w, x, y) \
  const char * w = x; \
  std::chrono::steady_clock::time_point y = std::chrono::steady_clock::now();

#define END_TIME_STAMP0(w, x, y) \
  std::chrono::steady_clock::time_point x = std::chrono::steady_clock::now(); \
      cout << " " << w << " took " \
        << std::chrono::duration_cast<std::chrono::milliseconds>(x - y).count() \
        << " milli seconds \n";

#define START_TIME_STAMP(x) \
  const char * ____tsStr = x; \
  std::chrono::steady_clock::time_point ts_start = std::chrono::steady_clock::now();

#define END_TIME_STAMP \
  std::chrono::steady_clock::time_point ts_end = std::chrono::steady_clock::now(); \
      cout << " " << ____tsStr << " took " \
        << std::chrono::duration_cast<std::chrono::milliseconds>(ts_end - ts_start).count() \
        << " milli seconds \n";

#define END_TIME_STAMP_NOPRINT \
  std::chrono::steady_clock::time_point ts_end = std::chrono::steady_clock::now();

#define RECOS_SANDBOX_BASE "s3://mindzaurus.recos.sandbox"






#endif // UTILS_HPP
