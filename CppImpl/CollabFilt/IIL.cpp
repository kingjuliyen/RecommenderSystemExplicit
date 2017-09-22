// Author: Senthil Kumar Thangavelu kingjuliyen@gmail.com

/*
g++ IIL.cpp\
  -O3 -o /tmp/iil -lpthread -std=c++11

  date && time /tmp/iil /tmp/startIndex.json && date

 e.g startIndex.json
    {
    "req-type": "startIndex",
    "req-id": "2432",
    "max-threads-count": 80,
    "csv-file-path": "/home/ubuntu/tmp/sf41.csv",
    "sandbox-dir": "/tmp/recos_sandbox/bkdkl"
    }


  date && time /tmp/iil /tmp/getsim.json  && date


e.g getsim.json
    {
        "req-type": "get-similarity",
        "req-id": "2432",
        "max-threads-count": 90,
        "sandbox-dir":  "/tmp/recos_sandbox/bkdkl"
    }
*/

#include <string>

#include "../utils/json/json.h"
#include "../utils/json/jsoncpp.cpp"
#include "../utils/Utils.hpp"
#include "../utils/RatingsStore.hpp"
#include "IIL.hpp"

RecoDriver rd;

void startIndex(Json::Value root)
{
  cout << " startIndex " << endl;

  if(root.isMember("csv-file-path")) {
    string csvpath = root["csv-file-path"].asString();
    string sandboxDir;
    if(root.isMember("sandbox-dir")){
      sandboxDir = root["sandbox-dir"].asString();
    }
    else {
      throw (string(" missing sandbox-dir path in json config file"));
    }

    INT_T threadsCount = 1;
    if(root.isMember("max-threads-count")){
      threadsCount = root["max-threads-count"].asInt();
    }
    rd.createRatingsStore(csvpath, sandboxDir, threadsCount);
    return;
  }
}

void getSimilarity(Json::Value root)
{
  cout << " getSimilarity " << endl;

try{
  string sandboxDir;
  if(root.isMember("sandbox-dir")){
    sandboxDir = root["sandbox-dir"].asString();
  }
  else {
    throw (string(" missing sandbox-dir path in json config file"));
  }

  INT_T threadsCount = 1;
  if(root.isMember("max-threads-count")) {
    threadsCount = root["max-threads-count"].asInt();
  }
  rd.loadRecoSetup(sandboxDir);
  rd.rankSimilarity(sandboxDir, threadsCount);
} catch(string e) {
  cout << e;
}
}

void parseConfig(string cfg)
{
  Json::Value root;
  Json::Reader reader;
  bool parsingSuccessful = reader.parse(cfg.c_str(), root);
  if(!parsingSuccessful) {
    throw(" RecommendationServer_MF::handleMessage !parsingSuccessful");
  }

  if(root.isMember("req-type")) {
    if(root["req-type"].asString() == "startIndex"){
      startIndex(root);
    }
  }

  if(root.isMember("req-type")) {
    if(root["req-type"].asString() == "get-similarity"){
      getSimilarity(root);
    }
  }
}

int main(int argc, char * argv[])
{
  string cfg = readFileasString(argv[1]);
  parseConfig(cfg);
}
