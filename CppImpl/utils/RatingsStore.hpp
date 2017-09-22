#ifndef INDEXER_HPP
#define INDEXER_HPP

#include <map>
#include <thread>
#include <mutex>
#include <algorithm>
#include "Utils.hpp"

typedef struct Rating_T {
  INT_T uid;
  FLT_T rating;
  Rating_T(INT_T u, INT_T r) : uid(u), rating(r) { }
  Rating_T(INT_T u) : uid(u) { }
  bool operator<(const Rating_T& another) const { return uid < another.uid; }
  bool operator()(const Rating_T& rt) const { return rt.uid == uid; }
} Rating_T;
typedef vector<Rating_T> RatingVector;

bool operator == (const Rating_T &r1, const Rating_T &r2) { return r1.uid == r2.uid; }

class RatingsStore {
  string ratingsCSV;
  string indexFileDir; // output files dir

  INT_T numUniqUsrs;
  INT_T numUniqItms;
  INT_T numThreads;

  vector <INT_T> uidList, iidList;
  vector<FLT_T> itemAvgRating;
  vector<RatingVector *> itemVectorCache;
  vector<RatingVector> rtVec;

  string outFilesDir()
  {
    return indexFileDir;
  }

  string usrIDXFile()
  {
    return "USR.idx";
  }

  string itmIdxFile()
  {
    return "ITM.idx";
  }

  string getIdxDir()
  {
    return outFilesDir() +"/idx/";
  }

  string itemVectorsDir()
  {
    return outFilesDir() +"/" + "item-vectors";
  }

  string usrIdxPath()
  {
    return  getIdxDir() + usrIDXFile();
  }

  string itmIdxPath()
  {
    return getIdxDir() + itmIdxFile();
  }

  void createOutpuFileDirs() {
    mkdir(outFilesDir());
    mkdir(itemVectorsDir());
    mkdir(getIdxDir());
  }

  void fill(char *s, int LEN) {
    for(int i=0; i<=LEN; i++) {
      if(s[i] == '\0') {
        s[i] = ' ';
      }
    }
  }

  bool checkMapped(map<string, INT_T> &m, string key, INT_T value)
  {
    if(m.find(key) == m.end()) {
      m[key] = value;
      return false;
    }
    return true;
  }

  INT_T getIndex(map<string, INT_T> m, string key)
  {
    auto it = m.find(key);
    if(it != m.end()) {
      return it->second;
    }
    throw (string("getIndex unreachable"));
  }

  string getAvgRatingsPath()
  {
    string fileName = getIdxDir() + "/" + "avg-ratings";
    return fileName;
  }

  void loadItmAvgRating()
  {
    string fileName = getAvgRatingsPath();
    itemAvgRating.clear();
    FILE * fp = fopen(fileName.c_str(), "rb");
    INT_T sz = itemAvgRating.size();
    INT_T read = fread(&sz, sizeof(sz), 1, fp);

    for(int i=0; i<sz; i++) {
      FLT_T avg;
      read = fread(&avg, sizeof(FLT_T), 1, fp);
      itemAvgRating.push_back(avg);
      // cout << " item " << i << " avg rating " << avg << endl;
    }
    fclose(fp);
  }

  void writeItemAvgRating()
  {
  START_TIME_STAMP("writeItemAvgRating");

    string fileName = getAvgRatingsPath();
    FILE * fp = fopen(fileName.c_str(), "wb");
    INT_T sz = itemAvgRating.size();
    fwrite(&sz, sizeof(sz), 1, fp);

    for(int i=0; i<sz; i++) {
      FLT_T avg = itemAvgRating[i];
      fwrite(&avg, sizeof(FLT_T), 1, fp);
      // cout << " item " << i << " avg rating " << avg << endl;
    }
    fclose(fp);

  END_TIME_STAMP;
  }

  mutex print_mutex;
  void thread_safeprint(STRING_T s) {
    print_mutex.lock();
    cout << s << endl;
    print_mutex.unlock();
  }

  // also returns average of ratings
  FLT_T saveItmVector(RatingVector &rv, string fileName, INT_T threadIndex)
  {
    // string d0 = " writing " + fileName +" threadIndex " + to_string(threadIndex);
    // thread_safeprint(d0);

    FLT_T sum = 0;
    sort(rv.begin(), rv.end());

    FILE * fp = fopen(fileName.c_str(), "wb");
    INT_T sz = rv.size();
    fwrite(&sz, sizeof(sz), 1, fp);

    for(int i=0; i<sz; i++) {
      Rating_T rt = rv[i];
      fwrite(&rt, sizeof(Rating_T), 1, fp);
      sum += rt.rating;
    }
    fclose(fp);
    // cout << " sum " << sum << " rv.size " << rv.size()
    //   << " avg " << sum/rv.size();
    return sum/rv.size();
  }

  void writeItmVector(INT_T start, INT_T end, INT_T threadIndex)
  {
    // cout << " writeItmVector threadIndex " << threadIndex
    //   << " , start " << start << " , end " << end << endl;
    for(INT_T i=start; i<=end; i++) {
      string path = itemVectorsDir() + "/"+ to_string(i);
      RatingVector &rv = rtVec[i];
      itemAvgRating[i] = saveItmVector(rv, path, threadIndex);
    }
  }

  void writeItemVectors(INT_T threadCount = 3)
  {
  START_TIME_STAMP("writeItemVectors")

    cout << " rtVec.size() " << rtVec.size() << endl;
    itemAvgRating = vector <FLT_T> (numUniqItms, FLT_T_MIN());
    INT_T itemComboIndexstart = -1 , itemComboIndexend = -1;
    INT_T comboSz = rtVec.size()/threadCount;
    vector<thread> threadList;

    if(threadCount > numUniqItms) {
      string err = string(" writeItemVectors:: reduce "
        "thread count lesser than " + to_string(numUniqItms));
      throw err;
    }

    for(INT_T i=0; i<threadCount; i++) {
      INT_T start = i*comboSz;
      INT_T end = (start + comboSz);
      // cout << " start " << start << " end " << end << "\n";
      itemComboIndexstart = start ;
      itemComboIndexend = end;

      if(i == threadCount-1){
        itemComboIndexend = rtVec.size() - 1;
      } else {
        itemComboIndexend -= 1;
      }

      // cout << " threadIndex " << i << " itemComboIndexstart " << itemComboIndexstart
      //   << " itemComboIndexend " << itemComboIndexend << endl;
      
      threadList.push_back(thread(&RatingsStore::writeItmVector, this,
        itemComboIndexstart, itemComboIndexend, i));

      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    } 

    for(INT_T i=0; i<threadList.size(); i++) {
      threadList[i].join();
    }

  END_TIME_STAMP;
  }

  map<INT_T, INT_T> uMap;
  map<INT_T, INT_T> iMap;

  bool updateCount(map<INT_T, INT_T> &m, INT_T key, INT_T value,
    vector<INT_T> &vi)
  {
    // cout << " value " << value << endl;
    auto it = m.find(key);
    if(it == m.end()) {
      m[key] = value;
      vi.push_back(key);
      return true;
    }
    return false;
  }

  INT_T getStrictMappedID(map<INT_T, INT_T> &m, INT_T key)
  {
    auto it = m.find(key);
    if( it != m.end()) {
      return it->second;
    }
    throw (" getStrictMappedID guaranteed key missing ");
    return INT_T_MIN();
  }

  INT_T getStrictMappedUid(INT_T u) {
    return getStrictMappedID(uMap, u);
  }

  INT_T getStrictMappedIid(INT_T i) {
    return getStrictMappedID(iMap, i);
  }

  void readCSV_pass2()
  {
  START_TIME_STAMP("readCSV_pass2");
    cout << " readCSV_pass2 " << endl;

    rtVec = vector<RatingVector>(numUniqItms);

    INT_T usr, itm, usrCount = 0, itmCount = 0;
    FLT_T rating;

    FILE *fp = fopen(ratingsCSV.c_str(), "rb");
    if(!fp) {
      throw (string(" Unable to open file " + ratingsCSV));
    }

    int count = 0;
    while(true) {
      int r = fscanf(fp,"%d %d %f ",&usr, &itm, &rating);
      if(r<0) {
        break;
      }
      INT_T uid = getStrictMappedUid(usr);
      INT_T iid = getStrictMappedIid(itm);

      // cout << " uid " << uid << " usr " << usr << endl;
      // cout << " iid " << iid << " itm " << itm << endl;

      RatingVector &rv = rtVec[iid];
      rv.push_back(Rating_T(uid, rating));
    }

  END_TIME_STAMP;
  }

  void readCSV_pass1()
  {
  START_TIME_STAMP("readCSV_pass1");
    cout << " readCSV_pass1 " << endl;

    INT_T usr, itm, usrCount = 0, itmCount = 0;
    FLT_T rating;
    INT_T maxUsrLen = 0, maxItmLen = 0;

    FILE *fp = fopen(ratingsCSV.c_str(), "rb");
    if(!fp) {
      throw (string(" Unable to open file " + ratingsCSV));
    }

    int count = 0;
    while(true) {
      int r = fscanf(fp,"%d %d %f ",&usr, &itm, &rating);
      if(r<0) {
        break;
      }
      if(updateCount(uMap, usr, usrCount, uidList)){
        usrCount++;
      }
      if(updateCount(iMap, itm, itmCount, iidList)){
        itmCount++;
      }
      count++;
    }

    cout << " Total entries " << count
      << " uMap.size() " << uMap.size()
      << " iMap.size() " << iMap.size() << endl;

    numUniqUsrs = uMap.size();
    numUniqItms = iMap.size();

  END_TIME_STAMP;
  }

  void writeIndexFile(vector<INT_T> &vs, string fileName) {
    cout << " writeIndexFile " << endl;
    FILE * fp = fopen(fileName.c_str(), "wb");
    if(!fp) {
      throw (string(" writeIndexFile Unable to open file " + fileName));
    }

    INT_T sz = vs.size();
    fwrite(&sz, sizeof(sz), 1, fp);

    for(INT_T i=0; i< sz; i++) {
      INT_T val = vs[i];
      fwrite(&val, sizeof(val), 1, fp);
    }

    fclose(fp);
  }

  void writeIndexFiles()
  {
  START_TIME_STAMP("readCSV");
    writeIndexFile(uidList, usrIdxPath());
    writeIndexFile(iidList, itmIdxPath());
  END_TIME_STAMP;
  }

  void readIndexFile(vector<INT_T> &vs, string fileName,
    map<INT_T, INT_T> &mp)
  {
    cout << " reading " << fileName << endl;
    FILE * fp = fopen(fileName.c_str(), "rb");
    if(!fp) {
      throw (string(" readIndexFile Unable to open file " + fileName));
    }
    INT_T sz = vs.size();
    INT_T read = fread(&sz, sizeof(sz), 1, fp);

    for(INT_T i=0; i< sz; i++) {
      INT_T val;
      read = fread(&val, sizeof(val), 1, fp);
      vs.push_back(val);
      mp[val] = i;
      // cout << " str \'" << str << "\'" << endl;
    }

    fclose(fp);
  }

  INT_T invalidId() {
    return INT_T_MIN();
  }

  bool isInvalidID(INT_T x) { return x == invalidId();}

  INT_T getCodedID(map<string, INT_T> &mp, string id)
  {
    auto it = mp.find(id);
    return (it == mp.end()) ? invalidId() : it->second;
  }

  void readIndexFiles()
  {
  START_TIME_STAMP("readIndexFiles");
    readIndexFile(uidList, usrIdxPath(), uMap);
    readIndexFile(iidList, itmIdxPath(), iMap);
    cout << " uidList.size() " << uidList.size() << endl;
    cout << " iidList.size() " << iidList.size() << endl;
  END_TIME_STAMP;
  }

  RatingVector * loadRatingVector(INT_T itemID) {
    string path = itemVectorsDir() + "/"+ to_string(itemID);
    //cout << " loadRatingVector reading " << path << endl;
    FILE * fp = fopen(path.c_str(), "rb");
    RatingVector * rvp = new RatingVector();

    INT_T sz = -1;
    int read = fread(&sz, sizeof(sz), 1, fp);

    for(INT_T i=0; i<sz; i++) {
      Rating_T rt(INT_T_MIN(), FLT_T_MIN());
      read = fread(&rt, sizeof(Rating_T), 1, fp);
      rvp->push_back(rt);
    }
    fclose(fp);
    return rvp;
  }

  mutex itemVectorCacheLock;

  RatingVector * getCachedItmVector(INT_T itemID) {
    auto rv = itemVectorCache[itemID];
    if(rv)
      return rv;
    rv = loadRatingVector(itemID);

    itemVectorCacheLock.lock();
    if(itemVectorCache[itemID]){
      rv->clear();
      delete rv;
    }
    else {
      itemVectorCache[itemID] = rv;
    }
    itemVectorCacheLock.unlock();
    return itemVectorCache[itemID];
  }

  public:
  RatingsStore(string _ratingsCSV, string _indexFileDir, INT_T _numThreads) :
    ratingsCSV(_ratingsCSV), indexFileDir(_indexFileDir),
    numThreads(_numThreads)
  {
    createOutpuFileDirs();

    readCSV_pass1();
    readCSV_pass2();
    writeIndexFiles();
    writeItemVectors(numThreads);
    writeItemAvgRating();
  }

  RatingsStore(string _indexFileDir):
    indexFileDir(_indexFileDir)
  {
    readIndexFiles();
    initVars();
    loadItmAvgRating();
  }

  RatingVector& getRatingVectorForItem(INT_T i) {
    return *getCachedItmVector(i);
  }

  FLT_T hasUsrRatedItem(INT_T u, INT_T i) {
    RatingVector* rv = getCachedItmVector(i);
    auto it = find(rv->begin(), rv->end(), Rating_T(u, 0));
    if(it!=rv->end()) {
      return it->rating;
    }
    return FLT_T_MIN();
  }

  void initVars()
  {
    itemVectorCache = vector<RatingVector *> (iidList.size(), 0);
    itemAvgRating = vector<FLT_T> (iidList.size(), FLT_T_MIN());
  }

  INT_T getNumItems() {
    //cout << " getNumItems() " << iidList.size() << endl;
    return iidList.size();
  }

  FLT_T getAvgRating(INT_T itm)
  {
    return itemAvgRating[itm];
  }

  FLT_T getRatingForCodedUsrID(INT_T u, INT_T i)
  {
    RatingVector* rv = getCachedItmVector(i);
    auto it = find(rv->begin(), rv->end(), Rating_T(u, 0));
    if(it != rv->end())
      return it->rating;
    return FLT_T_MIN();
  }

  void loadAllRatingVector()
  {
  START_TIME_STAMP("loadAllRatingVector")
    // cout << " loadAllRatingVector -> getNumItems() " << getNumItems() << endl;
    for(INT_T i=0; i< getNumItems(); i++) {
      //cout << " loading item " << i << endl;
      getCachedItmVector(i);
    }
  END_TIME_STAMP;
  }

  void printRealIDS(INT_T codedUsrID, INT_T codedItmID) 
  {
    INT_T uncodedUsrID = uidList[codedUsrID];
    INT_T uncodedItmID = iidList[codedItmID];
    FLT_T r = getRatingForCodedUsrID(codedUsrID, codedItmID);
    cout << " uncodedUsrID " << uncodedUsrID
      << " uncodedItmID " << uncodedItmID << " rating " << r << endl;
  }
};

#endif
