// Author: Senthil Kumar Thangavelu kingjuliyen@gmail.com

#ifndef HIDDENFACTORPREDICTOR_HPP
#define HIDDENFACTORPREDICTOR_HPP

#include <map>
#include <algorithm>
#include <thread>

#include "../utils/Utils.hpp"
#include "../utils/Mtx.hpp"
#include "../utils/UserItemTableHelper.hpp"

typedef struct USR_RANGE_T{
  INT_T firstUserIdx;
  INT_T lastUserIdx;
  USR_RANGE_T(INT_T f, INT_T l) : firstUserIdx(f), lastUserIdx(l) { }
} USR_RANGE_T;

typedef struct RECO_RANK_T {
  INT_T itemId;
  FLT_T predictedRating;

  RECO_RANK_T(INT_T iid, FLT_T rt) : itemId(iid), predictedRating(rt) { }
  bool operator<(const RECO_RANK_T& another) const
  {
    return predictedRating < another.predictedRating;
  }
} RECO_RANK_T;

class HiddenFactorPredictor {
  STRING_T inputFilesPath;
  Mtx *P, *Q;
  INT_T_VEC itemIndex;
  INT_T_VEC userIndex;
  INT_T num_factors;
  map < INT_T, INT_T > realUserToCodedUsrMap;
  map < INT_T, INT_T > realItemsToCodedItemMap;

  template<typename T>
  void loadVector(const char *filePath, vector<T>& vT)
  {
    FILE * fp = fopen(filePath, "r");
    if(!fp)
      throw(" loadVector file not found");
    long long vSz = 0;
    size_t rsz = fread(&vSz, sizeof(vSz), 1, fp);
    for(long long i=0; i < vSz; i++) {
      T v;
      rsz += fread(&v, sizeof(v), 1, fp);
      vT.push_back(v);
    }
    if(rsz != (vSz +1)) {
      vT.clear();
      throw(" loadVector if(rsz != (vSz +1))");
    }
  }

  void printMaps(map<INT_T, INT_T> &m, const char *tag, int count) {
    cout << " printMaps m.size() " << m.size() << " : " << tag << endl;

    for(map<INT_T, INT_T>::iterator it = m.begin(), e = m.end(); it != e; ++it) {
      cout << " real " << tag << it->first << " coded " << tag << it->second << endl;
    }
  }

  void createRealIdMap(INT_T_VEC &v, map<INT_T, INT_T> &m)
  {
    for(INT_T i=0; i<v.size(); i++) {
      m[v[i]] = i;
    }
  }

  void createRealUsersAndItemsMap() {
    realUserToCodedUsrMap = map<INT_T, INT_T> ();
    realItemsToCodedItemMap = map<INT_T, INT_T> ();

    cout << " createRealUsersAndItemsMap 1 " << endl;
    createRealIdMap(userIndex, realUserToCodedUsrMap);
    createRealIdMap(itemIndex, realItemsToCodedItemMap);
  }

  public:
    HiddenFactorPredictor(STRING_T inFPath, INT_T _num_factors) :
      inputFilesPath(inFPath), num_factors(_num_factors), P(0), Q(0)
    {
      cout << " HiddenFactorPredictor(STRING_T inFPath, INT_T _num_factors) " << endl;

      STRING_T Pstr = STRING_T(inputFilesPath+"P_matrix.mtx");
      STRING_T Qstr = STRING_T(inputFilesPath+"Q_matrix.mtx");
      P = new Mtx(Pstr.c_str());
      Q = new Mtx(Qstr.c_str());

      STRING_T itmStr = STRING_T(inputFilesPath+"itm.idx");
      STRING_T usrStr = STRING_T(inputFilesPath+"usr.idx");

      loadVector<INT_T>(itmStr.c_str(), itemIndex);
      loadVector<INT_T>(usrStr.c_str(), userIndex);

      cout << " itmStr.c_str() " << itmStr.c_str() << " itemIndex.size() " << itemIndex.size() << endl;
      cout << " usrStr.c_str() " << usrStr.c_str() << " userIndex.size() " << userIndex.size() << endl;
      createRealUsersAndItemsMap();
    }

    HiddenFactorPredictor(STRING_T inFPath):
      inputFilesPath(inFPath), P(0), Q(0)
    {
      STRING_T itmStr = STRING_T(inputFilesPath+"itm.idx");
      STRING_T usrStr = STRING_T(inputFilesPath+"usr.idx");

      loadVector<INT_T>(itmStr.c_str(), itemIndex);
      loadVector<INT_T>(usrStr.c_str(), userIndex);
      createRealUsersAndItemsMap();
    }

    // make it uncopyable as this obj is shared across threads
    HiddenFactorPredictor(const HiddenFactorPredictor&) = delete;
    HiddenFactorPredictor& operator=(const HiddenFactorPredictor&) = delete;

    ~HiddenFactorPredictor() {
      DELETE(P); DELETE(Q);
    }

    FLT_T getPredictedRating(INT_T u, INT_T i) {
      FLT_T r = 0;
      for (INT_T k=0; k<num_factors; k++){
        r += P->get(u,k) * Q->get(k,i);
      }
      return r;
    }

    STRING_T getRecommendationsString(INT_T realUsrId,
      vector<RECO_RANK_T> &recoList, int numRecommendations)
    {
      INT_T recoListSz = recoList.size() - 1;
      INT_T num = min(numRecommendations, recoListSz);

      std::ostringstream ss;
      ss << "userId:" << realUsrId << " items:";
      for(int i=0; i<=num; i++) {
        ss << recoList[i].itemId << ",";
      }
      return string(ss.str());
    }

    #define RECO_STR_LEN 64

    STRING_T getRecommendationAsString(INT_T realUsrId, INT_T numRecommendations)
    {
      STRING_T recoDir(inputFilesPath+"/recos/");
      std::ostringstream ss;
      ss << realUsrId << "_"
        << realUserToCodedUsrMap[realUsrId] << ".reco.txt";
      STRING_T recoFile(recoDir + string(ss.str()));
      cout << " HiddenFactorPredictor::getRecommendationAsString "
        << recoFile;

      FILE *fp = fopen(recoFile.c_str(), "r");
      if(!fp) {
        cout << " getRecommendationAsString Unable to open file "
          << recoFile << endl;
        return STRING_T("RECO_UNAVAILABLE");
      }

      std::ostringstream recos;

      for(int i=0; i<numRecommendations; i++) {
        char tmpReco[RECO_STR_LEN];
        char * chPtr = fgets(&tmpReco[0], RECO_STR_LEN, fp);
        if(!chPtr){
          break;
        }
        recos << STRING_T(tmpReco, strlen(tmpReco));
      }
      return string(recos.str());
    }

    // recoList contains real item ids
    void saveRecommendationsToFileSystem(INT_T usr,
      vector<RECO_RANK_T> &recoList, STRING_T recoDir,
      INT_T numRecommendationsPerUser)
    {
      std::ostringstream ss;
      ss << userIndex[usr] << "_" << usr << ".reco.txt";
      STRING_T recoFile(recoDir + string(ss.str()));

      FILE *fp = fopen(recoFile.c_str(), "wb");
      if(!fp) {
        cout << " saveRecommendationsToFileSystem Unable to write to file "
          << recoFile << endl;
        return;
      }

      for(INT_T i = 0; i<recoList.size(); i++) {
        std::ostringstream rs; //reco ostringstream
        rs << recoList[i].itemId << "\n";
        STRING_T itm(STRING_T(rs.str()));
        size_t written = fwrite(itm.c_str(), itm.length(), 1, fp);

        if(written <= 0) {
          cout << " saveRecommendationsToFileSystem if(written <= 0) for "
            << recoFile << endl;
          fclose(fp);
          return;
        }
      }

      fclose(fp);
    }

    void saveRecommendationsToDataBase(INT_T usr, vector<RECO_RANK_T> &recoList)
    {

    }

    // minimumRecoCutOff == the minimum rating required to recommend
    // no point recommending low rating items
    void generateRecommendationsForUsrRange(INT_T userFirst, INT_T userLast,
      STRING_T inputFilesDirPath, INT_T numRecommendationsPerUser,
      INT_T minimumRecoCutOff, UserItemTableHelper &uith, INT_T threadIndex)
    {
      cout << " generateRecommendationsForAllUsrRange threadIndex "
        << threadIndex << " started " << endl;
      for(INT_T usr = userFirst; usr <= userLast; usr++) {

        vector<RECO_RANK_T> recoList;
        recoList.clear();
        INT_T maxRec = numRecommendationsPerUser;

        for(INT_T item = 0; item < itemIndex.size(); item++) {
          if(uith.hasUserRatedItem(usr, item) == 1)
            continue;
          FLT_T prt = getPredictedRating(usr, item);
          INT_T realItemId = itemIndex[item];
          if(prt > minimumRecoCutOff) {
            recoList.push_back(RECO_RANK_T(realItemId, prt));
            if(!--maxRec)
              break;
          }
        }

        sort(recoList.begin(), recoList.end());
        reverse(recoList.begin(), recoList.end()); // change to descending

        saveRecommendationsToFileSystem(usr, recoList,
          STRING_T(inputFilesDirPath+"/recos/"), numRecommendationsPerUser);
        saveRecommendationsToDataBase(usr, recoList);
      }
    }

    STRING_T generateRecommendationsForAllUsers(STRING_T inputFilesDirPath,
      STRING_T userItemRatingFile, INT_T numRecommendationsPerUser, FLT_T minimumRecoCutOff,
        INT_T numThreads /* Ensure numThreads is a prime number like 61, 31 etc */)
    {
        cout << "generateRecommendationsForAllUsers ..." << endl;
        INT_T totalUsers = userIndex.size();
        INT_T usersPerThread = totalUsers/numThreads;

        cout << " userIndex.size() " << totalUsers << endl;
        cout << " itemIndex.size() " << itemIndex.size() << endl;
        cout << " numThreads " << numThreads
          << " usersPerThread " << usersPerThread << endl;

        INT_T userFirst = 0;
        INT_T userLast = 0;
        vector<USR_RANGE_T> usrRangesList;

        for(int i=0; i<numThreads; i++) {
          userFirst = i*usersPerThread;
          userLast = userFirst + usersPerThread - 1;

          usrRangesList.push_back(USR_RANGE_T(userFirst, userLast));
        }

        INT_T veryLastUsr = totalUsers - 1;
        userLast++;

        if((veryLastUsr - userLast) >= 0) {
          usrRangesList.push_back(USR_RANGE_T(userLast, veryLastUsr));
        }

        vector<thread> threadList;
        UserItemTableHelper uith(userItemRatingFile);
        uith.prepareTable();

        for(int i=0; i<usrRangesList.size(); i++) {
          USR_RANGE_T &uR = usrRangesList[i];
          userFirst = uR.firstUserIdx;
          userLast = uR.lastUserIdx;
          cout << " thread num " << i << " userFirst " << userFirst
            << " userLast " << userLast << endl;

          threadList.push_back (
            thread(
              &HiddenFactorPredictor::generateRecommendationsForUsrRange,
              this, userFirst, userLast, inputFilesDirPath,
              numRecommendationsPerUser, minimumRecoCutOff, std::ref(uith), i
            )
          );
          std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }

        for(INT_T i=0; i<threadList.size(); i++) {
          threadList[i].join();
        }

        return STRING_T("ALL USERS");
    }

    // This has to be real user id not coded id
    STRING_T generateRecommendationsForUser(INT_T realUsrId, INT_T numRecommendations = 5,
                FLT_T ratingCutoff = 4.0) {
      cout << " generating recommendations for user " << realUsrId << "\n";

      if(realUserToCodedUsrMap.find(realUsrId) == realUserToCodedUsrMap.end()) {
        cout << " generateRecommendationsForUser usr not found " << realUsrId << endl;
        return STRING_T("BAD_USER");
      }
      cout << " inputFilesPath " << inputFilesPath << endl;

      STRING_T userItemRatingsCSV = STRING_T(inputFilesPath + "user-item-ratings.csv");
      cout << " reading user item ratings from " << userItemRatingsCSV << endl;

      UserItemTableHelper uith(userItemRatingsCSV);
      uith.prepareTable();

      INT_T codedUsrId = realUserToCodedUsrMap[realUsrId];
      vector<RECO_RANK_T> recoList;
      recoList.clear();

      for(int i=0; i<itemIndex.size(); i++) {
        if(uith.hasUserRatedItem(codedUsrId, i) == 1)
          continue;

        FLT_T prt = getPredictedRating(codedUsrId, i);
        INT_T realItemId = itemIndex[i];

        // cout << " user " << realUsrId << " has rated item " << realItemId
        //   << " predicted rating " << prt << endl;
        if(prt > ratingCutoff) {
          recoList.push_back(RECO_RANK_T(realItemId, prt));
        }

      }

      sort(recoList.begin(), recoList.end());
      reverse(recoList.begin(), recoList.end()); // change to descending

      return STRING_T(
        getRecommendationsString(realUsrId, recoList, numRecommendations));
      //return STRING_T("SUCCESS");
    }
};

#endif // HIDDENFACTORPREDICTOR_HPP
