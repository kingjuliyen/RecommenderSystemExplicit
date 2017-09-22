// Author: Senthil Kumar Thangavelu kingjuliyen@gmail.com

#ifndef ITEMITEM_PREDICTOR_HPP
#define ITEMITEM_PREDICTOR_HPP

#include <thread>
#include <ctime>
#include <chrono>

#include "../utils/Utils.hpp"
#include "../utils/Mtx.hpp"
#include "../utils/UserItemTableHelper.hpp"

typedef struct USR_RANGE_T{
  INT_T firstUserIdx;
  INT_T lastUserIdx;
  USR_RANGE_T(INT_T f, INT_T l) : firstUserIdx(f), lastUserIdx(l) { }
} USR_RANGE_T;

typedef struct SIM_RANK_T {
  INT_T itemId;
  FLT_T similarity;

  SIM_RANK_T(INT_T iid, FLT_T sim) : itemId(iid), similarity(sim) { }
  bool operator<(const SIM_RANK_T& another) const { return similarity < another.similarity; }
  void print() { cout << " item: "<< itemId << " similarity: " << similarity << "\n"; }
  void printSim() { cout << " " << similarity ;
  }
} SIM_RANK_T;

typedef struct RECO_RANK_T {
  INT_T itemId;
  FLT_T predictedRating;

  RECO_RANK_T(INT_T iid, FLT_T rt) : itemId(iid), predictedRating(rt) { }
  bool operator<(const RECO_RANK_T& another) const
  {
    return predictedRating < another.predictedRating;
  }
} RECO_RANK_T;

typedef vector<Rating_T> RatingVector;

struct ItemItemPredictorParams {
  INT_T top_K_neighbours;
  INT_T verbose_mode_level;
  INT_T loop_mode_count;
  INT_T max_threads_count;
  INT_T num_recommendations;
  FLT_T training_sample_percentage;
  FLT_T similarity_cutoff_value;

  STRING_T csv_input_file_path;
  STRING_T sim_mtx_file_path;
  STRING_T item_index_table_path;
  STRING_T user_index_table_path;
  STRING_T recos_dir;
};

class ItemItemPredictor {
  ItemItemPredictorParams params;
  Mtx *similarityTable;
  INT_T_VEC itemIndex;
  INT_T_VEC userIndex;
  INT_T_VEC itemReverseIndex;
  INT_T_VEC userReverseIndex;
  vector<RatingVector> itemRV; // itembased rating vector
  vector < map<INT_T, FLT_T> > itmUsrMap; // item user
  FLT_T crossValidRMSE;

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

  FLT_T quickPredictRating(INT_T usr, vector<SIM_RANK_T> &predictList)
  {
    FLT_T numerator = 0, denominator = 0;

    for(int i=0; i < predictList.size(); i++) {
      SIM_RANK_T &sr = predictList[i];
      INT_T curItm = sr.itemId;
      FLT_T curSim = sr.similarity;
      FLT_T curRating = getMappedRating(usr, curItm, NAN);

      if(isnan(curRating))
        continue;

      numerator += curSim * curRating;
      denominator += curSim;
    }
    FLT_T rt = numerator / denominator;
    return isnan(rt) ? MIN_INVAID_RATING() : rt;
  }

  FLT_T quickPredict(INT_T usr, INT_T itm)
  {
    INT_T K = params.top_K_neighbours;
    vector<SIM_RANK_T> simRanks, predictList;

    simRanks.clear();
    getNeighbours(itm, params.similarity_cutoff_value, simRanks);

    for(int i=0; i<simRanks.size(); i++) {
      SIM_RANK_T &sr = simRanks[i];
      // if(isnan(getRating(usr, sr.itemId)))
      //   continue;
      predictList.push_back(SIM_RANK_T(sr.itemId, sr.similarity));
      if(!K--)
        break;
    }
    return quickPredictRating(usr, predictList);
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

  void generateRecommendationsForUsrRange(INT_T userFirst, INT_T userLast,
    STRING_T recoDirPath, INT_T numRecommendationsPerUser,
    INT_T minimumRecoCutOff, UserItemTableHelper &uith, INT_T threadIndex, bool useFirstBestFit)
  {
    cout << " generateRecommendationsForAllUsrRange threadIndex "
      << threadIndex << " started " << endl;
    for(INT_T usr = userFirst; usr <= userLast; usr++) {
      std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

      vector<RECO_RANK_T> recoList;
      recoList.clear();
      INT_T maxRec = numRecommendationsPerUser;

      for(INT_T item = 0; item < itemIndex.size(); item++) {
        if(uith.hasUserRatedItem(usr, item) == 1)
          continue;

        FLT_T prt = quickPredict(usr, item);
        INT_T realItemId = itemIndex[item];

        if(prt > minimumRecoCutOff) {
          recoList.push_back(RECO_RANK_T(realItemId, prt));
          if(!useFirstBestFit)
            continue;
          if(!--maxRec)
            break;
          }
      } // for(INT_T item = 0; item < itemIndex.size(); item++)

      sort(recoList.begin(), recoList.end());
      reverse(recoList.begin(), recoList.end()); // change to descending

      saveRecommendationsToFileSystem(usr, recoList,
        STRING_T(recoDirPath), numRecommendationsPerUser);

      std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
      cout << "\n Recommendation for " << usr << " took "
        << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
        << " milli seconds \n";
    } // for(INT_T usr = userFirst; usr <= userLast; usr++)
  }

  STRING_T generateRecommendationsForAllUsersThreaded(STRING_T recoDirPath,
    INT_T numRecommendationsPerUser, INT_T numThreads)
  {
    cout << " generateRecommendationsForAllUsers ..." << endl;
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
    UserItemTableHelper uith(params.csv_input_file_path);
    uith.prepareTable();

    for(int i=0; i<usrRangesList.size(); i++) {
      USR_RANGE_T &uR = usrRangesList[i];
      userFirst = uR.firstUserIdx;
      userLast = uR.lastUserIdx;
      cout << " thread num " << i << " userFirst " << userFirst
        << " userLast " << userLast << endl;

      threadList.push_back (
        thread(
          &ItemItemPredictor::generateRecommendationsForUsrRange,
          this, userFirst, userLast, recoDirPath,
          numRecommendationsPerUser, params.similarity_cutoff_value,  std::ref(uith), i, true
        )
      );
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    for(INT_T i=0; i<threadList.size(); i++) {
      threadList[i].join();
    }

    return STRING_T("ALL USERS");
  }

public:
  ItemItemPredictor(ItemItemPredictorParams &_params): params(_params)
  {
  }

  ~ItemItemPredictor() {
    DELETE(similarityTable);
  }

  void createReverseIndex(INT_T_VEC &vi, INT_T_VEC &revi)
  {
    for(INT_T i=0; i<vi.size(); i++) {
      INT_T id = vi[i];
      revi[id] = i;
    }
  }

  void createReverseLookupIndexes()
  {
    INT_T maxItemId = itemIndex[itemIndex.size() - 1];
    INT_T maxUserId = userIndex[userIndex.size() - 1];

    itemReverseIndex = INT_T_VEC(maxItemId + 1);
    userReverseIndex = INT_T_VEC(maxUserId + 1);

    createReverseIndex(itemIndex, itemReverseIndex);
    createReverseIndex(userIndex, userReverseIndex);
  }

  void loadValuesFromFileSystem()
  {
    cout << " ItemItemPredictor::loadSimilarityMatrixFromFileSystem from "
          << params.sim_mtx_file_path << "\n";
    similarityTable = new Mtx(params.sim_mtx_file_path.c_str());

    loadVector<INT_T>(params.item_index_table_path.c_str(), itemIndex);
    loadVector<INT_T>(params.user_index_table_path.c_str(), userIndex);
  }

  void readInputCSV()
  {
      INT_T userid, itemid, rating, rc = 0;
      FILE * csvFile = fopen (params.csv_input_file_path.c_str(), "r");

      if(!csvFile) {
        cout << " ERROR2: Unable to open input file check \"" << params.csv_input_file_path
             << "\" check --input-csv-file-path arg"<< "\n";
        throw(" Unable to open ratings csv input file");
      }

      while(true) {
          int r = fscanf(csvFile,"%d %d %d",&userid, &itemid, &rating);
          if(r<0) {
              break;
          }
          INT_T iid = itemReverseIndex[itemid];
          INT_T uid = userReverseIndex[userid];

          map<INT_T, FLT_T> &iuMap = itmUsrMap[iid];
          iuMap[uid] = rating;

          rc++;
          if((rc%5000000) == 0) {
            cout << " " << rc << " entries mapped " << endl;
          }
      }
      cout << " total entries read  " << rc << "\n";
      fclose(csvFile);
  }

  // rating done by the user
  // use coded ids for user and item ids
  FLT_T getRating(INT_T uid, INT_T iid)
  {
    RatingVector &rv = itemRV[iid];
    for(int i=0; i < rv.size(); i++) {
      if(rv[i].uId == uid)
        return rv[i].rtng;
    }
    return NAN;
  }

  // rating done by the user
  // use coded ids for user and item ids
  FLT_T getMappedRating(INT_T uid, INT_T iid, const FLT_T unrated = NAN)
  {
    map<INT_T, FLT_T> &uim = itmUsrMap[iid];
    auto uimIt = uim.find(uid);

    if(uimIt != uim.end())
      return uimIt->second;

    return unrated;
  }

  void populateRatings()
  {
    itmUsrMap = vector < map<INT_T, FLT_T> > (itemIndex.size());
    for(int i=0; i<itmUsrMap.size(); i++) {
      itmUsrMap[i] = map<INT_T, FLT_T>();
    }

    itemRV = vector<RatingVector>(itemIndex.size());

    readInputCSV();
  }

  // Take real userID and return internal userID used
  INT_T getCodedUserID(INT_T uid)
  {
    INT_T cuid = userReverseIndex[uid];
    return cuid;
  }

  INT_T getUncodedUserId(INT_T _uid)
  {
    INT_T uid = userIndex[_uid];
    return uid;
  }

  // Take real userID and return internal userID used
  INT_T getCodedItemID(INT_T iid)
  {
    INT_T ciid = itemReverseIndex[iid];
    return ciid;
  }

  INT_T getUncodedItemId(INT_T _iid)
  {
    INT_T iid = itemIndex[_iid];
    return iid;
  }

  void getNeighbours (const INT_T item, const FLT_T similarityCutoff,
                  vector<SIM_RANK_T> &simRanks )
  {
    for(INT_T c=0; c < similarityTable->cols; c++) {
      if(c == item)
        continue;

      FLT_T sim = similarityTable->get(item, c);
      if(isnan(sim))
        continue;

      if(sim > similarityCutoff) {
          simRanks.push_back(SIM_RANK_T(c, sim));
      }
    }

    sort(simRanks.begin(), simRanks.end());
    reverse(simRanks.begin(), simRanks.end()); // change to descending
  }

  FLT_T predictRating(INT_T usr, vector<SIM_RANK_T> &predictList)
  {
    FLT_T numerator = 0, denominator = 0;

    for(int i=0; i < predictList.size(); i++) {
      SIM_RANK_T &sr = predictList[i];
      INT_T curItm = sr.itemId;
      FLT_T curSim = sr.similarity;
      FLT_T curRating = getRating(usr, curItm);
      if(isnan(curRating))
        continue;
      numerator += curSim * curRating;
      denominator += curSim;
    }
    FLT_T rt = numerator / denominator;
    return isnan(rt) ? MIN_INVAID_RATING() : rt;
  }

  FLT_T predict(INT_T usr, INT_T itm)
  {
    INT_T K = params.top_K_neighbours;
    vector<SIM_RANK_T> simRanks, predictList;

    simRanks.clear();
    getNeighbours(itm, params.similarity_cutoff_value, simRanks);

    for(int i=0; i<simRanks.size(); i++) {
      SIM_RANK_T &sr = simRanks[i];
      // if(isnan(getRating(usr, sr.itemId)))
      //   continue;
      predictList.push_back(SIM_RANK_T(sr.itemId, sr.similarity));
      if(!K--)
        break;
    }
    return predictRating(usr, predictList);
  }

  // returns coded indexes for items
  INT_T_VEC
  recommendProducts(INT_T user, INT_T numItems)
  {
    INT_T_VEC vItems;
    vector<RECO_RANK_T> recoList;
    for(INT_T curItm = 0; curItm < similarityTable->cols; curItm++) {
      if(!isnan(getRating(user, curItm)))
        continue;
      FLT_T rt = predict(user, curItm);
      recoList.push_back(RECO_RANK_T(curItm, rt));
    }
    sort(recoList.begin(), recoList.end());
    reverse(recoList.begin(), recoList.end()); // change to descending
    cout << " ItemItemPredictor::recommendProducts:: considered "
      << recoList.size() << " comparisons\n";

    for(INT_T i=0; i<recoList.size(); i++) {
      vItems.push_back(recoList[i].itemId);
      if(!numItems--)
        break;
    }
    return vItems;
  }

  void prepareForPrediction()
  {
    cout << " ItemItemPredictor::prepareForPrediction started\n";
    cout << " about to loadValuesFromFileSystem \n";
    loadValuesFromFileSystem();
    cout << " about to createReverseLookupIndexes \n";
    createReverseLookupIndexes();
    cout << " about to createReverseLookupIndexes \n";
    populateRatings();
  }

#if 0
  FLT_T checkCrossValidation(INT_T numSamples = 10000,
    const char *path = "/tmp/testSetRatingsList.RatingEntry") {
      crossValidRMSE = 235;

      FILE * fp = fopen(path, "rb");
      if(!fp) {
        throw ("ItemItemPredictor::checkCrossValidation !fp");
      }

      INT_T numEntries = -1;
      //testSetRatingsList.size();

      size_t sz = fread(&numEntries, sizeof(INT_T), 1, fp);
      if(sz != 1) {
        throw ("ItemItemPredictor::checkCrossValidation sz!=1");
      }

      sz = 0;
      vector<RatingEntry> vre;
      for(INT_T i=0; i<numEntries; i++) {
        RatingEntry re;
        sz = fread(&re, sizeof(RatingEntry), 1, fp);
        //cout << "" << re.user_id << "," << re.item_id << "," << re.rating << endl;
        vre.push_back(re);
      }

      fclose(fp);
      INT_T maxItm = itemIndex[itemIndex.size()-1];
      INT_T maxUsr = userIndex[userIndex.size()-1];
      INT_T N = 0;
      FLT_T diff_sq_sum = 0;

      INT_T_VEC randomIndexList = getRandomShuffledIndexes(vre.size(), 0, numSamples);

      for(int j=0; j<randomIndexList.size(); j++) {
        INT_T randomIndex = randomIndexList[j];
        //cout << " randomIndex " << randomIndex << endl;

        INT_T uid = vre[randomIndex].user_id;
        INT_T iid = vre[randomIndex].item_id;
        FLT_T rtg = vre[randomIndex].rating;

        if(uid > maxUsr || iid > maxItm)
          continue;

        FLT_T rt = predict(getCodedUserID(uid), getCodedItemID(iid));
        if(isnan(rt)) {
          cout << " isnan(rt)\n";
          continue;
        }
        FLT_T diff = rtg - rt;
        diff_sq_sum += (diff * diff);
        N++;

        if(N%1000 == 0)
          cout << " Predicted " << N << " ratings so far "
          << " current rmse " << sqrt(diff_sq_sum/N)  << endl;
        // cout << " orig: " << rtg << " pred: " << rt << " diff: " << diff << endl;
      }

      FLT_T rmse = sqrt(diff_sq_sum/N);
      cout << " rmse " << rmse
        << " calculated for " << N << " items "<< endl;
      crossValidRMSE = rmse;
      return crossValidRMSE;
  }
#endif

  STRING_T generateRecommendationsForAllUsers(STRING_T recoDirPath, INT_T numRecommendationsPerUser,
        INT_T max_threads_count)
  {
    return generateRecommendationsForAllUsersThreaded(recoDirPath,
                  numRecommendationsPerUser, max_threads_count);
  }
};

#endif
