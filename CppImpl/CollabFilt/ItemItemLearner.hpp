// Author: Senthil Kumar Thangavelu kingjuliyen@gmail.com

#ifndef ITEM_ITEM_LEARNER_HPP
#define ITEM_ITEM_LEARNER_HPP

#include <cmath>
#include <algorithm>
#include <map>
#include <thread>
#include <boost/dynamic_bitset.hpp>

#include "../utils/Mtx.hpp"
#include "../utils/UserItemTableHelper.hpp"

typedef boost::dynamic_bitset<> DynBitSet;
typedef map<INT_T, FLT_T> * UID_RATING_PTR;
typedef map<INT_T, FLT_T> UID_RATING;
typedef vector<FLT_T> fltvec;

// random generator function:
inline int newRandom (int i) { return std::rand()%i; }
const FLT_T BAD_SIMILARITY = -100000.0;

typedef vector<Rating_T> RatingVector;

struct NeighbourHoodRecoParams {
    INT_T top_K_neighbours;
    STRING_T recommendation_type;
    FLT_T training_sample_percentage;
    INT_T max_row_dim;
    INT_T max_col_dim;
    STRING_T csv_input_file_path;
    INT_T verbose_mode_level;
    INT_T loop_mode_count;
    INT_T max_thread_count;
    STRING_T sim_mtx_file_save_path;
    STRING_T item_index_table_path;
    STRING_T user_index_table_path;
};

struct ItemCombination {
  INT_T item1;
  INT_T item2;
  ItemCombination(INT_T i1 , INT_T i2): item1(i1), item2(i2) {}
};

template < typename SIMILARITY_TYPE >
  class NeighbourHoodRecommender;

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

template < typename SIMILARITY_TYPE >
struct SimilarityThreadParams {
  NeighbourHoodRecommender<SIMILARITY_TYPE>& reco;
  INT_T nextThreadIndex;
  INT_T itemComboIndexstart;
  INT_T itemComboIndexend;
  SimilarityThreadParams(NeighbourHoodRecommender<SIMILARITY_TYPE>& _reco,
    INT_T nti, INT_T icis, INT_T icie): reco(_reco), nextThreadIndex(nti),
    itemComboIndexstart(icis), itemComboIndexend(icie)
    {
    }
};

template < typename SIMILARITY_TYPE >
void similarityThread(SimilarityThreadParams<SIMILARITY_TYPE> stp);

template < typename SIMILARITY_TYPE >
class NeighbourHoodRecommender {
    NeighbourHoodRecoParams algoParams;
    vector<RatingEntry> * ratingsList;
    vector<RatingEntry> testSetRatingsList;
    vector<INT_T> user_index_table; // user index
    vector<INT_T> item_index_table; // movie index
    const long long MAX_USERS, MAX_ITEMS;
    INT_T * uidRMap;
    INT_T * iidRMap;
    vector<RatingVector> itemRV; // itembased rating vector
    vector<FLT_T> itemAvgRating;
    vector<DynBitSet> userBst; // user bitset
    vector<fltvec> itemRtQuick; // quick rating lookup
    INT_T simCalcThreadCount;
    INT_T train_start, train_end, test_start, test_end;
    vector <long long> randomShuffledIndexes;

    Mtx * simTbl; // similarity table

    inline void partitionAsTrainingAndValidationSets(vector<RatingEntry> &T) {
        FLT_T tpct = T.size() * algoParams.training_sample_percentage;
        train_start = 0;
        train_end = test_start = (INT_T) tpct;
        test_end = T.size();
    }

    void printMaps(vector<INT_T> &vi, const char *s) {
        cout << s << "\n";
        for(INT_T i=0; i < vi.size(); i++) {
            cout << " " << i << " : " << vi[i] << ", ";
        }
    }

    void printRatingVector(RatingVector &rv, INT_T itm_id) {
      cout << " itemid " << itm_id << " orig itm id "
        << item_index_table[itm_id] << "\n";

      for(int i=0; i<rv.size(); i++) {
        cout << " " << rv[i].uId // transformed uids
          << " " << rv[i].rtng;
      }
      cout << "\n";
    }

    FLT_T getAvgRating(RatingVector &rv) {
      FLT_T avg = 0;
      FLT_T n = rv.size();
      for(int i=0; i<rv.size(); i++) {
        avg += rv[i].rtng;
      }
      return avg/n;
    }

    void printUsers(RatingVector &rv, const char * s) {
      cout << " " << s;
      for(INT_T x = 0; x < rv.size(); x++) {
        INT_T u = rv[x].uId;
        cout << " " << u;
      }
      cout << "\n";
    }

    void checkItemSimiliarty()
    {
      cout << " checkItemSimiliarty()\n";
      auto flt_min = std::numeric_limits<FLT_T>::min();
      INT_T num_items = item_index_table.size();
      simTbl = new Mtx(num_items, num_items, flt_min);
      INT_T item1 = 0, item2 = 0;
      INT_T cmb = 0;
      while(item1 < num_items) {
        for(item2 = item1+1; item2 < num_items; item2++) {
          FLT_T sim = getSimilarity(item1, item2);
          simTbl->set(item1, item2, sim);
          simTbl->set(item2, item1, sim);
          cmb++;
        }
        item1++;
      }
      simTbl->writeMtxToFileSystem(algoParams.sim_mtx_file_save_path.c_str());
      cout << " similarity comparisions made: " << cmb << "\n";
    }

    void checkItemSimiliartyThreaded(INT_T threadCount)
    {
      cout << " checkItemSimiliartyThreaded() threadCount " << threadCount << "\n";
      simCalcThreadCount = threadCount;

      auto flt_min = std::numeric_limits<FLT_T>::min();
      INT_T num_items = item_index_table.size();
      simTbl = new Mtx(num_items, num_items, flt_min);
      INT_T item1 = 0, item2 = 0;

      while(item1 < num_items) {
        for(item2 = item1+1; item2 < num_items; item2++) {
          itemCombo.push_back(ItemCombination(item1, item2));
        }
        item1++;
      }
      cout << " itemCombo.size() " << itemCombo.size() << "\n";
      INT_T comboSz = itemCombo.size()/threadCount;
      vector<thread> threadList;

      for(INT_T i=0; i<threadCount; i++) {
        INT_T start = i*comboSz;
        INT_T end = (start + comboSz);

        itemComboIndexstart = start ;
        itemComboIndexend = end;

        if(i == threadCount-1){
            itemComboIndexend = itemCombo.size() - 1;
        } else {
            itemComboIndexend -= 1;
        }

        SimilarityThreadParams<SIMILARITY_TYPE> stp(*this, nextThreadIndex,
          itemComboIndexstart, itemComboIndexend);

        threadList.push_back(thread(similarityThread<SIMILARITY_TYPE>, stp));
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        nextThreadIndex++;
      }

      for(INT_T i=0; i<threadList.size(); i++) {
          threadList[i].join();
      }
      cout << " similarity comparisions todo: " << itemCombo.size() << "\n";

      simTbl->writeMtxToFileSystem(algoParams.sim_mtx_file_save_path.c_str());
    }

    void setItemsRatingMaps(RatingVector &rv,
          DynBitSet &ubst, FLT_T avgRating, vector<FLT_T> &quickRating)
    {
      for(int i=0; i<rv.size(); i++) {
        INT_T u = rv[i].uId;
        FLT_T rtg = rv[i].rtng;
        // rv[i].weightedRtng = rtg - avgRating;
        ubst[u] = 1;
        quickRating[u] = rtg  - avgRating; // mean centered rating
      }
    }

    void populateRatings() { // convert to internal form
      cout << " populateRatings item_index_table.size() "
        << item_index_table.size() << "\n";

      itemRV = vector<RatingVector>(item_index_table.size());
      itemAvgRating = vector<FLT_T>(item_index_table.size());
      userBst = vector<DynBitSet> (item_index_table.size());
      itemRtQuick = vector<fltvec> (item_index_table.size());

      for(INT_T i = 0; i< ratingsList->size(); i++) {
        RatingEntry &re = ratingsList->at(i);
        INT_T u = re.user_id;
        INT_T iid = re.item_id;
        FLT_T r = re.rating;
        INT_T newIid = iidRMap[iid];
        INT_T newUid = uidRMap[u];
        RatingVector &rv = itemRV[newIid];
        rv.push_back(Rating_T(newUid, r));
      }
      DELETE(ratingsList);

      for(INT_T i=0; i<itemRV.size(); i++) {
        INT_T itm = i;
        RatingVector &rv = itemRV[i];
        sort(itemRV[i].begin(), itemRV[i].end());

        INT_T last = rv.size() -1;
        INT_T maxUsrId = rv[last].uId;
        itemRtQuick[i] = vector<FLT_T>(maxUsrId+1);

        itemAvgRating[i] = getAvgRating(rv);
        userBst[i]  = DynBitSet(MAX_USERS);

        setItemsRatingMaps(itemRV[i],userBst[i], itemAvgRating[i], itemRtQuick[i]);
      }
    }

    void mapUserAndItemIndexes() {
        sort(user_index_table.begin(), user_index_table.end()); // sorted user ids
        sort(item_index_table.begin(), item_index_table.end()); // sorted item ids

        INT_T umax = user_index_table[user_index_table.size() -1] + 1;
        INT_T imax = item_index_table[item_index_table.size() -1] + 1;

        uidRMap = new INT_T[umax]();
        iidRMap = new INT_T[imax]();

        if(!uidRMap || !iidRMap) {
            cout << " if(!uidRMap || !iidRMap) \n";
            return;
        }

        INT_T i = 0;
        for(vector<INT_T>::iterator it = user_index_table.begin(),
                    ie = user_index_table.end(); it != ie; it++) {
            INT_T uid = *it;
            uidRMap[uid] = i;
            i++;
        }

        i = 0;
        for(vector<INT_T>::iterator it = item_index_table.begin(),
                    ie = item_index_table.end(); it != ie; it++) {
            INT_T iid = *it;

            iidRMap[iid] = i;
            i++;
        }
        cout << " user_index_table.size() " << user_index_table.size()
          << " item_index_table.size() " << item_index_table.size() << "\n";
    }

    void shuffleRatingsListIndexes() {
      std::srand(std::time(0));
      random_shuffle(randomShuffledIndexes.begin(), randomShuffledIndexes.end(), newRandom);
    }

    bool isCrossValidationEnabled()
    {
        return algoParams.training_sample_percentage > 0;
    }

    INT_T totalEntriesRead;
    bool readInputAndPartitionForCrossvalidationTests()
    {
        cout << " NeighbourHoodRecommender::readInputAndPartitionForCrossvalidationTests\n";
        INT_T userid, itemid, rating;

        char * uid_table = new char[MAX_USERS]();
        char * iid_table = new char[MAX_ITEMS]();
        FILE * csvFile = fopen (algoParams.csv_input_file_path.c_str(), "r");

        if(!csvFile) {
            cout << "ERROR: Unable to open input file check " << algoParams.csv_input_file_path
                 << " check --input-csv-file-path arg"<< "\n";
            return false;
        }

        INT_T rc = 0; // ratings count
        vector<RatingEntry> tmpRatingList;

        while(true) {
            int r = fscanf(csvFile,"%d %d %d",&userid, &itemid, &rating);
            if(r<0) {
                break;
            }
            tmpRatingList.push_back(RatingEntry(userid, itemid, rating));
            randomShuffledIndexes.push_back(rc++);
        }
        totalEntriesRead = rc;
        partitionAsTrainingAndValidationSets(tmpRatingList);

        for(INT_T i=train_start; i<train_end; i++) {
            INT_T idx = randomShuffledIndexes[i];

            userid = tmpRatingList[idx].user_id;
            itemid = tmpRatingList[idx].item_id;
            rating = tmpRatingList[idx].rating;

            if(uid_table[userid] == 0) {
                uid_table[userid] = 1;
                user_index_table.push_back(userid);
            }
            if(iid_table[itemid] == 0) {
                iid_table[itemid] = 1;
                item_index_table.push_back(itemid);
            }

            ratingsList->push_back(RatingEntry(userid, itemid, rating));
        }

        for(INT_T i=test_start; i<test_end; i++) {
            INT_T idx = randomShuffledIndexes[i];

            userid = tmpRatingList[idx].user_id;
            itemid = tmpRatingList[idx].item_id;
            rating = tmpRatingList[idx].rating;

            testSetRatingsList.push_back(RatingEntry(userid, itemid, rating));
        }

        tmpRatingList.clear();

        if(!user_index_table.size() || !item_index_table.size()) {
          throw("NeighbourHoodRecommender::readInput "
            "if(!user_index_table.size() || !item_index_table.size())");
        }

        mapUserAndItemIndexes();

        DELETE_AR(uid_table);
        DELETE_AR(iid_table);
        fclose(csvFile);

        return true;
    }

    bool readInput() {
        cout << " NeighbourHoodRecommender::readInput\n";
        INT_T userid, itemid, rating;

        char * uid_table = new char[MAX_USERS]();
        char * iid_table = new char[MAX_ITEMS]();
        FILE * csvFile = fopen (algoParams.csv_input_file_path.c_str(), "r");

        if(!csvFile) {
            cout << "ERROR: Unable to open input file check " << algoParams.csv_input_file_path
                 << " check --input-csv-file-path arg"<< "\n";
            return false;
        }

        INT_T rc = 0; // ratings count

        while(true) {
            int r = fscanf(csvFile,"%d %d %d",&userid, &itemid, &rating);
            if(r<0) {
                break;
            }

            if(uid_table[userid] == 0) {
                uid_table[userid] = 1;
                user_index_table.push_back(userid);
            }
            if(iid_table[itemid] == 0) {
                iid_table[itemid] = 1;
                item_index_table.push_back(itemid);
            }

            ratingsList->push_back(RatingEntry(userid, itemid, rating));
        }

        if(!user_index_table.size() || !item_index_table.size()) {
          throw("NeighbourHoodRecommender::readInput "
            "if(!user_index_table.size() || !item_index_table.size())");
        }

        mapUserAndItemIndexes();

        DELETE_AR(uid_table);
        DELETE_AR(iid_table);
        fclose(csvFile);

        return true;
    }

    void writeIndexTableToFileSystem(vector<INT_T> vi, const char * path)
    {
      FILE *fp = fopen(path, "wb");
      if(!fp)
        throw("NeighbourHoodRecommender::writeIndexTableToFileSystem if(!fp)");
      long long viSz = vi.size();
      size_t sz = fwrite(&viSz, sizeof(viSz), 1, fp);
      if(sz!=1) {
        fclose(fp);
        throw("NeighbourHoodRecommender::writeIndexTableToFileSystem if(sz!=1)");
      }

      long long written = 0;
      for(long long i=0; i<vi.size(); i++) {
        INT_T val = vi[i];
        written += fwrite(&val, sizeof(val), 1, fp);
      }
      if(written != vi.size()) {
        fclose(fp);
        throw("NeighbourHoodRecommender::writeIndexTableToFileSystem written != vi.size()");
      }
      fclose(fp);
    }

    void writeItemAndUserIndexTables()
    {
      writeIndexTableToFileSystem(item_index_table,
              algoParams.item_index_table_path.c_str());
      writeIndexTableToFileSystem(user_index_table,
              algoParams.user_index_table_path.c_str());
    }

    void cleanupIntermediates()
    {
      cout << " NeighbourHoodRecommender::cleanupIntermediates\n";
      user_index_table.clear();
      item_index_table.clear();
      DELETE_AR(uidRMap);
      DELETE_AR(uidRMap);
      itemRV.clear();
      itemAvgRating.clear();
      userBst.clear();
      itemRtQuick.clear();
    }

    void doItemItemRecommendation() {
        cout << " NeighbourHoodRecommender::doItemItemRecommendation starting\n";
        populateRatings();
        if(algoParams.max_thread_count ==1)
          checkItemSimiliarty();
        else
          checkItemSimiliartyThreaded(algoParams.max_thread_count);
        writeItemAndUserIndexTables();
        cleanupIntermediates();
        Mtx mtx2(algoParams.sim_mtx_file_save_path.c_str());
        bool b = simTbl->compare(mtx2);
        if(!b) {
          cout << " NeighbourHoodRecommender::doItemItemRecommendation if(!b)\n";
        } else {
          cout << " NeighbourHoodRecommender::doItemItemRecommendation "
            "simTbl->compare(mtx2) == true\n";
        }
    }

    void writeTestSetEntriesToFile(const char *path = "testSetRatingsList.RatingEntry")
    {
      FLT_T rmse = FLT_T_MAX();
      INT_T totalTestSetcount = 0;
      INT_T actualRMSENumElements = 0;

      cout << " NeighbourHoodRecommender::writeTestSetEntriesToFile writing "
        << testSetRatingsList.size() << " entries to " << path << endl;

      FILE * fp = fopen(path, "wb");
      if(!fp) {
        throw ("NeighbourHoodRecommender::writeTestSetEntriesToFile !fp");
      }

      INT_T numEntries = testSetRatingsList.size();

      size_t sz = fwrite(&numEntries, sizeof(INT_T), 1, fp);
      if(sz!=1) {
        throw ("NeighbourHoodRecommender::writeTestSetEntriesToFile sz!=1");
      }

      sz = 0;
      for(int i=0; i<testSetRatingsList.size(); i++) {
        INT_T uid = testSetRatingsList[i].user_id;
        INT_T iid = testSetRatingsList[i].item_id;
        FLT_T rtg = testSetRatingsList[i].rating;
        RatingEntry re(uid, iid, rtg);
        sz += fwrite(&re, sizeof(RatingEntry), 1, fp);
        // cout << "" << uid << "," << iid << "," << rtg << endl;
      }
      if(sz != testSetRatingsList.size()) {
        cout << " NeighbourHoodRecommender::writeTestSetEntriesToFile "
          << "sz != testSetRatingsList.size()" << endl;
      }
      fclose(fp);
    }

public:
    INT_T nextThreadIndex;
    INT_T itemComboIndexstart, itemComboIndexend;
    vector<ItemCombination> itemCombo; // used by threaded version only

    FLT_T getSimilarity(INT_T i1, INT_T i2);

    NeighbourHoodRecommender(NeighbourHoodRecoParams params) :algoParams(params),
        MAX_USERS(params.max_row_dim), MAX_ITEMS(params.max_col_dim),
        simTbl(0), simCalcThreadCount(0), nextThreadIndex(0)
    {
        ratingsList = new vector<RatingEntry> ();
    }

    ~NeighbourHoodRecommender() {
      cout << " ~NeighbourHoodRecommender() freeing resources \n";
    }

    bool readInput(bool isCrossValidate) {
        bool b = false;
        if(isCrossValidate) {
          b = readInputAndPartitionForCrossvalidationTests();
        } else {
          b = readInput();
        }
        return b;
    }

    bool startTraining() {
      return analyzeInputs();
    }

    bool analyzeInputs() {

        if(!readInput(isCrossValidationEnabled())) {
            return false;
        }

        doItemItemRecommendation();

        if(isCrossValidationEnabled()) {
          writeTestSetEntriesToFile();
        }

        return true;
    }

    void setSimTableValue(INT_T row, INT_T col, FLT_T val) {
        simTbl->set(row, col, val);
        simTbl->set(col, row, val);
    }

};

template < typename SIMILARITY_TYPE >
void similarityThread(SimilarityThreadParams<SIMILARITY_TYPE> stp) {
    NeighbourHoodRecommender<SIMILARITY_TYPE> &reco = stp.reco;
    INT_T nextThreadIndex = stp.nextThreadIndex;
    INT_T itemComboIndexstart = stp.itemComboIndexstart;
    INT_T itemComboIndexend = stp.itemComboIndexend;

    cout << "nextThreadIndex " << nextThreadIndex
            <<" itemComboIndexstart  "<< itemComboIndexstart
            << " itemComboIndexend " << itemComboIndexend << "\n";

    for(int i=itemComboIndexstart; i<=itemComboIndexend; i++){
        ItemCombination ic = reco.itemCombo[i];
        FLT_T sim = reco.getSimilarity(ic.item1, ic.item2);
        reco.setSimTableValue(ic.item1, ic.item2, sim);
        reco.setSimTableValue(ic.item2, ic.item1, sim);
    }
    execShellCommand("date");
    cout << "\n Thread exit " << nextThreadIndex << " exited \n";
}

#include "SimilarityFunctions.hpp"

#endif // ITEM_ITEM_LEARNER_HPP
