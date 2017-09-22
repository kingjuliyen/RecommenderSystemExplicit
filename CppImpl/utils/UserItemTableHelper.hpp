#ifndef USERITEMTABLEHELPER_HPP
#define USERITEMTABLEHELPER_HPP

#include <cstdio>
#include <boost/dynamic_bitset.hpp>
#include "Utils.hpp"

typedef boost::dynamic_bitset<> DynBitSet;

typedef struct RatingEntry_T {
  INT_T user_id;
  INT_T item_id;
  INT_T rating;
  RatingEntry_T(INT_T u, INT_T i, INT_T r) : user_id(u), item_id(i), rating(r) { }
} RatingEntry_T;

typedef struct RatingEntry {
    INT_T user_id;
    INT_T item_id;
    INT_T rating;
    RatingEntry(INT_T u, INT_T i, INT_T r) : user_id(u), item_id(i), rating(r) { }
    RatingEntry() {}
} RatingEntry;

typedef struct Rating_T {
  INT_T uId; // user id
  INT_T rtng; // rating
  //FLT_T weightedRtng; // weighted rating
  Rating_T(INT_T u, INT_T r) : uId(u), rtng(r) { }
  Rating_T(INT_T u) : uId(u) { }
  bool operator<(const Rating_T& another) const { return uId < another.uId; }
  bool operator()(const Rating_T& rt) const { return rt.uId == uId; }
} Rating_T;
typedef vector<Rating_T> RatingVector;

class UserItemTableHelper {
  public:
  STRING_T userItemCSV;
  STRING_T outputFilesDir;
  INT_T MAX_USERS, MAX_ITEMS;
  vector<INT_T> user_index_table; // user index
  vector<INT_T> item_index_table; // movie index
  vector<RatingEntry_T> ratingsList;
  INT_T * uidRMap;
  INT_T * iidRMap;
  vector<RatingVector> itemRV; // itembased rating vector
  vector<DynBitSet> userBst; // user bitset

  UserItemTableHelper(STRING_T _userItemCSV, STRING_T _outputFilesDir = STRING_T("/tmp/")):
    userItemCSV(_userItemCSV),
    outputFilesDir(_outputFilesDir)
  {
    MAX_USERS = 5400000;
    MAX_ITEMS = 100000;
  }

  ~UserItemTableHelper() {
    DELETE_AR(uidRMap);
    DELETE_AR(iidRMap);
  }

  // Should be coded ID only
  bool hasUserRatedItem(INT_T usrId, INT_T itemId)
  {
    DynBitSet &ubst = userBst[itemId];
    return ubst[usrId] == 1;
  }

  void prepareTable()
  {
    readInput();
    populateRatings();
  }

  void populateRatings()
  { // convert to internal form
    cout << " populateRatings item_index_table.size() "
      << item_index_table.size() << "\n";

    itemRV = vector<RatingVector>(item_index_table.size());
    userBst = vector<DynBitSet> (item_index_table.size());

    INT_T umax = user_index_table[user_index_table.size() -1] + 1;
    for(INT_T i = 0; i< itemRV.size(); i++) {
      //cout << " userBst[" << i << "] for size " << umax << endl;
      userBst[i]  = DynBitSet(umax);
    }
    for(INT_T i = 0; i< ratingsList.size(); i++) {
      RatingEntry_T &re = ratingsList.at(i);
      INT_T u = re.user_id;
      INT_T iid = re.item_id;
      //FLT_T r = re.rating;
      INT_T newIid = iidRMap[iid];
      INT_T newUid = uidRMap[u];
      RatingVector &rv = itemRV[newIid];
      DynBitSet &ubst = userBst[newIid];
      ubst[newUid] = 1;
      //rv.push_back(Rating_T(newUid, r));
    }
    ratingsList.clear();

#if 0
    for(INT_T i=0; i<itemRV.size(); i++) {
      sort(itemRV[i].begin(), itemRV[i].end());
    }
#endif
    cout << " populateRatings done! " << endl;
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
      // cout << " " << uid << "\n";
      uidRMap[uid] = i;
      i++;
    }

    i = 0;
    for(vector<INT_T>::iterator it = item_index_table.begin(),
            ie = item_index_table.end(); it != ie; it++) {
      INT_T iid = *it;
      // cout << " " << mid << "\n";
      iidRMap[iid] = i;
      i++;
    }
    cout << " user_index_table.size() " << user_index_table.size()
      << " item_index_table.size() " << item_index_table.size() << "\n";
    // printMaps(user_index_table, "user index maps");
    // printMaps(item_index_table, "item index maps");
  }

  bool readInput() {
    cout << " UserItemTableHelper::readInput\n";
    INT_T userid, itemid, rating;

    char * uid_table = new char[MAX_USERS]();
    char * iid_table = new char[MAX_ITEMS]();
    FILE * csvFile = fopen (userItemCSV.c_str(), "r");

    if(!csvFile) {
      cout << "ERROR: Unable to open input file check " << userItemCSV
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

      ratingsList.push_back(RatingEntry_T(userid, itemid, rating));
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
};

#if 0
int main() {
  UserItemTableHelper uith("user-item-ratings.csv");
  uith.prepareTable();
}
#endif

#endif // USERITEMTABLEHELPER_HPP
