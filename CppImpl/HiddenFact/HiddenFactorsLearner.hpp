// Author: Senthil Kumar Thangavelu kingjuliyen@gmail.com

#ifndef HIDDENFACTORS_HPP
#define HIDDENFACTORS_HPP

#include <vector>
#include <algorithm>
#include <sstream>

#include "../utils/Utils.hpp"
#include "../utils/Mtx.hpp"

// random generator function:
inline int newRandom (int i) { return std::rand()%i; }

typedef struct RatingEntry {
  INT_T user_id;
  INT_T item_id;
  INT_T rating;
  RatingEntry(INT_T u, INT_T i, INT_T r) : user_id(u), item_id(i), rating(r) { }
} RatingEntry;

struct MatrixFactorizationParams {
  INT_T num_factors;
  FLT_T default_rating;
  FLT_T regularization_param_p;
  FLT_T regularization_param_q;
  FLT_T learning_rate_p;
  FLT_T learning_rate_q;
  FLT_T training_sample_percentage;
  INT_T gradient_descent_iteration_count;
  INT_T max_row_dim;
  INT_T max_col_dim;
  STRING_T csv_input_file_path;
  STRING_T p_q_matrix_output_file_path;
  INT_T verbose_mode_level;
  INT_T loop_mode_count;

  void print() {
    cout << "\n\n--------------- training parameters ---------------\n";
    cout
      << " num_factors: " <<  num_factors << "\n"
      << " default_rating: " << default_rating << "\n"
      << " regularization_param_p: " << regularization_param_p << "\n"
      << " regularization_param_q: " << regularization_param_q << "\n"
      << " learning_rate_p: " << learning_rate_p << "\n"
      << " learning_rate_q: " << learning_rate_q << "\n"
      << " training_sample_percentage: " << training_sample_percentage << "\n"
      << " max_row_dim: " << max_row_dim << "\n"
      << " max_col_dim: " << max_col_dim << "\n"
      << " gradient_descent_iteration_count: " << gradient_descent_iteration_count << "\n"
      << " csv_input_file_path: " << csv_input_file_path << "\n";
      cout << "-------------------------------------------------\n\n\n";
  }
} ;

class MatrixFactorization {
  MatrixFactorizationParams algoParams;
  vector<RatingEntry> * ratingsList;
  vector<INT_T> ratingsListShuffle;
  vector<INT_T> user_index_table; // user index
  vector<INT_T> item_index_table; // movie index
  const long long MAX_USERS, MAX_ITEMS;
  INT_T * uidRMap;
  INT_T * iidRMap;
  vector<INT_T> uidMap; // FIXME: uidRMap and user_index_table are copies remove one
  vector<INT_T> iidMap; // FIXME: iidMap and item_index_table are copies remove one
  FLT_T oldRMSE;
  INT_T train_start, train_end, test_start, test_end;
  Mtx *P, *Q, *Pstar, *Qstar;
  FLT_T lambda_p, lambda_q;
  FLT_T eta_p, eta_q;
  FLT_T finalRMSE;
  INT_T iterations;

  void printMaps(vector<INT_T> &vi, const char *s) {
    cout << s << "\n";
    for(INT_T i=0; i < vi.size(); i++) {
      cout << " " << i << " : " << vi[i] << "\n";
    }
  }

  void writeIndexTableToFileSystem(vector<INT_T> vi, const char * path) {
    FILE *fp = fopen(path, "wb");
    if(!fp)
      throw("HiddenFactorLearner::writeIndexTableToFileSystem if(!fp)");
    long long viSz = vi.size();
    size_t sz = fwrite(&viSz, sizeof(viSz), 1, fp);
    if(sz!=1) {
      fclose(fp);
      throw("HiddenFactorLearner::writeIndexTableToFileSystem if(sz!=1)");
    }

    long long written = 0;
    for(long long i=0; i<vi.size(); i++) {
      INT_T val = vi[i];
      written += fwrite(&val, sizeof(val), 1, fp);
    }
    if(written != vi.size()) {
      fclose(fp);
      throw("HiddenFactorLearner::writeIndexTableToFileSystem written != vi.size()");
    }
    fclose(fp);
  }

  void writeItemAndUserIndexTables()
  {
    STRING_T usrIdx(algoParams.p_q_matrix_output_file_path + "usr.idx");
    STRING_T itmIdx(algoParams.p_q_matrix_output_file_path + "itm.idx");

    writeIndexTableToFileSystem(item_index_table, itmIdx.c_str());
    writeIndexTableToFileSystem(user_index_table, usrIdx.c_str());
  }

  void mapUserAndItemIndexes() {
    sort(user_index_table.begin(), user_index_table.end());
    sort(item_index_table.begin(), item_index_table.end());

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
      uidMap.push_back(uid);
      uidRMap[uid] = i;
      i++;
    }

    i = 0;
    for(vector<INT_T>::iterator it = item_index_table.begin(),
      ie = item_index_table.end(); it != ie; it++) {
        INT_T iid = *it;
        iidMap.push_back(iid);
        iidRMap[iid] = i;
        i++;
    }
     writeItemAndUserIndexTables();
  }

  void shuffleRatingsListIndexes() {
    std::srand(std::time(0));
    random_shuffle(ratingsListShuffle.begin(), ratingsListShuffle.end(), newRandom);
  }

  bool readInput() {
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
      ratingsListShuffle.push_back(rc++);
    }
    mapUserAndItemIndexes();

    DELETE_AR(uid_table);
    DELETE_AR(iid_table);
    fclose(csvFile);
    return true;
  }

  bool terminalConditionMet() {
    algoParams.gradient_descent_iteration_count--;
    return algoParams.gradient_descent_iteration_count < 1;
  }

  void initializeQandPmatrices() {
    if(algoParams.verbose_mode_level > 0)
      cout << "initializeQandPmatrices " << "\n";
    if(user_index_table.size() >= algoParams.max_row_dim ||
      item_index_table.size() >= algoParams.max_row_dim) {
      cout << "user_index_table.size() >= MAX_DIM ||"
                " item_index_table.size() >= MAX_DIM\n";
      return;
    }

    P = new Mtx(user_index_table.size(), algoParams.num_factors, algoParams.default_rating);
    Q = new Mtx(algoParams.num_factors, item_index_table.size(), algoParams.default_rating);

    Pstar = new Mtx(user_index_table.size(), algoParams.num_factors, algoParams.default_rating);
    Qstar = new Mtx(algoParams.num_factors, item_index_table.size(), algoParams.default_rating);
  }

  inline void partitionAsTrainingAndValidationSets(vector<RatingEntry> &T) {
    FLT_T tpct = T.size() * algoParams.training_sample_percentage;
    train_start = 0;
    train_end = test_start = (INT_T) tpct;
    test_end = T.size();
  }

  FLT_T getPredictedRating5(INT_T u, INT_T i) {
    FLT_T r =
      P->get(u,0) * Q->get(0,i) + P->get(u,1) * Q->get(1,i) +
      P->get(u,2) * Q->get(2,i) + P->get(u,3) * Q->get(3,i) +
      P->get(u,4) * Q->get(4,i);
    return r;
  }

  FLT_T getPredictedRating(INT_T u, INT_T i) {
    FLT_T r = 0;
    for (INT_T k=0; k<algoParams.num_factors; k++){
      r += P->get(u,k) * Q->get(k,i);
    }
    return r;
  }

  FLT_T getEui(INT_T u, INT_T i, FLT_T rui) {
    FLT_T rcap_ui = getPredictedRating(u, i);
    FLT_T eui = rui - rcap_ui;
    return eui;
  }

  FLT_T getRMSEforT2dash(vector<RatingEntry> &T) {
    INT_T count = 0;
    FLT_T sse = 0;
    for(INT_T x=test_start; x<test_end; x++) {
      INT_T y = ratingsListShuffle[x];

      INT_T u = T[y].user_id;
      INT_T i = T[y].item_id;
      FLT_T r_ui = T[y].rating;

      u = uidRMap[u];
      i = iidRMap[i];

      FLT_T rcap_ui = getPredictedRating(u,i);
      FLT_T e_ui = r_ui - rcap_ui;
      sse += (e_ui * e_ui);
      count++;
    }
    FLT_T rmse = sqrt(sse/count);
    return rmse;
  }

  void updatePu(INT_T u, INT_T i, INT_T k, FLT_T e_ui) { // non simultaneous update
    FLT_T p_uk = P->get(u,k);
    FLT_T q_ki = Q->get(k,i);

    FLT_T pdash_uk = p_uk + eta_p * ((e_ui * q_ki) - (lambda_p * p_uk));
    P->set(u,k,pdash_uk);
  }

  void updateQi(INT_T u, INT_T i, INT_T k, FLT_T e_ui) { // non simultaneous update
    FLT_T p_uk = P->get(u,k);
    FLT_T q_ki = Q->get(k,i);

    FLT_T qdash_ki = q_ki + eta_q * ((e_ui * p_uk) - (lambda_q * q_ki));
    Q->set(k,i,qdash_ki);
  }

  void updatePuAndQiNonSimultaneous(INT_T u, INT_T i, INT_T k, FLT_T eui) { // non simultaneous update
    updatePu(u,i,k,eui);
    updateQi(u,i,k,eui);
  }

  void updatePu(FLT_T p_uk, FLT_T q_ki, INT_T k, FLT_T e_ui, INT_T u) { // simultaneous update
    FLT_T pdash_uk = p_uk + eta_p * ((e_ui * q_ki) - (lambda_p * p_uk));
    P->set(u,k,pdash_uk);
  }

  void updateQi(FLT_T p_uk, FLT_T q_ki, INT_T k, FLT_T e_ui, INT_T i) { // simultaneous update
    FLT_T qdash_ki = q_ki + eta_q * ((e_ui * p_uk) - (lambda_q * q_ki));
    Q->set(k,i,qdash_ki);
  }

  void updatePuAndQiSimultaneous(INT_T u, INT_T i, INT_T k, FLT_T e_ui) { // simultaneous update
    FLT_T p_uk = P->get(u,k);
    FLT_T q_ki = Q->get(k,i);
    updatePu(p_uk, q_ki, k, e_ui, u);
    updateQi(p_uk, q_ki, k, e_ui, i);
  }

  inline void do_for_each_element_of_T1dash(vector<RatingEntry> &T) {
    for(INT_T x=0; x<train_end; x++) {
      INT_T y = ratingsListShuffle[x];

      INT_T u = T[y].user_id;
      INT_T i = T[y].item_id;
      FLT_T rui = T[y].rating;

      u = uidRMap[u];
      i = iidRMap[i];
      FLT_T eui = getEui(u, i, rui);
      //cout << "eui " << eui << "\n";
      for(INT_T k=0; k < algoParams.num_factors; k++) {
        updatePuAndQiSimultaneous(u,i,k,eui);
        // updatePuAndQiNonSimultaneous(u,i,k,eui);
      }
    }
  }

  void updateQandP() {
    P->copy(Pstar);
    Q->copy(Qstar);
  }

  inline void doMatrixFactorization(vector<RatingEntry> *T) {
    if(algoParams.verbose_mode_level > 0)
      cout << "doMatrixFactorization " << "\n";

    partitionAsTrainingAndValidationSets(*T);
    initializeQandPmatrices();
    INT_T count = 0;
    FLT_T olddiff = 9999999;

    while(!terminalConditionMet()) {
      do_for_each_element_of_T1dash(*T);
      // for each element (u,i,rui) of T1'
      FLT_T newRMSE = getRMSEforT2dash(*T);

      if(algoParams.verbose_mode_level > 0)
        cout << "iteration: " << count << " RMSE: " << newRMSE << "\n";
      count++;
      system("date");
      FLT_T diff = (newRMSE - oldRMSE);
      if(diff<0) {
          diff *= -1;
      }

      if(diff > olddiff) {
          finalRMSE = newRMSE;
          break;
      }
      olddiff = diff;

      if(newRMSE < oldRMSE) {
        updateQandP();
      }
      oldRMSE = newRMSE;
    }
    iterations = count -1;
  }

  void startMatrixFactorization(vector<RatingEntry> *T) {
    oldRMSE = 0;
    doMatrixFactorization(T);
  }

public:

  MatrixFactorization(MatrixFactorizationParams params) : algoParams(params), MAX_USERS(params.max_row_dim),
    MAX_ITEMS(params.max_col_dim), uidRMap(0), iidRMap(0),
    train_start(-1), train_end(-1), test_start(-1), test_end(-1),
    P(0), Q(0), Pstar(0), Qstar(0), finalRMSE(99.0),
    lambda_p(params.learning_rate_p), lambda_q(params.learning_rate_q),
    eta_p(params.regularization_param_p), eta_q(params.regularization_param_q)
  {
    ratingsList = new vector<RatingEntry> ();
  }

  ~MatrixFactorization() {
    DELETE_AR(uidRMap); DELETE_AR(iidRMap);
    DELETE(P); DELETE(Q); DELETE(Pstar); DELETE(Qstar);
    DELETE(ratingsList);
  }

  bool startTraining() {
    if(algoParams.verbose_mode_level > 0)
      algoParams.print();

    if(!readInput()){
      return false;
    }
    shuffleRatingsListIndexes();
    startMatrixFactorization(ratingsList);
    writePqmatrixOutputFile();

    return true;
  }

  void writePqmatrixOutputFile()
  {
    STRING_T P_matrixPath =
      STRING_T(algoParams.p_q_matrix_output_file_path + "P_matrix.mtx");
    STRING_T Q_matrixPath =
      STRING_T(algoParams.p_q_matrix_output_file_path + "Q_matrix.mtx");
    P->writeMtxToFileSystem(P_matrixPath.c_str());
    Q->writeMtxToFileSystem(Q_matrixPath.c_str());
  }

  FLT_T getFinalRMSE() { return finalRMSE; }
  INT_T getNumIterations() { return iterations; }

  void printPredictedValues(INT_T numSamples) {
    vector<RatingEntry> &T = *ratingsList;
    for(INT_T x=test_start; x < (test_start + numSamples); x++) {
      INT_T y = ratingsListShuffle[x];

      INT_T u = T[y].user_id;
      INT_T i = T[y].item_id;
      FLT_T r_ui = T[y].rating;

      u = uidRMap[u];
      i = iidRMap[i];

      FLT_T rcap_ui = getPredictedRating(u,i);
      FLT_T e_ui = r_ui - rcap_ui;
      cout << " user_id " << u << " item id " << i
        << " predicted rating " << rcap_ui << " original rating " << r_ui
        << " error " << e_ui << "\n";
    }
  }

  // Take real userID and return internal userID used
  INT_T getCodedUserID(INT_T uid)
  {
    INT_T cuid = uidRMap[uid];
    return cuid;
  }

  INT_T getUncodedUserId(INT_T _uid)
  {
    INT_T uid = user_index_table[_uid];
    return uid;
  }

  // Take real userID and return internal userID used
  INT_T getCodedItemID(INT_T iid)
  {
    INT_T ciid = iidRMap[iid];
    return ciid;
  }

  INT_T getUncodedItemId(INT_T _iid)
  {
    INT_T iid = item_index_table[_iid];
    return iid;
  }

};


#endif // HIDDENFACTORS_HPP
