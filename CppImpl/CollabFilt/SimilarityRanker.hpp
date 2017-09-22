// Author: Senthil Kumar Thangavelu kingjuliyen@gmail.com

#ifndef SIMILARITYRANKDER_HPP
#define SIMILARITYRANKDER_HPP

struct ItemCombination {
  INT_T item1;
  INT_T item2;
  ItemCombination(INT_T i1 , INT_T i2): item1(i1), item2(i2) {}
};

class SimilarityRanker {
  string simfilesdir;
  RatingsStore * rtStore;
  INT_T threadCount;
  vector<ItemCombination> itemCombo;

  string getSimFilesDir()
  {
    return simfilesdir;
  }

  mutex muCU0;
  long long cu_count;

  void updateCommonUsrsTimeSpent(long long t, bool clear=false) {
    muCU0.lock();
    if(!clear)
      cu_count += t;
    else
      cu_count = 0;
    muCU0.unlock();
  }

  mutex upg0;
  long long gwrcount;
  void updategwr(long long x, bool clear=false) {
    upg0.lock();
    if(!clear)
      gwrcount += x;
    else
      gwrcount = 0;
    upg0.unlock();
  }


  mutex gs0m;
  long long gs0count;
  void updategs(long long x, bool clear=false) {
    gs0m.lock();
    if(!clear) {
      gs0count += x;
    }
    else {
      gs0count = 0;
    }
    gs0m.unlock();
  }

  void getCommonUsrsOfItems(INT_T i1, INT_T i2, vector<INT_T> &cu)
  {
    //START_TIME_STAMP("getCommonUsrsOfItems")
    TIME_POINT t0s = NOW();
    auto rv1 =  rtStore->getRatingVectorForItem(i1);
    for(int i=0; i<rv1.size(); i++) {
      if(rtStore->hasUsrRatedItem(rv1[i].uid, i2) != FLT_T_MIN())
        cu.push_back(rv1[i].uid);
    }
    TIME_POINT t0e = NOW();
    auto df = std::chrono::duration_cast
      <std::chrono::milliseconds> (t0e - t0s).count();
    updateCommonUsrsTimeSpent(df);
    //END_TIME_STAMP;
  }

  FLT_T getWeightedRating(INT_T usr, INT_T itm)
  {
  //START_TIME_STAMP("getWeightedRating")
    TIME_POINT t0s = NOW();
    auto r = rtStore->getRatingForCodedUsrID(usr, itm)
              - rtStore->getAvgRating(itm);
    TIME_POINT t0e = NOW();
    auto df = std::chrono::duration_cast
      <std::chrono::milliseconds> (t0e - t0s).count();
    updategwr(df);
    return r;
  //END_TIME_STAMP;
  }

  INT_T simCount;
  long long loopcount;

  mutex m0;
  void thread_safe_print(string s)
  {
    m0.lock();
    cout << s << endl;
    m0.unlock();
  }

  mutex m1;
  void incsimCount(long long _loopcount) {
    m1.lock();
    simCount++;
    loopcount += _loopcount;
    m1.unlock();
  }

  FLT_T getSimilarity(INT_T i1, INT_T i2)
  {
  //START_TIME_STAMP("getSimilarity")
    TIME_POINT ts = NOW();
    FLT_T numerator = 0;
    FLT_T Su1 = 0, Su2 = 0; // mean centered rating
    FLT_T Su1_squared_sum = 0, Su2_squared_sum = 0;

    vector<INT_T> commonUsrs;
    getCommonUsrsOfItems(i1, i2, commonUsrs);

    if(commonUsrs.size() == 0)
      return 0;

    long long lcount0 = 0;

    for(int usr = 0; usr < commonUsrs.size(); usr++ ) {
      Su1 = getWeightedRating(usr, i1);
      Su2 = getWeightedRating(usr, i2);
      numerator += Su1 * Su2;
      Su1_squared_sum += Su1 * Su1;
      Su2_squared_sum += Su2 * Su2;
      lcount0++;
    }

    FLT_T denominator = sqrt(Su1_squared_sum) * sqrt(Su2_squared_sum);
    FLT_T adjusted_cosine = numerator/denominator;

    TIME_POINT te = NOW();
    auto df = std::chrono::duration_cast
      <std::chrono::milliseconds> (te - ts).count();

    updategs(df);

    long long oldLcount = loopcount;
    incsimCount(lcount0);
    FLT_T completed = ((FLT_T)simCount/(FLT_T)itemCombo.size()) * 100.0;

    string s = " completed " + to_string(completed) + "% " +
      to_string(simCount) + "/" + to_string(itemCombo.size()) + " " +
      " loopcount " + to_string(loopcount - oldLcount) +
      " getSimilarity " + to_string(gs0count) +
      " getCommonUsrsOfItems " + to_string(cu_count) +
      " getweightedrating " + to_string(gwrcount)
      ;

    if(simCount % 200000 == 0) {
      thread_safe_print(s);
      updategs(0, true);
      updateCommonUsrsTimeSpent(0, true);
      updategwr(0, true);
    }

  //END_TIME_STAMP;
    return adjusted_cosine;
  }

  void compareSimilarity(INT_T start, INT_T end, INT_T threadIndex)
  {
    cout << " thread " << threadIndex <<
      " start " << start << " end " << end << endl;
      for(int i=start; i<=end; i++) {
        INT_T i1 = itemCombo[i].item1;
        INT_T i2 = itemCombo[i].item2;
        FLT_T sim = getSimilarity(i1, i2);
      }
  }

  void checkItemSimiliartyThreadedImpl()
  {
    simCount = 0;
    cout << " checkItemSimiliartyThreadedImpl " << endl;
    INT_T item1 = 0, item2 = 0;

    INT_T num_items = rtStore->getNumItems();

    cout << " num_items " << num_items << endl;
    cout << " checkItemSimiliartyThreadedImpl  while(item1 < num_items) " << endl;
    while(item1 < num_items) {
      for(item2 = item1+1; item2 < num_items; item2++) {
        itemCombo.push_back(ItemCombination(item1, item2));
      }
      item1++;
    }

    cout << " itemCombo.size() " << itemCombo.size() << "\n";
    INT_T comboSz = itemCombo.size()/threadCount;
    vector<thread> threadList;
    INT_T itemComboIndexstart, itemComboIndexend;

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

      threadList.push_back(
        thread(
          &SimilarityRanker::compareSimilarity, this,
          itemComboIndexstart, itemComboIndexend, i)
      );
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

      for(INT_T i=0; i<threadList.size(); i++) {
          threadList[i].join();
      }
  }

  public:
  SimilarityRanker(string _simfilesdir, RatingsStore * _rtStore,
    INT_T _threadCount) :
    simfilesdir(_simfilesdir), rtStore(_rtStore), threadCount (_threadCount),
    loopcount(0), gs0count(0) //, cu_count(0), gwrcount(0)
  {
  }

  void checkItemSimiliartyThreaded()
  {
    cout << " SimilarityRanker::checkItemSimiliartyThreaded \n getchar() " << endl;
    rtStore->loadAllRatingVector();
    checkItemSimiliartyThreadedImpl();
  }
};

#endif
