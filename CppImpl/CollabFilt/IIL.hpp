
// Author: Senthil Kumar Thangavelu kingjuliyen@gmail.com

#ifndef IIL_HPP
#define IIL_HPP

#include "SimilarityRanker.hpp"

class RecoDriver {
  RatingsStore * rtStore;

  string sandboxdir;

  public:
  RecoDriver(): rtStore(0)
  {
  }

  void createRatingsStore(string csvpath, string outfilesDir, INT_T threadsCount)
  {
    RatingsStore rs(csvpath, outfilesDir, threadsCount);
  }

  void loadRecoSetup(string recosetupdir)
  {
    rtStore = new RatingsStore(recosetupdir);
  }

  string getSimFilesDir() {
    return sandboxdir + "/" + "simi-files";
  }

  void createSimilarityFilesDir()
  {
    mkdir(getSimFilesDir());
  }

  void rankSimilarity(string sandboxDir, INT_T threadsCount)
  {
    sandboxdir = sandboxDir;
    createSimilarityFilesDir();
    SimilarityRanker sr(getSimFilesDir(), rtStore, threadsCount);
    sr.checkItemSimiliartyThreaded();
  }
};

#endif
