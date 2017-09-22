// Author: Senthil Kumar Thangavelu kingjuliyen@gmail.com

#ifndef SIMILARITY_FUNCTIONS_HPP
#define SIMILARITY_FUNCTIONS_HPP


class AdjustedCosine { };
    // adjusted cosine similarity
    // considers local average of ratings,
    // where item ratings set N varies and
    // is not the same as
    // sum(all ratings of item)/ (number of all ratings of item)

  template<>
  FLT_T NeighbourHoodRecommender<AdjustedCosine>::
     getSimilarity(INT_T i1, INT_T i2)
    {
      RatingVector &rv1 = itemRV[i1];
      RatingVector &rv2 = itemRV[i2];
      vector<FLT_T> &quickRating1 = itemRtQuick[i1];
      vector<FLT_T> &quickRating2 = itemRtQuick[i2];
      DynBitSet &ubst1 = userBst[i1];

      FLT_T rtng1 = 0;
      FLT_T rtng2 = 0;

      vector<INT_T> users;
      FLT_T numerator = 0;

      FLT_T Su1 = 0, Su2 = 0; // mean centered rating
      FLT_T Su1_squared_sum = 0, Su2_squared_sum = 0;

      for(INT_T x = 0; x < rv2.size(); x++) {
        INT_T u2 = rv2[x].uId;
        if(!ubst1[u2]) // that user hasn't rated both item1 and item2
          continue;
        users.push_back(u2);
        Su1 = quickRating1[u2];
        Su2 = quickRating2[u2];
        numerator += Su1 * Su2;
        Su1_squared_sum += Su1 * Su1;
        Su2_squared_sum += Su2 * Su2;
      }

      if(users.size() <=1)
        return 0;

      FLT_T denominator = sqrt(Su1_squared_sum) * sqrt(Su2_squared_sum);
      FLT_T adjusted_cosine = numerator/denominator;

      return adjusted_cosine;
    }

    class RawCosine { };

    // Raw cosine similarity
    template<>
    FLT_T NeighbourHoodRecommender<RawCosine>::
    getSimilarity(INT_T i1, INT_T i2)
    {
      RatingVector &rv2 = itemRV[i2];
      vector<FLT_T> &quickRating1 = itemRtQuick[i1];
      vector<FLT_T> &quickRating2 = itemRtQuick[i2];
      DynBitSet &ubst1 = userBst[i1];

      FLT_T numerator = 0;
      FLT_T rtng1_sq = 0;
      FLT_T rtng2_sq = 0;

      INT_T users = 0;
      for(INT_T x = 0; x < rv2.size(); x++) {
        INT_T u2 = rv2[x].uId;
        if(!ubst1[u2])
          continue;

        numerator += quickRating1[u2] * quickRating2[u2];
        rtng1_sq += quickRating1[u2] * quickRating1[u2];
        rtng2_sq += quickRating2[u2] * quickRating2[u2];
        users++;
      }

      if(users == 0)
        return 0;

      FLT_T denominator = sqrt(rtng1_sq * rtng2_sq);

      FLT_T cosine_coeff = numerator / denominator;

      return cosine_coeff;
    }
#endif
