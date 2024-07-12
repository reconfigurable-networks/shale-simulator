#include <numeric>
#include <vector>
#include <cmath>
#include <algorithm>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include "util.hpp"

double Lerp(double v0, double v1, double t) {
   return (1 - t)*v0 + t*v1;
}

std::vector<double> quantiles(const std::vector<int>& inData, const std::vector<double>& probs) {
   std::vector<int> data{};
   if (inData.empty()) {
      data.push_back(0);
      data.push_back(0);
   } else if (1 == inData.size()) {
      data.push_back(inData[0]);
      data.push_back(inData[0]);
   } else {
      data = inData;
   }

   std::sort(data.begin(), data.end());
   std::vector<double> quants;

   for (size_t i = 0; i < probs.size(); ++i) {
      double poi = Lerp(-0.5, data.size() - 0.5, probs[i]);

      size_t left = std::max(int64_t(std::floor(poi)), int64_t(0));
      size_t right = std::min(int64_t(std::ceil(poi)), int64_t(data.size() - 1));

      double datLeft = data.at(left);
      double datRight = data.at(right);

      double quantile = Lerp(datLeft, datRight, poi - left);

      quants.push_back(quantile);
   }

   return quants;
}

double mean(const std::vector<int>& data) {
   using namespace boost::accumulators;

   accumulator_set<double, stats<tag::mean>> acc;

   acc = std::for_each(data.begin(), data.end(), acc);
   return boost::accumulators::mean(acc);
}
