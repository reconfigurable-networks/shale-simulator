#ifndef __UTIL_H
#define __UTIL_H

std::vector<double> quantiles(const std::vector<int>& inData, const std::vector<double>& probs);

double mean(const std::vector<int>& data);

#endif
