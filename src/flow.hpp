#ifndef __FLOW_H
#define __FLOW_H

#include "nodeid.hpp"

typedef struct {
   int flow_id;
   NodeID source_id;
   NodeID dest_id;
   int num_frames;
   int quantized_num_frames;
   int remain_frames;
   int start_tick;
   double credit;
   int budget;
} Flow;

#endif
