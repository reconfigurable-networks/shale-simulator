#ifndef __DEFINES_H
#define __DEFINES_H

#include <atomic>
#include <fstream>
#include "nodeid.hpp"

extern std::atomic_int completed_flows;
extern std::atomic_uint64_t total_frames_recvd;
extern std::atomic_int *active_flows_with_dest;
extern std::ofstream fct_csv;

#define LINKS_PER_PHASE (NODES_PER_PHASE - 1)
#define EPOCH_LENGTH (LINKS_PER_PHASE * NUM_PHASES)

#define MAX_PHASES 4
#define TOKENS_PER_PACKET 2

extern int NUM_PHASES;
extern int NODES_PER_PHASE;
extern int PROP_DELAY_TS;
extern const BucketID INVALID_BUCKET;
extern BucketID DIRECT_TO_DEST_BUCKET;

extern int MAX_TOKENS_PER_BUCKET;
extern int MAX_TOKENS_FIRSTHOP_BUCKET;

extern bool USE_FSR;
extern bool USE_HBH;
extern bool USE_PRIO;
extern bool QUANTIZED_PRIO;
extern bool SPRAY_SHORT;
extern bool SPRAY_BUCKET;
extern bool USE_RD;

extern double PRIO_FACTOR;
extern bool PRIO_LOG;

extern int RD_CELLS_PER_PULL;
extern int RD_STARTING_BUDGET;
extern double RD_TARGET_BW_FACTOR;
extern int RD_MAX_QUEUE_LENGTH;

extern double TOTAL_FSR;

extern bool *is_failed_node;

#define MAX_FLOW_CREDIT 4.0

#endif
