#ifndef __nodeid_h
#define __nodeid_h

#include <iostream>
extern int MAX_NODE_ID;

typedef struct NodeID {
   int id;
   operator int() const {
      return id;
   }
} NodeID;

typedef struct BucketID {
   int id;
   operator int() const {
      return id;
   }
   friend bool operator<(const BucketID& l, const BucketID& r) {
      return l.id < r.id;
   }
} BucketID;

int extract_coord(NodeID id, int phase);
NodeID adjust_coord(NodeID id, int phase, int offset);
NodeID set_coord(NodeID id, int phase, int value);
BucketID bucket_of(NodeID id, int rem_spray_hops);

NodeID node_id_from_array(int *coords);

std::ostream& operator<<(std::ostream &strm, const NodeID id);
std::ostream& operator<<(std::ostream &strm, const BucketID id);

#endif
