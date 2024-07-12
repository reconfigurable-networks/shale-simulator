#include "nodeid.hpp"
#include "defines.hpp"

void fill_array_with_coords(int *coords, NodeID id){
   auto node_id = id.id;
   for(int i = 0; i < NUM_PHASES; i++){
      coords[i] = node_id % NODES_PER_PHASE;
      node_id /= NODES_PER_PHASE;
   }
}

NodeID node_id_from_array(int *coords){
   int nid = 0;
   for(int i = NUM_PHASES - 1; i >= 0; i--){
      nid *= NODES_PER_PHASE;
      nid += coords[i];
   }
   NodeID id;
   id.id = nid;
   return id;
}

int extract_coord(NodeID id, int phase){
   int node_id = id.id;
   for(int i = 0; i < phase; i++){
      node_id /= NODES_PER_PHASE;
   }
   return node_id % NODES_PER_PHASE;
}

NodeID adjust_coord(NodeID id, int phase, int offset){
   int coords[NUM_PHASES];
   fill_array_with_coords(coords, id);
   coords[phase] += offset;
   coords[phase] %= NODES_PER_PHASE;
   return node_id_from_array(coords);
}

NodeID set_coord(NodeID id, int phase, int value){
   int coords[NUM_PHASES];
   fill_array_with_coords(coords, id);
   coords[phase] = value;
   coords[phase] %= NODES_PER_PHASE;
   return node_id_from_array(coords);
}

BucketID bucket_of(NodeID id, int rem_spray_hops){
   auto node_id = id.id;
   BucketID bucketid;
   bucketid.id = node_id + rem_spray_hops * MAX_NODE_ID;
   return bucketid;
}

std::ostream& operator<<(std::ostream &strm, const NodeID id){
   auto node_id = id.id;
   strm  << "[";
   for(int i = 0; i < NUM_PHASES-1; i++){
      strm << node_id % NODES_PER_PHASE << " ";
      node_id /= NODES_PER_PHASE;
   }
   return strm << node_id % NODES_PER_PHASE << "]";
}

std::ostream& operator<<(std::ostream &strm, const BucketID id){
   auto node_id = id.id;
   strm  << "[";
   for(int i = 0; i < NUM_PHASES; i++){
      strm << node_id % NODES_PER_PHASE << " ";
      node_id /= NODES_PER_PHASE;
   }
   return strm << "| " << node_id << "]";
}

BucketID bucket_of(NodeID id, int rem_spray_hops);

