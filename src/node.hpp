#ifndef __NODE_H
#define __NODE_H

#include <map>
#include <list>
#include <vector>
#include <boost/circular_buffer.hpp>
#include <queue>
#include <random>
#include "nodeid.hpp"
#include "flow.hpp"

typedef struct {
   NodeID src;
   NodeID dest;
   int hops;
   int generated_tick;
   int sequence_num;
   int flow_id;
   int timestamp[MAX_PHASES*2];
   int flow_length;
} Packet;

typedef struct {
   Packet *packet;
   int sender_phase;
   int sender_link;
   BucketID bucket;
   int priority;
} PacketInfo;

typedef struct {
   BucketID tokens[TOKENS_PER_PACKET];
} PacketTokens;

typedef enum {PULL, DROP, NACK, INVALID} RDCType;

typedef struct {
   RDCType type;
   NodeID src;
   NodeID dest;
   int hops;
   int sequence_num;
   int flow_id;
} RDControl;

std::ostream& operator<<(std::ostream &strm, const RDControl id);

typedef struct {
   int num_outstanding_tokens;
   std::deque<PacketInfo> queue;
} Bucket;

class PriorityQueue : public std::priority_queue<std::pair<int,BucketID>, std::vector<std::pair<int,BucketID>>> {
   public:
   void update(int new_priority, BucketID bucket);
   void assert_does_not_contain(BucketID bucket);
};

class Node {
public:
   NodeID id;
   bool failed;
private:
   Node ***adjacent_node;

   std::deque<Flow> send_flows;
   std::list<Flow> currently_sending_flows;
   std::list<Flow> finished_sending_flows;
   std::list<Flow>::iterator **last_sent_flow;

   std::map<int,Flow> receive_flows;
   std::map<int,int> remaining_receive_frames;

   int currently_receiving_long_flow_num;

   bool **link_failed;

   PriorityQueue **send_queue;
   std::deque<BucketID> **token_queue;
   std::map<BucketID,Bucket> **buckets;
   int **max_send_queue_length;
   int **cur_enqueued_frames_per_link;
   int **max_enqueued_frames_per_link;
   int cur_buffer_occupancy;
   int max_buffer_occupancy;

   std::deque<RDControl> **rdc_send_queue;
   std::deque<RDControl> local_rdc_queue;
   double rd_pacing_delay = 0;
   std::deque<Packet*> packet_retransmit_queue;

   boost::circular_buffer<Packet*> received_packet_queue;
   boost::circular_buffer<PacketTokens> received_tokens_queue;
   boost::circular_buffer<RDControl> received_rdc_queue;

   int sent_frames;
   std::map<BucketID,int> buckets_in_use;
   int cur_buckets_in_use;
   int max_buckets_in_use;

   std::mt19937 random_generator;
   std::vector<int> spray_order;
   std::uniform_int_distribution<int> spray_distribution;

   public:
   int credit_interval;

   Node (NodeID id);
   void add_send_flow (Flow flow);
   void add_recv_flow (Flow flow);
   void set_adjacent_nodes (std::vector<Node *> nodes);

   void fail_node ();

   void send_packet (int cur_tick);
   void receive_packet (int cur_tick);

   void adjust_flow_credit (int cur_tick);

   void send_tokens (int cur_tick);
   void receive_tokens (int cur_tick);

   void send_rdc (int cur_tick);
   void receive_rdc (int cur_tick);

   void record_current_queue_lengths (std::ofstream &outfile);
   void record_max_queue_lengths (std::ofstream &outfile);
   void add_max_queue_lengths (std::vector<int> &queue_lengths);
   void add_max_buffer_occupancy (std::vector<int> &buffer_occupancies);
   void record_cur_enqueued_frames (std::ofstream &outfile);
   void record_max_enqueued_frames (std::ofstream &outfile);
   void record_cur_buffer_occupancy (std::ofstream &outfile);
   void record_max_buffer_occupancy (std::ofstream &outfile);
   void record_incomplete_flows (std::ofstream &outfile, int cur_tick);

   void add_max_buckets_in_use (std::vector<int> &buckets_in_use_vector);
   void record_cur_buckets_in_use (std::ofstream &outfile);
   void record_max_buckets_in_use (std::ofstream &outfile);

   ~Node();

   private:
   void await_token (PacketInfo packet_info, int send_phase, int send_link, int cur_tick);
   void enqueue_bucket_for_sending (BucketID bucket, int send_phase, int send_link, int cur_tick);

   void receive_packet_destined_to_this_node (int cur_tick, Packet *received_packet);
   void receive_packet_to_be_forwarded (int cur_tick, PacketInfo received_packet_info);
   void receive_packet_to_be_sprayed (int cur_tick, PacketInfo received_packet_info);

   void receive_rdc_destined_to_this_node (int cur_tick, RDControl received_rdc);
   void receive_rdc_to_be_forwarded (int cur_tick, RDControl received_rdc);
   void receive_rdc_to_be_sprayed (int cur_tick, RDControl received_rdc);
public:
   bool direct_path_has_failed_node (int phase, int link, NodeID dest_id);
};


#endif
