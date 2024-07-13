#include "defines.hpp"
#include "node.hpp"
#include <iostream>
#include <fstream>
#include <climits>
#include <cassert>
#include <cstdlib>
#include <cmath>
#include <map>
#include <algorithm>
#include <numeric>
#include <mutex>

int happens_in_next_epoch (int cur_phase, int cur_link, int next_phase, int next_link);
std::mutex mtx;

//Node::Node (NodeID id) : random_generator(id.id),
Node::Node (NodeID id) : random_generator((std::random_device())()),
                         spray_distribution(0,LINKS_PER_PHASE-1)
{
   this->id = id;

   received_packet_queue.set_capacity(PROP_DELAY_TS + 1);
   received_tokens_queue.set_capacity(PROP_DELAY_TS + 1);
   received_rdc_queue.set_capacity(PROP_DELAY_TS + 1);

   adjacent_node = new Node**[NUM_PHASES];
   failed = false;
   link_failed = new bool*[NUM_PHASES];

   send_queue = new PriorityQueue*[NUM_PHASES];
   rdc_send_queue = new std::deque<RDControl>*[NUM_PHASES];
   token_queue = new std::deque<BucketID>*[NUM_PHASES];
   buckets = new std::map<BucketID,Bucket>*[NUM_PHASES];
   max_send_queue_length = new int*[NUM_PHASES];
   cur_enqueued_frames_per_link = new int*[NUM_PHASES];
   max_enqueued_frames_per_link = new int*[NUM_PHASES];
   cur_buffer_occupancy = 0;
   max_buffer_occupancy = 0;
   last_sent_flow = new std::list<Flow>::iterator*[NUM_PHASES];

   spray_order.resize(LINKS_PER_PHASE);
   std::iota(spray_order.begin(), spray_order.end(), 0);


   for (int x = 0; x < NUM_PHASES; x++) {
      adjacent_node[x] = new Node*[LINKS_PER_PHASE];
      link_failed[x] = new bool[LINKS_PER_PHASE]();
      send_queue[x] = new PriorityQueue[LINKS_PER_PHASE];
      rdc_send_queue[x] = new std::deque<RDControl>[LINKS_PER_PHASE];
      token_queue[x] = new std::deque<BucketID>[LINKS_PER_PHASE];
      buckets[x] = new std::map<BucketID,Bucket>[LINKS_PER_PHASE];
      max_send_queue_length[x] = new int[LINKS_PER_PHASE]();
      cur_enqueued_frames_per_link[x] = new int[LINKS_PER_PHASE]();
      max_enqueued_frames_per_link[x] = new int[LINKS_PER_PHASE]();
      last_sent_flow[x] = new std::list<Flow>::iterator[LINKS_PER_PHASE];

      for (int y = 0; y < LINKS_PER_PHASE; y++) {
         last_sent_flow[x][y] = currently_sending_flows.end();
      }
   }
}

Node::~Node () {
   for (Packet *packet : received_packet_queue) {
      delete packet;
   }

   for (int x = 0; x < NUM_PHASES; x++) {
      delete[] adjacent_node[x];
      delete[] send_queue[x];
      delete[] rdc_send_queue[x];
      delete[] max_send_queue_length[x];
      delete[] cur_enqueued_frames_per_link[x];
      delete[] max_enqueued_frames_per_link[x];
      delete[] last_sent_flow[x];
   }
   delete[] adjacent_node;
   delete[] send_queue;
   delete[] rdc_send_queue;
   delete[] max_send_queue_length;
   delete[] cur_enqueued_frames_per_link;
   delete[] max_enqueued_frames_per_link;
   delete[] last_sent_flow;
}

void Node::add_send_flow (Flow flow) {
   send_flows.push_back(flow);
}

void Node::add_recv_flow (Flow flow) {
   receive_flows[flow.flow_id] = flow;
   remaining_receive_frames[flow.flow_id] = flow.num_frames;
}

void Node::set_adjacent_nodes (std::vector<Node *> nodes) {
   for (int x = 0; x < NUM_PHASES; x++) {
      for (int y = 0; y < LINKS_PER_PHASE; y++) {
         auto adjacent_id = adjust_coord(id, x, y+1);
         adjacent_node[x][y] = nodes[adjacent_id];
      }
   }
}

void Node::send_packet (int cur_tick) {
   auto cur_phase = (cur_tick / LINKS_PER_PHASE) % NUM_PHASES;
   auto cur_link = cur_tick % LINKS_PER_PHASE;

   //start the next flow, if needed
   if (!failed && !send_flows.empty() && send_flows[0].start_tick <= cur_tick) {
      active_flows_with_dest[send_flows[0].dest_id.id]++;
      send_flows[0].credit = 1;
      send_flows[0].budget = RD_STARTING_BUDGET;
      currently_sending_flows.push_back(send_flows[0]);
      send_flows.pop_front();
   }

   Packet *packet_to_send = NULL;

   if (failed) {
      //just send a NULL packet
   } else if (link_failed[cur_phase][cur_link]) {
      //still send a NULL packet
      assert(send_queue[cur_phase][cur_link].empty());
      //get rid of this assert if we want to support nodes failing during the simulation...
   } else if (!send_queue[cur_phase][cur_link].empty()) {
      //Send the next packet in the send queue
      cur_enqueued_frames_per_link[cur_phase][cur_link]--;
      cur_buffer_occupancy--;

      auto bucket = send_queue[cur_phase][cur_link].top().second;
      assert(!buckets[cur_phase][cur_link][bucket].queue.empty());
      auto &packet_info = buckets[cur_phase][cur_link][bucket].queue.front();
      packet_to_send = packet_info.packet;

      //return token to original sender of packet
      if(USE_HBH && packet_info.bucket != INVALID_BUCKET) {
         token_queue[packet_info.sender_phase][packet_info.sender_link].push_back(packet_info.bucket);
      }

      send_queue[cur_phase][cur_link].pop();
      buckets[cur_phase][cur_link][bucket].queue.pop_front();

      //Spend a token if using HBH
      if(USE_HBH && bucket != DIRECT_TO_DEST_BUCKET) {
         assert(buckets[cur_phase][cur_link][bucket].num_outstanding_tokens < MAX_TOKENS_PER_BUCKET);
         buckets[cur_phase][cur_link][bucket].num_outstanding_tokens++;
      }

      //Re-enqueue the bucket if there are still tokens and queued frames remaining
      if(buckets[cur_phase][cur_link][bucket].num_outstanding_tokens < MAX_TOKENS_PER_BUCKET && !buckets[cur_phase][cur_link][bucket].queue.empty()) {
         enqueue_bucket_for_sending(bucket, cur_phase, cur_link, cur_tick);
      }

      //If we aren't using hop-by-hop congestion control, save memory by erasing buckets with empty queues
      if (!USE_HBH && buckets[cur_phase][cur_link][bucket].queue.empty()) {
         buckets[cur_phase][cur_link].erase(bucket);
      }

      auto cur_hop = packet_to_send->hops;
      packet_to_send->timestamp[cur_hop] = cur_tick;

   }
   else if (!packet_retransmit_queue.empty()) {
   //If there are any packets waiting to be retransmitted, send one of those
      packet_to_send = packet_retransmit_queue.front();
      packet_retransmit_queue.pop_front();
   }
   else if (!currently_sending_flows.empty()) {
   //If the send queue is empty, try to generate a new packet to send
      
      auto &flow = last_sent_flow[cur_phase][cur_link];

      for (int i = 0; i < currently_sending_flows.size(); i++) {
         if (flow == currently_sending_flows.end()) {
            flow = currently_sending_flows.begin();
         }

         auto dest = flow->dest_id;

         //Don't send a packet for a flow if we don't have credit for it yet.
         if (USE_FSR && flow->credit < 1) {
            flow++;
            continue;
         }
         if (USE_RD && flow->budget < 1) {
            flow++;
            continue;
         }

         // deal with tokens for HBH
         // note that we don't need a token to send directly to the destination.
         if (USE_HBH && adjacent_node[cur_phase][cur_link]->id != dest) {
            BucketID relevant_bucket = bucket_of(dest,NUM_PHASES-1);
            //check if we are about to allocate a new bucket
            if (!buckets[cur_phase][cur_link].count(relevant_bucket)) {
               if (!buckets_in_use[relevant_bucket]) {
                  cur_buckets_in_use++;
                  if (cur_buckets_in_use > max_buckets_in_use) {
                     max_buckets_in_use = cur_buckets_in_use;
                  }
               }
               buckets_in_use[relevant_bucket]++;
            }
            // don't send a packet for a flow if we don't have a token for it yet.
            if (buckets[cur_phase][cur_link][relevant_bucket].num_outstanding_tokens == MAX_TOKENS_FIRSTHOP_BUCKET) {
               flow++;
               continue;
            }
            //If do have remaining tokens, spend a token for this packet before continuing
            //Also record that we allocated a new bucket
            buckets[cur_phase][cur_link][relevant_bucket].num_outstanding_tokens++;
         }



         packet_to_send = new Packet();
         packet_to_send->src = id;
         packet_to_send->dest = flow->dest_id;
         packet_to_send->hops = 0;
         packet_to_send->generated_tick = cur_tick;
         packet_to_send->flow_id = flow->flow_id;
         packet_to_send->sequence_num = flow->num_frames - flow->remain_frames;
         packet_to_send->timestamp[0] = cur_tick;
         if (QUANTIZED_PRIO) {
            packet_to_send->flow_length = flow->quantized_num_frames;
         } else {
            packet_to_send->flow_length = flow->num_frames;
         }

	 sent_frames++;
         flow->remain_frames--;
         if (USE_FSR) {
            flow->credit--;
         }
         if (USE_RD) {
            flow->budget--;
         }

         if (flow->remain_frames == 0) {
            //clean up the now-finished flow
            auto finished_flow = flow;
            active_flows_with_dest[finished_flow->dest_id.id]--;
            for (int x = 0; x < NUM_PHASES; x++) {
               for (int y = 0; y < LINKS_PER_PHASE; y++) {
                  if(last_sent_flow[x][y] == finished_flow) {
                     last_sent_flow[x][y]++;
                  }
               }
            }
            finished_sending_flows.push_back(*finished_flow);
            currently_sending_flows.erase(finished_flow);
         } else {
            //start from the next flow next time a frame can be sent.
            flow++;
         }
         break;
      }
   }

   //Send packet to adjacent node.
   //This has to be run even if there is no packet in the send queue and no packet can be generated,
   //as even in this case a NULL packet must still be sent.
   adjacent_node[cur_phase][cur_link]->received_packet_queue.push_back(packet_to_send);
}

void Node::receive_packet (int cur_tick) {
   auto cur_phase = (cur_tick / LINKS_PER_PHASE) % NUM_PHASES;
   auto cur_link = cur_tick % LINKS_PER_PHASE;

   Packet *received_packet = received_packet_queue.front();
   received_packet_queue.pop_front();

   if (failed || !received_packet) {
      return;
   }

   received_packet->hops++;

   //Check if packet is destined to this node
   if (received_packet->dest == id) {
      receive_packet_destined_to_this_node(cur_tick, received_packet);
      return;
   }

   PacketInfo received_packet_info;
   received_packet_info.packet = received_packet;
   received_packet_info.sender_phase = cur_phase;
   received_packet_info.sender_link = LINKS_PER_PHASE - 1 - cur_link;
   int rem_spray = NUM_PHASES - received_packet->hops;
   rem_spray = rem_spray < 0 ? 0 : rem_spray;
   received_packet_info.bucket = bucket_of(received_packet->dest, rem_spray);
   if (USE_PRIO) {
      if(PRIO_LOG) {
         received_packet_info.priority = - (log2(received_packet->flow_length) * EPOCH_LENGTH * PRIO_FACTOR + cur_tick);
      } else {
         received_packet_info.priority = - (received_packet->flow_length * EPOCH_LENGTH * PRIO_FACTOR + cur_tick);
      }
   } else {
      received_packet_info.priority = - cur_tick;
   }

   if (received_packet->hops >= NUM_PHASES) {
      //this packet is done being sprayed
      receive_packet_to_be_forwarded(cur_tick, received_packet_info);
      return;
   }
   else {
      //this packet still needs to be sprayed
      receive_packet_to_be_sprayed(cur_tick, received_packet_info);
      return;
   }
}

void Node::receive_packet_destined_to_this_node(int cur_tick, Packet *received_packet) {
   total_frames_recvd++;

   auto flow_id = received_packet->flow_id;

   receive_flows[flow_id].remain_frames--;

   if (receive_flows[flow_id].remain_frames == 0) {
      auto duration = cur_tick - receive_flows[flow_id].start_tick + PROP_DELAY_TS + 1;
      completed_flows ++;
      std::lock_guard<std::mutex>lock(mtx);
      if(fct_csv.is_open()){
         fct_csv << flow_id << ",";
         fct_csv << receive_flows[flow_id].num_frames << ",";
         fct_csv << duration << ",";
         fct_csv << receive_flows[flow_id].start_tick << std::endl;
      }
   }
   else if (USE_RD && ((receive_flows[flow_id].num_frames - receive_flows[flow_id].remain_frames) % RD_CELLS_PER_PULL == 0)) {
      RDControl pull_to_send;
      pull_to_send.type = PULL;
      pull_to_send.src = id;
      pull_to_send.dest = receive_flows[flow_id].source_id;
      pull_to_send.hops = 0;
      pull_to_send.sequence_num = -1; //unused for PULL messages in this design
      pull_to_send.flow_id = flow_id;
      local_rdc_queue.push_back(pull_to_send);
   }
   delete received_packet;
}

void Node::receive_packet_to_be_forwarded(int cur_tick, PacketInfo received_packet_info) {
   auto cur_epoch = cur_tick / EPOCH_LENGTH;
   auto cur_phase = (cur_tick / LINKS_PER_PHASE) % NUM_PHASES;
   auto cur_link = cur_tick % LINKS_PER_PHASE;

   //checking phases in order, starting from the next phase, to find the first phase where the destination coordinate differs from the current node
   for(int sending_phase_offset = 1; sending_phase_offset <= NUM_PHASES; sending_phase_offset++) {
      int sending_phase = (cur_phase + sending_phase_offset) % NUM_PHASES;
      int packet_sending_coord = extract_coord(received_packet_info.packet->dest, sending_phase);
      int this_sending_coord = extract_coord(id, sending_phase);
      int offset_on_sending_phase = (packet_sending_coord - this_sending_coord + NODES_PER_PHASE) % NODES_PER_PHASE;
      if (offset_on_sending_phase == 0) {
         //this node matches dest on the current sending phase
         //therefore, move on to the next phase.
         received_packet_info.packet->hops++;
         received_packet_info.packet->timestamp[received_packet_info.packet->hops-1] = received_packet_info.packet->timestamp[received_packet_info.packet->hops-2];
         continue;
      }

      int sending_link = offset_on_sending_phase-1;

      assert(!link_failed[sending_phase][sending_link]);

      await_token(received_packet_info, sending_phase, sending_link, cur_tick);

      return;
   }
}

void Node::receive_packet_to_be_sprayed(int cur_tick, PacketInfo received_packet_info) {
   auto cur_epoch = cur_tick / EPOCH_LENGTH;
   auto cur_phase = (cur_tick / LINKS_PER_PHASE) % NUM_PHASES;
   auto cur_link = cur_tick % LINKS_PER_PHASE;

   int spray_phase = (cur_phase + 1) % NUM_PHASES;

   int selected_link = INT_MAX;

   //randomize order to check spraying hops
   std::shuffle(spray_order.begin(), spray_order.end(), random_generator);

   int rem_spray = NUM_PHASES - received_packet_info.packet->hops - 1;
   rem_spray = rem_spray < 0 ? 0 : rem_spray;

   if (SPRAY_SHORT) {
      // Check each queue and spray via the one with the shortest queue length.
      int selected_total_awaiting = INT_MAX;
      int selected_bucket_awaiting = INT_MAX;

      BucketID relevant_bucket = bucket_of(received_packet_info.packet->dest, rem_spray);


      for (int i = 0; i < LINKS_PER_PHASE; i++) {
         int check_link = spray_order[i];
         int check_total_awaiting = cur_enqueued_frames_per_link[spray_phase][check_link];
         int check_bucket_awaiting = 0;

         if (link_failed[spray_phase][check_link]) continue;
         if (rem_spray == 0 && direct_path_has_failed_node(spray_phase,check_link,received_packet_info.packet->dest)) continue;

         bool should_select_check_link;
         if (SPRAY_BUCKET) {
            if(buckets[spray_phase][check_link].count(relevant_bucket)) {
               check_bucket_awaiting = buckets[spray_phase][check_link][relevant_bucket].queue.size();
               check_bucket_awaiting += buckets[spray_phase][check_link][relevant_bucket].num_outstanding_tokens;
            }
            should_select_check_link = (check_bucket_awaiting < selected_bucket_awaiting) ||
                  (check_bucket_awaiting == selected_bucket_awaiting && check_total_awaiting < selected_total_awaiting);
         } else {
            should_select_check_link = check_total_awaiting < selected_total_awaiting;
         }

         if (should_select_check_link) {
            selected_link = check_link;
            selected_total_awaiting = check_total_awaiting;
            selected_bucket_awaiting = check_bucket_awaiting;
         }
      }
   } else {
      for (int selected_index = 0; selected_index < LINKS_PER_PHASE; selected_index++) {
         int check_link = spray_order[selected_index];
         if (link_failed[spray_phase][check_link]) continue;
         if (rem_spray == 0 && direct_path_has_failed_node(spray_phase,check_link,received_packet_info.packet->dest)) continue;
         selected_link = check_link;
         break;
      }
   }

   assert(selected_link < LINKS_PER_PHASE);

   await_token(received_packet_info, spray_phase, selected_link, cur_tick);

   return;
}

std::ostream& operator<<(std::ostream &strm, const RDControl msg){
   strm << "(";
   switch(msg.type) {
      case PULL:
         strm << "PULL";
         break;
      case DROP:
         strm << "DROP";
         break;
      case NACK:
         strm << "NACK";
         break;
      case INVALID:
         return strm << "invalid)";
   }
   return strm << "|" << msg.src << "->" << msg.dest << "|f-" << msg.flow_id << ")";
}

void Node::send_rdc (int cur_tick) {
   auto cur_phase = (cur_tick / LINKS_PER_PHASE) % NUM_PHASES;
   auto cur_link = cur_tick % LINKS_PER_PHASE;

   RDControl rdc_to_send;
   rdc_to_send.flow_id = INT_MAX;
   rdc_to_send.type = INVALID;

   if (rd_pacing_delay > 0) {
      rd_pacing_delay -= 1;
      if (rd_pacing_delay < 0) {
         rd_pacing_delay = 0;
      }
   }

   if (failed) {
      //just send a NULL packet
   } else if (link_failed[cur_phase][cur_link]) {
      //still send a NULL packet
      assert(rdc_send_queue[cur_phase][cur_link].empty());
      //get rid of this assert if we want to support nodes failing during the simulation...
   } else if (!rdc_send_queue[cur_phase][cur_link].empty()) {
      //Send the next pull in the send queue
      rdc_to_send = rdc_send_queue[cur_phase][cur_link].front();
      rdc_send_queue[cur_phase][cur_link].pop_front();
   }
   else if (!local_rdc_queue.empty() && rd_pacing_delay < 10) {
      //If the send queue is empty, try to send a pending local pull
      rdc_to_send = local_rdc_queue.front();
      local_rdc_queue.pop_front();

      //limit our PULL and NACK sending rate so that we don't request beyond our bandwidth guarantee
      if (rdc_to_send.type == PULL) {
         rd_pacing_delay += RD_CELLS_PER_PULL / RD_TARGET_BW_FACTOR;
      } else if (rdc_to_send.type == NACK) {
         rd_pacing_delay += 1 / RD_TARGET_BW_FACTOR;
      }
   }

   //Send pull to adjacent node.
   //This has to be run even if there is no packet in the send queue and no packet can be generated,
   //as even in this case a NULL packet must still be sent.
   adjacent_node[cur_phase][cur_link]->received_rdc_queue.push_back(rdc_to_send);
}

void Node::receive_rdc (int cur_tick) {
   auto cur_phase = (cur_tick / LINKS_PER_PHASE) % NUM_PHASES;
   auto cur_link = cur_tick % LINKS_PER_PHASE;

   RDControl received_rdc = received_rdc_queue.front();

   if (failed || received_rdc.type == INVALID) {
      return;
   }

   received_rdc.hops++;

   //Check if the pull is destined to this node
   if (received_rdc.dest == id) {
      receive_rdc_destined_to_this_node(cur_tick, received_rdc);
      return;
   }

   if (received_rdc.hops >= NUM_PHASES) {
      //this rdc is done being sprayed
      receive_rdc_to_be_forwarded(cur_tick, received_rdc);
      return;
   }
   else {
      //this rdc still needs to be sprayed
      receive_rdc_to_be_sprayed(cur_tick, received_rdc);
      return;
   }

   received_rdc_queue.pop_front();
}

void Node::receive_rdc_destined_to_this_node(int cur_tick, RDControl received_rdc) {
   if (received_rdc.type == PULL) {
      auto flow_id = received_rdc.flow_id;

      auto flow = std::find_if(currently_sending_flows.begin(), currently_sending_flows.end(), [=](Flow f){
         return f.flow_id == flow_id;
      });
      if (flow != currently_sending_flows.end()) {
         flow->budget += RD_CELLS_PER_PULL;
      }
   } else if (received_rdc.type == DROP) {
      RDControl nack_to_send;
      nack_to_send.type=NACK;
      nack_to_send.src = id;
      nack_to_send.dest = received_rdc.src;
      nack_to_send.hops = 0;
      nack_to_send.sequence_num = received_rdc.sequence_num;
      nack_to_send.flow_id = received_rdc.flow_id;
      local_rdc_queue.push_back(nack_to_send);
   } else if (received_rdc.type == NACK) {
      Packet *packet_to_resend = new Packet();
      packet_to_resend->src = id;
      packet_to_resend->dest = received_rdc.src;
      packet_to_resend->hops = 0;
      packet_to_resend->generated_tick = cur_tick;
      packet_to_resend->flow_id = received_rdc.flow_id;
      packet_to_resend->sequence_num = received_rdc.sequence_num;
      packet_to_resend->timestamp[0] = cur_tick;
      packet_retransmit_queue.push_back(packet_to_resend);
   }
}

void Node::receive_rdc_to_be_forwarded(int cur_tick, RDControl received_rdc) {
   auto cur_epoch = cur_tick / EPOCH_LENGTH;
   auto cur_phase = (cur_tick / LINKS_PER_PHASE) % NUM_PHASES;
   auto cur_link = cur_tick % LINKS_PER_PHASE;

   //checking phases in order, starting from the next phase, to find the first phase where the destination coordinate differs from the current node
   for(int sending_phase_offset = 1; sending_phase_offset <= NUM_PHASES; sending_phase_offset++) {
      int sending_phase = (cur_phase + sending_phase_offset) % NUM_PHASES;
      int rdc_sending_coord = extract_coord(received_rdc.dest, sending_phase);
      int this_sending_coord = extract_coord(id, sending_phase);
      int offset_on_sending_phase = (rdc_sending_coord - this_sending_coord + NODES_PER_PHASE) % NODES_PER_PHASE;
      if (offset_on_sending_phase == 0) {
         //this node matches dest on the current sending phase
         //therefore, move on to the next phase.
         received_rdc.hops++;
         continue;
      }

      int sending_link = offset_on_sending_phase-1;

      assert(!link_failed[sending_phase][sending_link]);

      rdc_send_queue[sending_phase][sending_link].push_back(received_rdc);

      return;
   }
}

void Node::receive_rdc_to_be_sprayed(int cur_tick, RDControl received_rdc) {
   auto cur_epoch = cur_tick / EPOCH_LENGTH;
   auto cur_phase = (cur_tick / LINKS_PER_PHASE) % NUM_PHASES;
   auto cur_link = cur_tick % LINKS_PER_PHASE;

   int spray_phase = (cur_phase + 1) % NUM_PHASES;

   int selected_link = INT_MAX;

   //randomize order to check spraying hops
   std::shuffle(spray_order.begin(), spray_order.end(), random_generator);

   int rem_spray = NUM_PHASES - received_rdc.hops - 1;
   rem_spray = rem_spray < 0 ? 0 : rem_spray;

   for (int selected_index = 0; selected_index < LINKS_PER_PHASE; selected_index++) {
      int check_link = spray_order[selected_index];
      if (link_failed[spray_phase][check_link]) continue;
      if (rem_spray == 0 && direct_path_has_failed_node(spray_phase,check_link,received_rdc.dest)) continue;
      selected_link = check_link;
      break;
   }

   assert(selected_link < LINKS_PER_PHASE);

   rdc_send_queue[spray_phase][selected_link].push_back(received_rdc);

   return;
}


bool Node::direct_path_has_failed_node (int phase, int link, NodeID dest_id) {
   NodeID cur_id = adjacent_node[phase][link]->id;
   int cur_phase = phase;

   while(cur_id != dest_id) {
      cur_phase += 1;
      cur_phase %= NUM_PHASES;
      cur_id = set_coord(cur_id, cur_phase, extract_coord(dest_id, cur_phase));
      if(is_failed_node[cur_id.id]) return true;
   }
   return false;
}


void Node::adjust_flow_credit (int cur_tick) {
   if (failed) return;
   for (Flow &flow : currently_sending_flows) {
      flow.credit += TOTAL_FSR / (double)active_flows_with_dest[flow.dest_id];
      if (flow.credit > MAX_FLOW_CREDIT) {
         flow.credit = MAX_FLOW_CREDIT;
      }
   }
}

void Node::send_tokens (int cur_tick) {
   auto cur_phase = (cur_tick / LINKS_PER_PHASE) % NUM_PHASES;
   auto cur_link = cur_tick % LINKS_PER_PHASE;

   adjacent_node[cur_phase][cur_link]->received_tokens_queue.push_back();
   auto &sent_tokens = adjacent_node[cur_phase][cur_link]->received_tokens_queue.back();

   if (failed) {
      for (int i = 0; i < TOKENS_PER_PACKET; i++) {
         sent_tokens.tokens[i] = INVALID_BUCKET;
      }
   }
   else {
      for(int i = 0; i < TOKENS_PER_PACKET; i++){
         if (!token_queue[cur_phase][cur_link].empty()) {
            sent_tokens.tokens[i] = token_queue[cur_phase][cur_link].front();
            token_queue[cur_phase][cur_link].pop_front();
         } else {
            sent_tokens.tokens[i] = INVALID_BUCKET;
         }
      }
   }
}

void Node::receive_tokens (int cur_tick) {
   auto cur_epoch = cur_tick / EPOCH_LENGTH;
   auto cur_phase = (cur_tick / LINKS_PER_PHASE) % NUM_PHASES;
   auto recvd_link = cur_tick % LINKS_PER_PHASE;
   int corr_link = LINKS_PER_PHASE - 1 - recvd_link;

   if (!failed) {
      for (BucketID bucket : received_tokens_queue.front().tokens) {
         if(bucket == INVALID_BUCKET) continue;
         assert(buckets[cur_phase][corr_link][bucket].num_outstanding_tokens > 0);

         buckets[cur_phase][corr_link][bucket].num_outstanding_tokens--;

         if (buckets[cur_phase][corr_link][bucket].num_outstanding_tokens == MAX_TOKENS_PER_BUCKET - 1 && !buckets[cur_phase][corr_link][bucket].queue.empty()) {
            enqueue_bucket_for_sending(bucket, cur_phase, corr_link, cur_tick);
         }
         if (buckets[cur_phase][corr_link][bucket].num_outstanding_tokens == 0 && buckets[cur_phase][corr_link][bucket].queue.empty()) {
            buckets[cur_phase][corr_link].erase(bucket);
            buckets_in_use[bucket]--;
            if (!buckets_in_use[bucket]) {
               buckets_in_use.erase(bucket);
               cur_buckets_in_use--;
            }
         }
      }
   }
   received_tokens_queue.pop_front();
}

void Node::await_token(PacketInfo packet_info, int send_phase, int send_link, int cur_tick) {
   // NOTE: await_token is called whenever we want to enqueue a packet, regardless of whether HBH is in use.
   // The code path is identical regardless of whether or not HBH is in use. Note that in send_packet(), tokens
   // are only spent if HBH is in use, so if HBH is not in use, the check for available tokens always succeeds.

   assert(!link_failed[send_phase][send_link]);

   if (USE_RD && RD_MAX_QUEUE_LENGTH > 0 && cur_enqueued_frames_per_link[send_phase][send_link] >= RD_MAX_QUEUE_LENGTH) {
      RDControl drop_to_send;
      drop_to_send.type = DROP;
      drop_to_send.src = packet_info.packet->src;
      drop_to_send.dest = packet_info.packet->dest;
      drop_to_send.hops = packet_info.packet->hops;
      drop_to_send.sequence_num = packet_info.packet->sequence_num;
      drop_to_send.flow_id = packet_info.packet->flow_id;
      rdc_send_queue[send_phase][send_link].push_back(drop_to_send);
      delete packet_info.packet;
      return;
   }

   cur_enqueued_frames_per_link[send_phase][send_link]++;
   cur_buffer_occupancy++;
   if(cur_enqueued_frames_per_link[send_phase][send_link] > max_enqueued_frames_per_link[send_phase][send_link]) {
      max_enqueued_frames_per_link[send_phase][send_link] = cur_enqueued_frames_per_link[send_phase][send_link];
   }
   if(cur_buffer_occupancy > max_buffer_occupancy) {
      max_buffer_occupancy = cur_buffer_occupancy;
   }

   int rem_spray = NUM_PHASES - packet_info.packet->hops - 1;
   rem_spray = rem_spray < 0 ? 0 : rem_spray;
   BucketID bucket = bucket_of(packet_info.packet->dest, rem_spray);

   //Tokens aren't needed when sending directly to the destination, so we have a special bucket for this case.
   if (adjacent_node[send_phase][send_link]->id == packet_info.packet->dest) {
      bucket = DIRECT_TO_DEST_BUCKET;
   }
   else if (USE_HBH) {
      //check if we are about to allocate a new bucket
      if (!buckets[send_phase][send_link].count(bucket)) {
         if (!buckets_in_use[bucket]) {
            cur_buckets_in_use++;
            if (cur_buckets_in_use > max_buckets_in_use) {
               max_buckets_in_use = cur_buckets_in_use;
            }
         }
         buckets_in_use[bucket]++;
      }
   }

   //Add the packet to the bucket's queue
   buckets[send_phase][send_link][bucket].queue.push_back(packet_info);

   //If the bucket has available tokens, we need to make sure it is in the send queue with the correct priority.
   //If this is the first packet in this bucket, we need to add the bucket to the send queue.
   //Otherwise, if there are already packets in the bucket, the bucket must already be in the send queue.
   //However, if this packet is now the lowest priority packet in the bucket, we need to update the priority of the bucket.
   if(bucket == DIRECT_TO_DEST_BUCKET || buckets[send_phase][send_link][bucket].num_outstanding_tokens < MAX_TOKENS_PER_BUCKET) {
      if (buckets[send_phase][send_link][bucket].queue.size() == 1) {
         enqueue_bucket_for_sending(bucket, send_phase, send_link, cur_tick);
      } else if (buckets[send_phase][send_link][bucket].queue.front().packet == packet_info.packet) {
         send_queue[send_phase][send_link].update(packet_info.priority, bucket);
      }
   }
}

void Node::enqueue_bucket_for_sending(BucketID bucket, int send_phase, int send_link, int cur_tick) {
   send_queue[send_phase][send_link].assert_does_not_contain(bucket);
   send_queue[send_phase][send_link].push({buckets[send_phase][send_link][bucket].queue.front().priority,bucket});

   //Queuing stats collection
   if(send_queue[send_phase][send_link].size() > max_send_queue_length[send_phase][send_link]) {
      max_send_queue_length[send_phase][send_link] = send_queue[send_phase][send_link].size();
   }
}

void Node::fail_node () {
   failed = true;
   is_failed_node[id.id] = true;
   for (int phase = 0; phase < NUM_PHASES; phase++) {
      for (int link = 0; link < LINKS_PER_PHASE; link++) {
         int neighbor_link = LINKS_PER_PHASE - link - 1;
         adjacent_node[phase][link]->link_failed[phase][neighbor_link] = true;
      }
   }
}

void Node::record_current_queue_lengths (std::ofstream &outfile) {
   if (failed) return;
   for(int x = 0; x < NUM_PHASES; x++) {
      for(int y = 0; y < LINKS_PER_PHASE; y++) {
         if(send_queue[x][y].size()) {
            outfile << id << "," << x << "," << y << "," << send_queue[x][y].size() << std::endl;
         }
      }
   }
}

void Node::record_max_queue_lengths (std::ofstream &outfile) {
   if (failed) return;
   for(int x = 0; x < NUM_PHASES; x++) {
      for(int y = 0; y < LINKS_PER_PHASE; y++) {
         if(max_send_queue_length[x][y]) {
            outfile << id << "," << x << "," << y << "," << max_send_queue_length[x][y] << std::endl;
         }
      }
   }
}

void Node::record_cur_enqueued_frames (std::ofstream &outfile) {
   if (failed) return;
   for(int x = 0; x < NUM_PHASES; x++) {
      for(int y = 0; y < LINKS_PER_PHASE; y++) {
         if(cur_enqueued_frames_per_link[x][y]) {
            outfile << id << "," << x << "," << y << "," << cur_enqueued_frames_per_link[x][y] << std::endl;
         }
      }
   }
}

void Node::record_max_enqueued_frames (std::ofstream &outfile) {
   if (failed) return;
   for(int x = 0; x < NUM_PHASES; x++) {
      for(int y = 0; y < LINKS_PER_PHASE; y++) {
         if(max_enqueued_frames_per_link[x][y]) {
            outfile << id << "," << x << "," << y << "," << max_enqueued_frames_per_link[x][y] << std::endl;
         }
      }
   }
}

void Node::add_max_queue_lengths (std::vector<int> &queue_lengths) {
   for(int x = 0; x < NUM_PHASES; x++) {
      for(int y = 0; y < LINKS_PER_PHASE; y++) {
         queue_lengths.push_back(max_enqueued_frames_per_link[x][y]);
      }
   }
}

void Node::add_max_buckets_in_use (std::vector<int> &buckets_in_use_vector) {
   if (failed) return;
   buckets_in_use_vector.push_back(max_buckets_in_use);	
}

void Node::record_cur_buckets_in_use (std::ofstream &outfile) {
   if (failed) return;
   outfile << id << "," << cur_buckets_in_use << std::endl;
}

void Node::record_max_buckets_in_use (std::ofstream &outfile) {
   if (failed) return;
   outfile << id << "," << max_buckets_in_use << std::endl;
}

void Node::add_max_buffer_occupancy (std::vector<int> &buffer_occupancies) {
   if (failed) return;
   buffer_occupancies.push_back(max_buffer_occupancy);	
}

void Node::record_cur_buffer_occupancy (std::ofstream &outfile) {
   if (failed) return;
   outfile << id << "," << cur_buffer_occupancy << std::endl;
}

void Node::record_max_buffer_occupancy (std::ofstream &outfile) {
   if (failed) return;
   outfile << id << "," << max_buffer_occupancy << std::endl;
}

void Node::record_incomplete_flows (std::ofstream &outfile, int cur_tick) {
   if (failed) return;
   for(const auto & [flow_id, flow] : receive_flows) {
      if(flow.remain_frames > 0 && flow.remain_frames < flow.num_frames) {
         outfile << flow_id << ",";
         outfile << flow.num_frames - flow.remain_frames << ",";
         outfile << cur_tick - flow.start_tick + PROP_DELAY_TS + 1 << ",";
         outfile << flow.num_frames << std::endl;
      }
   }
}

int happens_in_next_epoch (int cur_phase, int cur_link, int next_phase, int next_link) {
   if (next_phase > cur_phase) return 0;
   if (next_phase < cur_phase) return 1;
   if (next_link > cur_link) return 0;
   return 1;
}

void PriorityQueue::update(int new_priority, BucketID bucket) {
   auto element = std::find_if(this->c.begin(),this->c.end(), [=](std::pair<int,BucketID> pair){return pair.second == bucket;});
   assert(element != this->c.end());
   element->first = new_priority;
   std::make_heap(this->c.begin(), this->c.end(), this->comp);
}

void PriorityQueue::assert_does_not_contain(BucketID bucket) {
   auto element = std::find_if(this->c.begin(),this->c.end(), [=](std::pair<int,BucketID> pair){return pair.second == bucket;});
   assert(element == this->c.end());
}
