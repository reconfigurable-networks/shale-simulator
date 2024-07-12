#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <boost/program_options.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/tee.hpp>
#include <boost/lexical_cast.hpp>
#include <cmath>
#include <filesystem>
#include <algorithm>
#include <atomic>
#include <vector>
#include <cstdlib>
#include <chrono>
#include <ctime>
#include <numeric>
#include <execution>
#include <array>
#include "defines.hpp"
#include "nodeid.hpp"
#include "flow.hpp"
#include "node.hpp"
#include "util.hpp"
#include <sys/time.h>
#include <sys/resource.h>

using namespace std;
namespace po = boost::program_options;

std::atomic_int completed_flows = 0;
std::atomic_uint64_t total_frames_recvd = 0;
std::ofstream fct_csv;
std::atomic_int *active_flows_with_dest;

int NUM_PHASES = 3;
int NODES_PER_PHASE = 16;
int PROP_DELAY_TS = 0;
int MAX_NODE_ID = 4096;
const BucketID INVALID_BUCKET = {INT_MAX};
BucketID DIRECT_TO_DEST_BUCKET = {MAX_NODE_ID * NUM_PHASES + 1};

int MAX_TOKENS_PER_BUCKET = 1;
int MAX_TOKENS_FIRSTHOP_BUCKET = 1;

bool USE_FSR;
bool USE_HBH;
bool USE_PRIO;
bool QUANTIZED_PRIO;
bool SPRAY_SHORT;
bool SPRAY_BUCKET;
bool USE_RD;

double PRIO_FACTOR = 1;
bool PRIO_LOG;

int RD_CELLS_PER_PULL;
int RD_STARTING_BUDGET;
double RD_TARGET_BW_FACTOR;
int RD_MAX_QUEUE_LENGTH;

double TOTAL_FSR = 1;

double TSFRAC = 1;

bool *is_failed_node;

void fail_n_nodes (int num_to_fail, std::vector<Node *> nodes);
void coordinate_loop (std::vector<int> &idxs_to_fail, int *coords, const int cur_coord, const int sum, const int sum_so_far);


int main(int argc, const char *argv[]) {
   ifstream testcase;
   std::filesystem::path output_dir;

   typedef boost::iostreams::tee_device<std::ostream, std::ofstream> Tee;
   typedef boost::iostreams::stream<Tee> TeeStream;
   std::ofstream logfile;

   po::options_description desc{"Options"};
   desc.add_options()
      ("help,h", "Show this help")
      ("input,i", po::value<string>(), "Filename of test case (required)")
      ("output,o", po::value<string>(), "Output directory")
      ("payload-length,p", po::value<int>()->default_value(52), "Payload length in bytes")
      ("slot-length,s", po::value<double>()->default_value(5.632e-9), "Timeslot length in seconds")
      ("propagation-delay,d", po::value<double>()->default_value(0), "Propagation delay in seconds")
      ("num-phases,l", po::value<int>()->default_value(3), "Value of tuning parameter h")
      ("num-nodes,n", po::value<int>()->default_value(4096), "Total number of nodes to simulate (including failed nodes)")
      ("max-ticks,t", po::value<int>()->default_value(0), "Maximum number of timeslots to simulate. 0 = unlimited")
      ("max-flows,f", po::value<int>()->default_value(0), "Maximum number of flows to finish before terminating simulation. 0 = unlimited")
      ("max-flows-read", po::value<int>()->default_value(0), "Maximum number of flows to read from the input file. 0 = unlimited")
      ("num-failed-nodes,F", po::value<int>()->default_value(0), "Number of failed nodes to simulate. Note: workload must not use node IDs above n-F (i.e. failed nodes must not be included in the workload)")
      ("flow-size-multiplier,X", po::value<double>()->default_value(1), "Multiplier by which to adjust flow sizes")
      ("load-factor-adjust,L", po::value<double>()->default_value(1), "Value by which to divide flow start times (thus adjusting load)")
      ("max-flow-size,m", po::value<int>()->default_value(0), "Ignore flows with size above this argument. 0 = disabled")
      ("min-flow-size,M", po::value<int>()->default_value(0), "Ignore flows with size below this argument. 0 = disabled")
      ("hop-by-hop,H", po::bool_switch(&USE_HBH), "Use hop-by-hop congestion control")
      ("tokens-per-bucket", po::value<int>()->default_value(1), "Number of tokens each bucket starts with for hop-by-hop congestion control")
      ("tokens-per-firsthop-bucket,T", po::value<int>()->default_value(1), "Number of tokens each bucket starts with for hop-by-hop congestion control, for buckets corresponding to the first hop. Note: if tokens-per-bucket is greater, it will overwrite this value.")
      ("fair-sending-rate,R", po::value<double>()->default_value(0), "Have nodes telepathically limit sending rate in case of incast. 0 = disabled")
      ("receiver-driven,N", po::bool_switch(&USE_RD), "Use receiver-driven transport")
      ("rd-cells-per-pull", po::value<int>()->default_value(10), "For receiver-driven transport, number of cells requested each time the receiver sends a PULL to the sender")
      ("rd-starting-budget", po::value<int>()->default_value(0), "For receiver-driven transport, number of cells the sender can send before receiving the first PULL from the receiver. If 0, the starting budget is calculated based on the value of h and the propagation delay.")
      ("rd-target-bw-fraction", po::value<double>()->default_value(1), "For receiver-driven transport, how quickly the receiver should request flow (with PULL and NACK messages) as a fraction of line rate.")
      ("rd-max-queue-length", po::value<int>()->default_value(0), "Maximum queue length before packet trimming. When using receiver-driven transport, if enqueueing a cell would cause a queue to exceed this length, the cell is trimmed and a DROP message is sent to the destination instead. Note that this length only applies to cell queues, not to receiver-driven control messages which use separate unbounded queues.")
      ("prioritization,P", po::bool_switch(&USE_PRIO), "Use prioritization")
      ("quantized-prioritization,Q", po::bool_switch(&QUANTIZED_PRIO), "Use quantized prioritization")
      ("prio-factor,x", po::value<double>()->default_value(1), "Factor by which to multiply flow sizes for prioritization")
      ("prio-log", po::bool_switch(&PRIO_LOG)->default_value(0), "Use the log of the flow size for prioritization")
      ("spray-via-shortest,S", po::bool_switch(&SPRAY_SHORT), "Spray via the shortest outgoing queue (breaking ties randomly)")
      ("spray-via-shortest-bucket,B", po::bool_switch(&SPRAY_BUCKET), "Spray via the outgoing queue with the greatest number of remaining tokens, breaking ties by the shortest overall length (requires -S to be set)")
      ("timeslot-fraction", po::value<double>()->default_value(1), "For interleaving, fraction of timeslots allocated to the current schedule")
      ;


   po::variables_map vm;
   po::store(parse_command_line(argc, argv, desc), vm);
   po::notify(vm);

   if(vm.count("help")){
      cerr << desc << endl;
      return 0;
   }
   if(!vm.count("input")) {
      cerr << desc << endl;
      return 1;
   }

   bool logging = false;
   if(!vm.count("output")) {
      cerr << "Warning: no output will be saved" << endl;
      logfile.open("/dev/null");
   } else {
      logging = true;
      output_dir = vm["output"].as<string>();
      if (std::filesystem::exists(output_dir / "stats")) {
         cerr << "Error: output directory appears to contain a completed run. Program will exit." << endl;
         return -1;
      }

      std::filesystem::create_directories(output_dir);

      if(std::filesystem::exists(output_dir / "fct.csv")) {
         const std::time_t now = std::time(nullptr);
         std::filesystem::rename(output_dir / "fct.csv",
                  output_dir / ("fct.csv-"+boost::lexical_cast<std::string>(now)));
      }

      fct_csv.open(output_dir / "fct.csv");
      if(!fct_csv.is_open()) {
         cerr << "Error: could not open file " << output_dir / "fct.csv" << " for writing" << endl;
         exit(EXIT_FAILURE);
      }
      logfile.open(output_dir / "log");
      if(!logfile.is_open()) {
         cerr << "Error: could not open file " << output_dir / "log" << " for writing" << endl;
         exit(EXIT_FAILURE);
      }
   }
   
   Tee tee(std::cout, logfile);
   TeeStream logged_cout(tee);
   Tee tee2(std::cerr, logfile);
   TeeStream logged_cerr(tee2);

   for (int i = 0; i < argc; ++i) logged_cout << argv[i] << " ";
   logged_cout << std::endl;

   if(logging) {
      logged_cout << "Output directory: " << vm["output"].as<string>() << endl;
   }

   logged_cout << "Test case filename: " << vm["input"].as<string>() << endl;
   testcase.open(vm["input"].as<string>());
   if (!testcase.is_open()) {
      logged_cerr << "Error: could not open file " << vm["input"].as<string>() << endl;
      exit(EXIT_FAILURE);
   }

   TSFRAC = vm["timeslot-fraction"].as<double>();
   if(TSFRAC <= 0 || TSFRAC > 1) {
      logged_cerr << "Error: prioritization factor must be greater than 0 and at most 1." << endl;
      exit(EXIT_FAILURE);
   }

   int PAYLOAD_LENGTH = vm["payload-length"].as<int>();
   double SLOT_LENGTH_INCL_GB = vm["slot-length"].as<double>();
   SLOT_LENGTH_INCL_GB /= TSFRAC;
   double prop_delay_seconds = vm["propagation-delay"].as<double>();
   if(prop_delay_seconds < 0) {
      logged_cerr << "Warning: provided propagation delay was negative, using 0 instead" << endl;
      prop_delay_seconds = 0;
   }
   PROP_DELAY_TS = ceil(prop_delay_seconds / SLOT_LENGTH_INCL_GB);


   NUM_PHASES = vm["num-phases"].as<int>();
   if(NUM_PHASES > MAX_PHASES) {
      logged_cerr << "Error: num phases exceeds the maximum (set during compile time)" << endl;
      exit(EXIT_FAILURE);
   }
   int num_nodes = vm["num-nodes"].as<int>();
   NODES_PER_PHASE = ceil(pow(num_nodes, 1.0 / (double)NUM_PHASES));
   MAX_NODE_ID = pow(NODES_PER_PHASE,NUM_PHASES);
   DIRECT_TO_DEST_BUCKET = {MAX_NODE_ID * NUM_PHASES + 1};

   active_flows_with_dest = new std::atomic_int[MAX_NODE_ID]();

   int max_flows = vm["max-flows"].as<int>();
   if(max_flows == 0) max_flows = INT_MAX;
   int max_ticks = vm["max-ticks"].as<int>();
   max_ticks *= TSFRAC;
   if(max_ticks == 0) max_ticks = INT_MAX;
   int max_flows_read = vm["max-flows-read"].as<int>();
   if(max_flows_read == 0) max_flows_read = INT_MAX;
   double flow_size_multiplier = vm["flow-size-multiplier"].as<double>();
   double load_factor = vm["load-factor-adjust"].as<double>();

   std::vector<uint64_t> total_frames_recvd_M;
   total_frames_recvd_M.push_back(0);


   int min_flow_size = vm["min-flow-size"].as<int>();
   int max_flow_size = vm["max-flow-size"].as<int>();
   if(max_flow_size == 0) max_flow_size = INT_MAX;


   MAX_TOKENS_PER_BUCKET = vm["tokens-per-bucket"].as<int>();
   if(MAX_TOKENS_PER_BUCKET <= 0) {
      logged_cerr << "Error: must have at least one token per bucket." << endl;
      exit(EXIT_FAILURE);
   }

   MAX_TOKENS_FIRSTHOP_BUCKET = vm["tokens-per-firsthop-bucket"].as<int>();
   if(MAX_TOKENS_FIRSTHOP_BUCKET < MAX_TOKENS_PER_BUCKET) {
      MAX_TOKENS_FIRSTHOP_BUCKET = MAX_TOKENS_PER_BUCKET;
   }

   TOTAL_FSR = vm["fair-sending-rate"].as<double>();
   if(TOTAL_FSR == 0) {
      USE_FSR = 0;
   } else {
      USE_FSR = 1;
   }
   if (TOTAL_FSR < 0) {
      logged_cerr << "Error: sending rate limit cannot be negative." << endl;
      exit(EXIT_FAILURE);
   }


   PRIO_FACTOR = vm["prio-factor"].as<double>();
   if(PRIO_FACTOR <= 0) {
      logged_cerr << "Error: prioritization factor must be positive." << endl;
      exit(EXIT_FAILURE);
   }

   RD_CELLS_PER_PULL = vm["rd-cells-per-pull"].as<int>();
   RD_STARTING_BUDGET = vm["rd-starting-budget"].as<int>();
   RD_TARGET_BW_FACTOR = vm["rd-target-bw-fraction"].as<double>();
   if (RD_STARTING_BUDGET == 0) {
      RD_STARTING_BUDGET = 4 * (PROP_DELAY_TS + LINKS_PER_PHASE) * NUM_PHASES * RD_TARGET_BW_FACTOR;
   }
   RD_MAX_QUEUE_LENGTH = vm["rd-max-queue-length"].as<int>();
   if (RD_MAX_QUEUE_LENGTH < 0) {
      RD_MAX_QUEUE_LENGTH = 0;
   }

   if(USE_RD) {
      logged_cout << "Using receiver-driven transport with parameters:";
      logged_cout << "   cells-per-pull = " << RD_CELLS_PER_PULL;
      logged_cout << "   starting-budget = " << RD_STARTING_BUDGET;
      logged_cout << "   target-bw-fraction = " << RD_TARGET_BW_FACTOR;
      logged_cout << "   max-queue-length = " << RD_MAX_QUEUE_LENGTH;
      logged_cout << std::endl;
   }

   auto exec_start_time = std::chrono::system_clock::now();


   std::vector<Node *> nodes;
   nodes.resize(MAX_NODE_ID);
   for(int i = 0; i < MAX_NODE_ID; i++){
      NodeID id = {i};
      nodes[i] = new Node(id);
      nodes[i]->credit_interval = 2*NUM_PHASES;
   }
   for(int i = 0; i < MAX_NODE_ID; i++){
      nodes[i]->set_adjacent_nodes(nodes);
   }

   is_failed_node = new bool[MAX_NODE_ID]();

   int num_failed_nodes = vm["num-failed-nodes"].as<int>();
   fail_n_nodes (num_failed_nodes, nodes);
   logged_cout << "Failed " << num_failed_nodes << " nodes" << std::endl;
   int num_good_nodes = MAX_NODE_ID - num_failed_nodes;

   std::vector<int> ttable({});
   for (int i = 0; i < MAX_NODE_ID; i++) {

      if (nodes[i]->failed) continue;

      ttable.push_back(i);
   }
   assert(ttable.size() == num_good_nodes);
   logged_cout << "Num remaining nodes: " << num_good_nodes << " nodes" << std::endl;


//   std::vector<int> quantization_vector{0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 96, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168, 174, 186, 192, 204, 210, 222, 228, 240, 252, 264, 276, 288, 300, 312, 330, 342, 360, 377, 389, 407, 431, 449, 467, 491, 509, 533, 557, 581, 611, 635, 665, 695, 725, 760, 796, 832, 868, 904, 946, 988, 1036, 1084, 1131, 1179, 1233, 1287, 1347, 1407, 1472, 1538, 1604, 1676, 1754, 1831, 1915, 1999, 2089, 2185, 2286, 2388, 2496, 2603, 2723, 2843, 2974, 3106, 3244, 3393, 3543, 3704, 3872, 4045, 4225, 4416, 4614, 4817, 5039, 5260, 5499, 5745, 6002, 6271, 6559, 6852, 7157, 7480, 7815, 8168, 8533, 8916, 9317, 9736, 10173, 10627, 11106, 11603, 12123, 12668, 13236, 13835, 14451, 15103, 15779, 16485, 17227, 17999, 18807, 19657, 20536, 21458, 22421, 23426, 24479, 25580, 26729, 27926, 29183, 30493, 31857, 33287, 34783, 36345, 37978, 39684, 41461, 43322, 45266, 47301, 49425, 51645, 53961, 56384, 58915, 61560, 64318, 67208, 70224, 73377, 76674, 80115, 83711, 87469, 91394, 95493, 99783, 104259, 108938, 113833, 118943, 124280, 129857, 135685, 141776, 148143, 154790, 161737, 169001, 176589, 184511, 192792, 201451, 210492, 219940, 229813, 240129, 250905, 262166, 273936, 286232, 299079, 312506, 326532, 341192, 356504, 372510, 389228, 406700, 424956, 444032, 463957, 484786, 506549, 529286, 553041, 577867, 603806, 630906, 659226, 688815, 719739, 752044, 785798, 821071, 857924, 896435, 936674, 978715, 1022647, 1068553, 1116518, 1166630, 1218999, 1273719, 1330886, 1390627, 1453048, 1518269, 1586422, 1657633, 1732033, 1809784, 1891018, 1975895, 2064590, 2157264, 2254091, 2355274, 2460992, 2571455, 2686879, 2807485, 2933506, 3065181, 3202768, 3346530, 3496742, 3653698, 3817703, INT_MAX};
   std::vector<int> quantization_vector{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 27, 28, 30, 31, 33, 35, 37, 39, 41, 43, 45, 48, 50, 53, 56, 59, 62, 65, 69, 72, 76, 80, 84, 89, 94, 99, 104, 109, 115, 121, 128, 135, 142, 149, 157, 165, 174, 184, 193, 204, 214, 226, 238, 250, 264, 278, 292, 308, 324, 341, 359, 378, 399, 420, 442, 465, 490, 516, 543, 572, 602, 634, 668, 703, 741, 780, 821, 865, 911, 959, 1010, 1063, 1120, 1179, 1241, 1307, 1376, 1449, 1526, 1607, 1692, 1782, 1876, 1975, 2080, 2190, 2306, 2428, 2557, 2692, 2835, 2985, 3143, 3310, 3485, 3670, 3864, 4069, 4284, 4511, 4750, 5002, 5267, 5546, 5839, 6148, 6474, 6817, 7178, 7558, 7959, 8380, 8824, 9291, 9783, 10301, 10847, 11421, 12026, 12663, 13334, 14040, 14784, 15566, 16391, 17259, 18173, 19135, 20149, 21216, 22339, 23523, 24768, 26080, 27461, 28916, 30447, 32059, 33757, 35545, 37427, 39409, 41497, 43694, 46008, 48445, 51010, 53712, 56556, 59551, 62705, 66026, 69522, 73204, 77081, 81163, 85461, 89987, 94753, 99771, 105055, 110618, 116476, 122645, 129140, 135979, 143180, 150763, 158747, 167154, 176006, 185327, 195142, 205476, 216358, 227816, 239881, 252584, 265961, 280046, 294877, 310493, 326936, 344250, 362481, 381677, 401890, 423174, 445584, 469182, 494029, 520192, 547740, 576747, 607291, 639452, 673316, 708974, 746520, 786055, 827683, 871515, 917669, 966268, 1017440, 1071321, 1128057, 1187797, 1250700, 1316935, 1386678, 1460114, 1537439, 1618859, 1704592, 1794864, 1889917, 1990003, 2095391, 2206359, 2323204, 2446237, 2575785, 2712195, 2855828, 3007067, 3166317, 3333999, 3510562, 3696475, 3892234, INT_MAX};





   int num_flows = 0;



   string line;
   while(testcase.peek() != EOF && num_flows < max_flows_read){
      getline(testcase, line);
      stringstream ss(line);

      Flow flow;
      int read_id;

      ss >> flow.flow_id;
      if(ss.peek() == ',') ss.ignore();
      ss >> read_id;
      flow.source_id.id = ttable[read_id];
      if(ss.peek() == ',') ss.ignore();
      ss >> read_id;
      flow.dest_id.id = ttable[read_id];
      if(ss.peek() == ',') ss.ignore();


      int flow_length; ss >> flow_length;
      if (flow_length < min_flow_size) continue;
      if (flow_length > max_flow_size) continue;
      flow_length = (int)(flow_length * flow_size_multiplier);
      flow.num_frames = (flow_length + PAYLOAD_LENGTH - 1) / PAYLOAD_LENGTH;
      flow.remain_frames = flow.num_frames;
      flow.quantized_num_frames = *std::prev(std::upper_bound(quantization_vector.begin(), quantization_vector.end(), flow.num_frames));
      if(ss.peek() == ',') ss.ignore();

      double start_time; ss >> start_time;
      flow.start_tick = (start_time/load_factor) / SLOT_LENGTH_INCL_GB;

      if(flow.start_tick < 0) {
         break;
      }

      nodes[flow.source_id]->add_send_flow(flow);
      nodes[flow.dest_id]->add_recv_flow(flow);

      num_flows++;
   }

   int first_received_tick[EPOCH_LENGTH];
   int first_received_feedback_tick[EPOCH_LENGTH];
   for (int i = 0; i < EPOCH_LENGTH; i++) {
      first_received_tick[i] = -1;
      first_received_feedback_tick[i]=-1;
   }

   std::ofstream recvd_frames_file;
   if(logging) {
      if(std::filesystem::exists(output_dir / "recvd_frames.csv")) {
         const std::time_t now = std::time(nullptr);
         std::filesystem::rename(output_dir / "recvd_frames.csv",
                                 output_dir / ("recvd_frames.csv-"+boost::lexical_cast<std::string>(now)));
      }
      recvd_frames_file.open(output_dir / "recvd_frames.csv");
      recvd_frames_file << 0 << "," << 0 << std::endl;
   }

   //main loop
   int send_tick;
   for(send_tick = 0; (completed_flows < num_flows) && (completed_flows < max_flows) && (send_tick < max_ticks + PROP_DELAY_TS); send_tick++){
      int receive_tick = send_tick - PROP_DELAY_TS;
      if(receive_tick >= total_frames_recvd_M.size() * 1000000 * TSFRAC) {
         total_frames_recvd_M.push_back(total_frames_recvd);
         if(logging) {
            recvd_frames_file << receive_tick << "," << total_frames_recvd << std::endl;

            if(USE_HBH){
               std::ofstream active_buckets_file;
               active_buckets_file.open(output_dir / ("active-buckets-"+std::to_string(receive_tick)+".csv"));
               for (auto node : nodes) {
                  node->record_cur_buckets_in_use(active_buckets_file);
               }
            }
            std::ofstream buffer_occupancy_file;
            buffer_occupancy_file.open(output_dir / ("buffer-occupancy-"+std::to_string(receive_tick)+".csv"));
            for (auto node : nodes) {
               node->record_cur_buffer_occupancy(buffer_occupancy_file);
            }
         }
      }
      if (receive_tick >= 0 && receive_tick % 100 == 0) {
         logged_cout << "starting tick " << receive_tick << "    completed flows: " << completed_flows << endl;
      }
      if (USE_FSR) {
         std::for_each(std::execution::par, std::begin(nodes), std::end(nodes), [=](auto&& node) {
            node->adjust_flow_credit(send_tick);
         });
      }
      std::for_each(std::execution::par, std::begin(nodes), std::end(nodes), [=](auto&& node) {
         node->send_packet(send_tick);
      });
      if (USE_RD) {
         std::for_each(std::execution::par, std::begin(nodes), std::end(nodes), [=](auto&& node) {
            node->send_rdc(send_tick);
         });
      }
      if (USE_HBH && first_received_tick[send_tick % EPOCH_LENGTH] >= 0) {
         std::for_each(std::execution::par, std::begin(nodes), std::end(nodes), [=](auto&& node) {
            node->send_tokens(send_tick);
         });
         if (send_tick - first_received_tick[send_tick % EPOCH_LENGTH] <= EPOCH_LENGTH) {
            first_received_feedback_tick[send_tick % EPOCH_LENGTH] = send_tick;
         }
      }
      if(receive_tick >= 0) {
         if (receive_tick < EPOCH_LENGTH) {
            int cur_phase = (receive_tick / LINKS_PER_PHASE) % NUM_PHASES;
            int cur_link = receive_tick % LINKS_PER_PHASE;
            int recv_link = LINKS_PER_PHASE - 1 - cur_link;
            int recv_index = recv_link + cur_phase * LINKS_PER_PHASE;
            first_received_tick[recv_index] = receive_tick + PROP_DELAY_TS;
         }
         std::for_each(std::execution::par, std::begin(nodes), std::end(nodes), [=](auto&& node) {
            node->receive_packet(receive_tick);
         });
         if (USE_RD) {
            std::for_each(std::execution::par, std::begin(nodes), std::end(nodes), [=](auto&& node) {
               node->receive_rdc(receive_tick);
            });
         }
         if (USE_HBH && first_received_feedback_tick[receive_tick % EPOCH_LENGTH] >= 0
             && receive_tick >= first_received_feedback_tick[receive_tick % EPOCH_LENGTH]) {
            std::for_each(std::execution::par, std::begin(nodes), std::end(nodes), [=](auto&& node) {
               node->receive_tokens(receive_tick);
            });
         }
      }
   }
   int last_completed_tick = send_tick - PROP_DELAY_TS;

   if (logging) {
      recvd_frames_file << last_completed_tick << "," << total_frames_recvd << std::endl;
   }

   total_frames_recvd_M.push_back(total_frames_recvd);

   logged_cout << endl;
   logged_cout << "Simulation complete. Total timeslots: " << last_completed_tick << endl;
   logged_cout << endl;



   std::vector<int> max_buckets;
   for (auto node : nodes) {
      node->add_max_buckets_in_use(max_buckets);
   }
   int max_bucket = *std::max_element(max_buckets.begin(), max_buckets.end());
   if(USE_HBH){
      logged_cout << "Max buckets in use: " << max_bucket << endl;
   }

   std::vector<int> max_queue_lengths;
   for (auto node : nodes) {
      node->add_max_queue_lengths(max_queue_lengths);
   }
   int max_queue_length = *std::max_element(max_queue_lengths.begin(), max_queue_lengths.end());
   logged_cout << "Max queue length: " << max_queue_length << endl;

   std::vector<int> max_buffer_occupancies;
   for (auto node : nodes) {
      node->add_max_buffer_occupancy(max_buffer_occupancies);
   }
   int max_buffer_occupancy = *std::max_element(max_buffer_occupancies.begin(), max_buffer_occupancies.end());
   logged_cout << "Max buffer occupancy: " << max_buffer_occupancy << endl;


   if(USE_HBH && logging) {
      std::ofstream active_buckets_file;
      active_buckets_file.open(output_dir / "max-active-buckets.csv");
      for (auto node : nodes) {
         node->record_max_buckets_in_use(active_buckets_file);
      }
   }
   if(USE_HBH && logging) {
      std::ofstream active_buckets_file;
      active_buckets_file.open(output_dir / "active-buckets-final.csv");
      for (auto node : nodes) {
         node->record_cur_buckets_in_use(active_buckets_file);
      }
   }
   if(logging) {
      std::ofstream queue_lengths_file;
      queue_lengths_file.open(output_dir / "max-queue-lengths.csv");
      for (auto node : nodes) {
         node->record_max_enqueued_frames(queue_lengths_file);
      }
   }
   if(logging) {
      std::ofstream queue_lengths_file;
      queue_lengths_file.open(output_dir / "queue-lengths-final.csv");
      for (auto node : nodes) {
         node->record_cur_enqueued_frames(queue_lengths_file);
      }
   }
   if(logging) {
      std::ofstream buffer_occupancy_file;
      buffer_occupancy_file.open(output_dir / "max-buffer-occupancy.csv");
      for (auto node : nodes) {
         node->record_max_buffer_occupancy(buffer_occupancy_file);
      }
   }
   if(logging) {
      std::ofstream buffer_occupancy_file;
      buffer_occupancy_file.open(output_dir / "buffer-occupancy-final.csv");
      for (auto node : nodes) {
         node->record_cur_buffer_occupancy(buffer_occupancy_file);
      }
   }
   if(logging) {
      std::ofstream incomplete_flows_file;
      incomplete_flows_file.open(output_dir / "incomplete-flows.csv");
      for (auto node : nodes) {
         node->record_incomplete_flows(incomplete_flows_file, last_completed_tick);
      }
   }

   auto exec_finish_time = std::chrono::system_clock::now();

   std::chrono::duration<double> elapsed_seconds = exec_finish_time - exec_start_time;
   logged_cout << "elapsed time: " << elapsed_seconds.count() << " s" << endl;

   struct rusage usage;
   getrusage(RUSAGE_SELF, &usage);
   logged_cout << "max ram used: " << usage.ru_maxrss << " kB" << endl;

   if(logging) {
      std::ofstream stats_file;
      stats_file.open(output_dir / "stats");
      if(USE_HBH){
         stats_file << "max_buckets_in_use " << max_bucket << endl;
      }
      stats_file << "max_queue_length_frames " << max_queue_length << endl;
      stats_file << "max_queue_length_bytes " << max_queue_length * PAYLOAD_LENGTH << endl;
      stats_file << "max_buffer_occupancy_frames " << max_buffer_occupancy << endl;
      stats_file << "max_buffer_occupancy_bytes " << max_buffer_occupancy * PAYLOAD_LENGTH<< endl;
      stats_file << "total_frames_recvd " << total_frames_recvd << endl;
      stats_file << "total_system_throughput " << (double)total_frames_recvd / (double)MAX_NODE_ID / (double)(last_completed_tick/TSFRAC) << endl;
      for(int m = 1; m < total_frames_recvd_M.size(); m++) {
      stats_file << "total_frames_recvd_by_t=" << m << "M " << total_frames_recvd_M[m] << endl;
      stats_file << "total_frames_recvd_after_t=" << m << "M " << total_frames_recvd - total_frames_recvd_M[m] << endl;
      stats_file << "total_system_throughput_after_t=" << m << "M " << (double)(total_frames_recvd - total_frames_recvd_M[m]) / (double)MAX_NODE_ID / (((double)last_completed_tick/TSFRAC) - 1000000*m) << endl;
      }
      for(int m = 1; m <= total_frames_recvd_M.size(); m++) {
      stats_file << "total_system_throughput_from_t=" << m-1 << "M_to_t=" << m << "M " << (double)(total_frames_recvd_M[m] - total_frames_recvd_M[m-1]) / (double)MAX_NODE_ID / 1000000.0 << endl;
      }
      stats_file << "elapsed_time_sec " << elapsed_seconds.count() << endl;
      stats_file << "max_rss_kbyte " << usage.ru_maxrss << endl;
   }


   return 0;
   for(int i = 0; i < MAX_NODE_ID; i++){
      delete nodes[i];
   }
   
   return 0;
}

//fails N nodes, ensuring that they are evenly distributed.
void fail_n_nodes (int num_to_fail, std::vector<Node *> nodes) {
   const int base_sum = (NODES_PER_PHASE-1) * NUM_PHASES / 2;
   int *coords = new int[NUM_PHASES];

   int num_failed = 0;
   int iterations = 0;
   while (num_failed < num_to_fail) {
      int sum = base_sum;
      if(iterations % 2) {
         sum += (iterations + 1) / 2;
      } else {
         sum -= iterations / 2;
      }

      std::vector<int> idxs_to_fail;

      coordinate_loop(idxs_to_fail, coords, 0, sum, 0);

      if (idxs_to_fail.size() > num_to_fail - num_failed) {
         int num_to_fail_now = num_to_fail - num_failed;
         std::mt19937 random_generator(1);
         std::shuffle(idxs_to_fail.begin(), idxs_to_fail.end(), random_generator);
         for (int i = 0; i < num_to_fail_now; i++) {
            nodes[idxs_to_fail[i]]->fail_node();
         }
         num_failed += num_to_fail_now;
      } else {
         for (int i = 0; i < idxs_to_fail.size(); i++) {
            nodes[idxs_to_fail[i]]->fail_node();
         }
         num_failed += idxs_to_fail.size();
      }
      iterations++;

   }

   delete [] coords;
}

void coordinate_loop (std::vector<int> &idxs_to_fail, int *coords, const int cur_coord, const int sum, const int sum_so_far) {
   if (cur_coord == NUM_PHASES - 1) {
      int value = sum - sum_so_far;
      if(value < 0) return;
      if(value >= NODES_PER_PHASE) return;
      coords[cur_coord] = value;
      NodeID id = node_id_from_array(coords);
      idxs_to_fail.push_back(id.id);
   }
   else {
      for(int i = 0; i < NODES_PER_PHASE; i++) {
         coords[cur_coord] = i;
         coordinate_loop(idxs_to_fail, coords, cur_coord+1, sum, sum_so_far+i);
      }
   }
}
