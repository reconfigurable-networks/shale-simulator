This is a packet-level simulator for Shale, a datacenter-scale reconfigurable network.
A detailed explanation of the algorithms can be found in the SIGCOMM paper.

Required libraries:
 - onetbb
 - boost

Build instructions: Just run `make` in the root directory of the repository.

### Reproducing figures in the Shale paper

This release includes all of the files needed to replicate figures 9-13 in the Shale paper.
Each figure corresponds to one test case as follows:
 - Figure 9 = datamining-10000-inter
 - Figure 10 = aditya-10000
 - Figure 11 = datamining-10000
 - Figure 12 = failed-throughput-10000
 - Figure 13 = aditya+

#### Preparing network traces


### Smaller-scale simulator test

