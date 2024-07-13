This is a packet-level simulator for Shale, a datacenter-scale reconfigurable network.
A detailed explanation of the algorithms can be found in the SIGCOMM paper.

Required libraries:
 - onetbb
 - boost

Build instructions: Run `make` in the root directory of the repository.

## Reproducing figures in the Shale paper

This release includes all of the files needed to replicate figures 9-13 in the Shale paper.
Note that replicating these figures requires significant computational resources.
We originally performed our experiments on a cluster managed by the Slurm workload manager.
To allow our simulator to be tested more easily on a single PC, we provide instructions for a smaller-scale simulator test at the bottom of this readme.

Each figure corresponds to one test case as follows:
 - Figure 9 = `datamining-10000-inter`
 - Figure 10 = `aditya-10000`
 - Figure 11 = `datamining-10000`
 - Figure 12 = `failed-throughput-10000`
 - Figure 13 = `aditya+`

### Preparing network traces

Before running the aditya+ workload, it is necessary to prepare additional network traces for each system size. This can be done as follows:

```
cd workload/traces
./resize-aditya.sh
```

### Preparing simulation jobs

We originally performed our experiments on a cluster managed by the Slurm workload manager.
Here, we provide Python scripts that generate one Slurm job file for each simulation used in our experiments.
They can be run as follows:

```
cd workloads/slurm-files
./aditya-10000.py
./aditya+.py
./datamining-10000-inter.py
./datamining-10000.py
./failed-throughput-10000.py
```

Each script generates Slurm files in the corresponding directory.

### Running simulations

When logged into a Slurm cluster, the jobs for a given test case can be started as follows:

```
cd workloads/slurm-files/datamining-10000
for file in *; do sbatch --requeue $file; done
```

To run these jobs in another environment, note that the job files can also be run as shell scripts.
Each job file contains one uncommented line which contains the command line for the specified simulation run.

### Generating graphs

Once all of the jobs for a given test case have completed, graphs can be generated as follows:

```
cd results
./graph-datamining-10000.py
```

For `failed-throughput-10000`, the graphing script will open a window with the resulting graph. For all other test cases, the graphing script generates a PDF file with many graphs, some of which were selected for inclusion in the paper.




## Smaller-scale simulator test

We provide a shell script which can be used to run a smaller-scale version of the Figure 12 experiment, `failed-throughput-10000`.
The simulations run by this shell script are identical to those used to generate Figure 12, except that they are run for only 1% of the amount of time
Running this script takes under an hour on a laptop with an Intel i7-13700H.
This script can be started as follows:

```
cd workloads
./shorter-failed-throughput.sh
```

Once the script finishes, a graph can be generated as follows. Note that the observed throughput will be lower than in Figure 12; this is because these shorter simulations are affected more strongly by startup effects that lower throughput.

```
cd results
./graph-datamining-10000.py
```

