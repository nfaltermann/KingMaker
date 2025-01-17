# KingMaker



KingMaker is the workflow management for producing ntuples with the [CROWN](github.com/KIT-CMS/CROWN) framework. The workflow management is based on [law](github.com/riga/law), which is using [luigi](https://github.com/spotify/luigi) as backend.

---
## Setup

Setting up KingMaker should be straight forward:

```sh
git clone --recursive git@github.com:KIT-CMS/KingMaker.git
cd KingMaker
source setup.sh <Analysis Name>
```

this should setup the environment specified in the luigi.cfg file (located at `lawluigi_configs/<Analysis Name>_luigi.cfg`), which includes all needed packages.
The environment is sourced from the conda instance located at `/cvmfs/etp.kit.edu/LAW_envs/conda_envs/miniconda/` if possible. 
If the relevant environment is not available this way, the environment will be set up in a local conda instance.
The environment files are located at `conda_environments/<Analysis Name>_env.cfg`.
In addition other files are installed depending on the analysis.

A list of available analyses can be found in the `setup.sh` script or by running 
```sh
source setup.sh -l
```

In addition a `luigid` scheduler is also started if there isn't one running already. 

When setting up an already cloned version, a
```sh
source setup.sh <Analysis Name>
```
is enough.

---
# The KingMaker analysis
---
## Workflow

Currently, the workflow of the KingMaker analysis consists of four distinct tasks:

1. [ProduceSamples](processor/tasks/ProduceSamples.py)
    The main task, which is used to steer the Production of multiple samples at once
2. [CROWNRun](processor/tasks/CROWNRun.py)
    The task used to run CROWN with a specific file
3. [CROWNBuild](processor/tasks/CROWNBuild.py)
    This task is used to compile CROWN from source, and create a tarball, which is used by CROWNRun
4. [ConfigureDatasets](processor/tasks/ConfigureDatasets.py)
    This task is used to create NanoAOD filelists (if not existent) and readout the needed configuration parameters for each sample. This determines the CROWN tarball that is used for that job

---
## Run KingMaker

Normally, `KingMaker` can be run by running the `ProduceSamples` task. This is done using e.g.

```bash

law run ProduceSamples --local-scheduler False --analysis config --sample-list samples.txt --workers 1 --production-tag TestingCROWN

```
The required paramters for the task are:

1. `--local-scheduler False` - With this setting, the luigid scheduler is used  
2. `--analysis config` - The CROWN config to be used
3. `--sample-list samples.txt` - path to a txt file, which contains a list of nicks to be processed
4. `--production-tag TestingCROWN`

Additionally, some optional paramters are beneficial:

1. `--workers 1` - number of workers, currently, this number should not be larger than the number of tarballs to be built
2. `--print-status 2` - print the current status of the task
3. `--remove-output 2` - remove all output files
4. `--CROWNRun-workflow local` - run everything local instead of using HTCondor

---
## Tracking of Samples

The Samples are tracked and handled via nicks. For each sample, a unique nick has to be used. A collection of all samples and their settings is stored in the `datasets.yaml` file found in the `sample_database` folder. Additionally, a `nick.yaml` is generated for each individual sample, which contains all sample settings and a filelist of all `.root` files belonging to this sample.

```yaml
campaign: RunIISummer20UL18NanoAODv2
datasetname: DYJetsToLL_0J_TuneCP5_13TeV-amcatnloFXFX-pythia8
datatier: NANOAODSIM
dbs: /DYJetsToLL_0J_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18NanoAODv2-106X_upgrade2018_realistic_v15_L1v1-v1/NANOAODSIM
energy: 13
era: 2018
extension: ''
filelist:
- root://cms-xrd-global.cern.ch///store/mc/RunIISummer20UL18NanoAODv2/DYJetsToLL_0J_TuneCP5_13TeV-amcatnloFXFX-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v15_L1v1-v1/270000/D1972EE1-2627-2D4E-A809-32127A576CF2.root
- root://cms-xrd-global.cern.ch///store/mc/RunIISummer20UL18NanoAODv2/DYJetsToLL_0J_TuneCP5_13TeV-amcatnloFXFX-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v15_L1v1-v1/50000/394775F4-CEDE-C34D-B56E-6C4839D7A027.root
- root://cms-xrd-global.cern.ch///store/mc/RunIISummer20UL18NanoAODv2/DYJetsToLL_0J_TuneCP5_13TeV-amcatnloFXFX-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v15_L1v1-v1/50000/359B11BC-AE08-9B45-80A2-CC5EED138AB7.root
[....]
generators: amcatnloFXFX-pythia8
nevents: 85259315
nfiles: 85
nick: temp_nick
prepid: SMP-RunIISummer20UL18NanoAODv2-00030
sample_type: mc
status: VALID
version: 1
```

If a sample specific config is not available yet, `ConfigureDatasets` will perform a DAS query to get a filelist for this sample.

---
## Configuration

The default configuration provided in the repository should work out of the box. However some parameters might be changed. The configuration is spread across two files `lawluigi_configs/KingMaker_luigi.cfg` and `lawluigi_configs/KingMaker_law.cfg`. The HTCondor setting can also be found there.

---

# The ML_train workflow
---
## Workflow

The ML_train workflow currently contains a number of the tasks necessary for the `htt-ml` analysis. The-`NMSSM` analysis is currently not yet supported. It should be noted, that all created files are stored in remote storage and might be subject to file caching under certain circumstances.
The tasks , located in are:

1. [CreateTrainingDataShard](processor/tasks/MLTraining.py#L30)
    Remote workflow task that creates the process-datasets for the machine learning tasks from the config files you provide. The task uses the `ntuples` and `friend trees` described in the [Sample setup](https://github.com/tvoigtlaender/sm-htt-analysis/tree/master/utils/setup_samples.sh). These dependencies are currently not checked by LAW. Also uses the [create_training_datashard](https://github.com/tvoigtlaender/sm-htt-analysis/tree/master/ml_datasets/create_training_datashard.py) script. \
    The task branches each return a root file that consists of one fold of one of the processes described in the provided configs. These files can then be used for the machine learning tasks.
2. [RunTraining](processor/tasks/MLTraining.py#L141)
    Remote workflow task that performs the neural network training using GPU resources if possible. Uses the [Tensorflow_training](https://github.com/tvoigtlaender/sm-htt-analysis/tree/master/ml_trainings/Tensorflow_training.py) script. The hyperparameters of this training are set by the provided config files.\
    Each branch task returns a set of files for one fold of one training specified in the configs. Each set includes the trained `.h5` model, the preprocessing object as a `.pickle` file and a graph of the loss as a `.pdf` and `.png`. The task also returns a set of files that can be used with the [lwtnn](https://github.com/lwtnn/lwtnn) package.
3. [RunTesting](processor/tasks/MLTraining.py#L415)
    Remote workflow task that performs a number of tests on the trained neural network using GPU resources if possible. Uses the [ml_tests](https://github.com/tvoigtlaender/sm-htt-analysis/tree/master/ml_tests) scripts. The tests return a number plots and their `.json` files in a tarball, which is copied to the remote storage. The plots include confusion, efficiency, purity, 1D-taylor and taylor ranking.
5. [RunAllAnalysisTrainings](processor/tasks/MLTraining.py#L707)
    Task to run all trainings described in the configs.

## Run ML_train

Normally, the `ML_train` workflow can be run by running the `RunAllAnalysisTrainings` task:

```bash

law run RunAllAnalysisTrainings --analysis-config <Analysis configs>

```
Alternatively a single training/testing can be performed by using the `RunTraining`/`RunTesting` task directly:
```bash

law run RunTraining --training-information '[["<Training name>","<Training configs>"]]'
law run RunTesting --training-information '[["<Training name>","<Training configs>"]]'

```
Similarly it is possible to create only a single data-shard:

```bash

law run CreateTrainingDataShard --datashard-information '[["<Process name>", "<Process class>"]]' --process-config-dirs '["<Process dir>"]'

```

An example of how the above scripts could look like with the example configs: 
```bash

law run RunAllAnalysisTrainings --analysis-config ml_configs/example_configs/sm.yaml
law run RunTraining --training-information '[["sm_2018_mt","ml_configs/example_configs/trainings.yaml"]]'
law run RunTesting --training-information '[["sm_2018_mt","ml_configs/example_configs/trainings.yaml"]]'
law run CreateTrainingDataShard --datashard-information '[["2018_mt_ggh", "ggh"]]' --process-config-dirs '["ml_configs/example_configs/processes"]'

```
---
# Configurations, not set in the `ml_config` config files.
There are a number of parameters to be set in the [luigi](lawluigi_configs/ML_train_luigi.cfg) and [law](lawluigi_configs/ML_train_law.cfg) config files:

- `ENV_NAME`: The Environment used in all non-batch jobs. Can be set individually for each batch job.
- `additional_files`: What files should be transfered into a batch job in addition to the usual ones (`lawluigi_configs`, `processor` and `law`).
- `production_tag`: Can be any string. Used to differentiate the runs. Default is a unique timestamp.
- A number of htcondor specific settings that can be adjusted if necessary.

Useful command line arguments:
1. `--workers`; The number of tasks that are handled simultaneously. 
2. `--print-status -1`; Return the current status of all tasks involved in the workflow. 
3. `--remove-output -1`; Remove all output files of tasks.
