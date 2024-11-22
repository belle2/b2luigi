# `b2luigi` Starter Kit for the B2GM 2024
This repository contains in total 13 different exercises that demonstrate the core principle of [`b2luigi`](https://gitlab.desy.de/belle2/software/b2luigi). The first 4 exercises explain the basic functionalities of [`luigi`](https://luigi.readthedocs.io) and show some of the additions made in `b2luigi`. Exercises 5 to 13 describe a very simplified workflow for a common analysis. In these exercises, the target is to simulate and reconstruct events and work with quantities derived from the reconstruction. Furthermore, exercises 9, 10 and 11 demonstrate the submission of tasks to the supported batch systems by running practically the same ntuple extraction. Exercises 4 to 11 are run with the help of [`basf2`](https://gitlab.desy.de/belle2/software/basf2), while exercises 12 and 13 showcase simple Python code based on the produced ntuples.

This starter kit assumes profound knowledge in Bash, Python and `basf2`.

## Environment Setup
The starter kit can be run in any environment in which `basf2` and `b2luigi` are present and accessible. However, for better control of the environment and reproducibility, it is recommended to run any analysis or tool in a designated virtual Python environment. The setup of this environment works as follows (assuming a Belle II cvmfs setup):

Clone this repository with either ssh (an ssh key is required to be set up on the DESY GitLab):
```bash
git clone git@gitlab.desy.de:alexander.heidelbach/starterkit_b2luigi.git
```

or via https:
```bash
git clone https://gitlab.desy.de/alexander.heidelbach/starterkit_b2luigi.git
```

Change to the directory:
```bash
cd starterkit_b2luigi
```

Create the virtual environment with a dedicated release. For this workshop, we make use of `prerelease-09-00-00c`.
```bash
source /cvmfs/belle.cern.ch/tools/b2setup
b2venv prerelease-09-00-00c
```

This command creates a `venv` directory in the repository with the name "venv". The next step is to activate the environment:
```bash
source venv/bin/activate
```

This environment will be based on the `basf2` environment. However, Python packages that are not provided by the externals will be installed in the virtual environment. To install the requirements for this starter kit run:
```bash
pip3 install -r requirements.txt
```

## Optional Requirements
To run on the LSF batch system, access to kekccc is required. A HTConder batch system is installed on the NAF servers. To submit jobs via `gbasf2` a valid grid certificate is required.
