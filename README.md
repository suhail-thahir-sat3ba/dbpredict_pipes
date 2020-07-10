# dbpredict_pipes
``dbpredict`` predicts individuals at risk of developing diabetes or complications that arise from having diabetes using health insurance claims data. ``dbpredict_pipes`` provides an end-user specific interface to prepare data for processing and prediction in ``dbpredict``.

This version is designed for compatibility with, and exclusive use by, Independent Health Association, Inc.

## Table of Contents
* [Installation](#Installation)
* [Usage](#Usage)
* [Authors](#Authors)
* [History](#History)

## Installation
The following sections detail how to install ``dbpredict_pipes``.

### Prerequisites
The use of ``dbpredict_pipes`` requires the following non-native libraries:
* pandas
* dask.dataframe
* sqlalchemy
* cx_Oracle

### Installing ``dbpredict_pipes``
From the directory with the compressed `dbpredict` and `dbrpedict_pipes` packages (`.tar.gz` or `.whl`) run the following script.
```bash
pip install dbpredict_pipes
pip install dbpredict
```

## Usage
``dbpredict_pipes`` is called directly from ``dbpredict``. Use of ``dbpredict_pipes`` on its own or outside 
of ``dbpredict`` is not recommended.

## Authors
This package was developed by the FTI Center for Healthcare Economics and Policy. Its principal authors were Bryan Perry and Suhail Thahir. Inquiries should be directed to [bryan.perry@fticonsulting.com](mailto:bryan.perry@fticonsulting.com).

## History
This is version 0.3.0 of ``dbpredict_pipes``.
