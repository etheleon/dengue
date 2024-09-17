# Introduction

This is the reprository for running the analysis for the dengue forecasting.

---


<!-- mtoc-start -->

* [Folder Structure](#folder-structure)
* [Initialisation](#initialisation)

<!-- mtoc-end -->

# Folder Structure

* `data`: Where data files are stored
* `scripts`: various scripts
* `tables`: Where schema,table DDLs are kept

```
.
├── data
│   └── release_site
│       ├── hdb.xlsx
│       ├── landed.xlsx
│       └── rct.xlsx
├── README.md
├── scripts
│   ├── inla_model
│   │   └── predict.R
│   └── insert_data.ipynb
└── tables
    └── create_release_sites.sql
```

# Initialisation

The repo contains a submodule from [EmilieFinch/dengue-singapore](https://github.com/EmilieFinch/dengue-singapore). 
After initial git clone also initialise the submodule

1. Initialise submodule

  ```bash
  git submodule update --init
  ```
