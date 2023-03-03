### Drought-analysis

_Corrado Motta_

Set of notebooks and scripts to perform drought analysis at different NUTS resolution.

The `conf` folder contains the configuration file. It is a single file (.ini) used by all scripts and notebooks.
The `scripts` folder contains the scripts used. Each script can be visualized and examplified by using the available jupyter notebooks.

__Note:__ The drought impact data tables are not uploaded. Make sure to add them to the `csv` folder before running the analysis.

The list of current scripts and associated notebooks is the following:

|Name of the script	| Description | Notebook |
| :-------------| :----------------------------------------------------------- | :----------------------------------------- |
|`conf.py`		| Module and class to read and parse the configuration file. | - | 
|`fill_nuts.py`		| Script to fill NUTS_3 regions without a frequency median value by using the coarser NUTS resolutions | `Fill NUTS_3.ipynb` | 
|`historical_data.py`		| Script to calculate zonal statistics (median) for historical drought data in __NetCDF__ format | `Zonal statistics.ipynb` | 
|`projections_data.py`    | Script to calculate zonal statistics (median) for projection drought data in __npy__ format using multi-processing | `Zonal statistics.ipynb` | 
| todo		| Notebook to perform the main drought analysis on a country level (both curve fitting and projections) | `Drought analysis_NUTS0.ipynb`| 
| todo		| Notebook to perform the main drought analysis on a NUTS_3 level (both curve fitting and projections) |`Drought analysis_NUTS3.ipynb`|
