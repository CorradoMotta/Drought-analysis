# historical_data.py
#===================
# Author:         Corrado Motta
# Date:           02/2023

#-----------------------------------------------------
# Simple script to read the historical drought data
# from a NetCDF file and perform zonal statistics.
# It makes use of paralell processing for the execution
# of the main function. No shared data are invovled 
# and therefore no locks are needed.
#-----------------------------------------------------

# LIST OF IMPORTS

# to work with multidimensional arrays and to import NetCDF files
import xarray
# extension of xarray to convert ndarrays to raster
import rioxarray
# python module to computer statistics
from rasterstats import zonal_stats
# to wrap function
from functools import wraps
# to calculate execution time
import time
# to create the final table
import pandas as pd
# for the affine transformation
from affine import Affine
# for os miscellaneous
import os
# for numpy arrays
import numpy as np
# to create tiff image
import rasterio
# to suppress warnings on large loops
import warnings
warnings.filterwarnings("ignore")
# to read conf file
from conf import conf_ini

# main
if __name__ =="__main__":
    
    # read configuration file
    params = conf_ini(""../conf/conf.ini"")
    
    # open dataset
    ds_disk = xarray.open_dataset(params.historical_data['nc_path'])
    
    # create a dataArray for the frequency data
    frq_da = xarray.DataArray(
    ds_disk.frq.data, # first the data frq is a multi-dimensional array --> ('year', 'y', 'x')
    coords={
        "year": ds_disk.year.data, # year coords
        "y": ds_disk.y.data, # lat coords
        "x": ds_disk.x.data, # lon coords
    },
    dims=["year", "y","x"], # finally the dims
    )
    
    # add crs information
    frq_da.rio.write_crs(params.historical_data['crs'], inplace=True)
    
    # calculate affine transformation
    frq_aff = frq_da.rio.transform()
    
    # ready to perform zonal statistics
    start_time = time.perf_counter()
    
    # list to append all data
    frq_zonal = []
    print("performing zonal statistics for NUTS-{0}..".format(params.historical_data['nuts_level']))
    for i, year in enumerate(frq_da.year):
        print("{0}) year {1}".format(i,int(year.data)))

        tmp_zonal = zonal_stats(params.historical_data['shp_path'], # NUTS shapefile
                             frq_da.isel(year=i).to_numpy(), # our ndarray
                             stats="median", # we are interested only in median values
                             affine=frq_aff, # affine transformation
                             geojson_out=True # to add info from shp fields
                            )
        
        # add medians value
        year = int(year.data)
        
        for i, obj in enumerate(tmp_zonal):
            nuts_id_value = obj['properties']["NUTS_ID"]
            median_value = -999
            if(obj['properties']["median"] is not None):
                median_value = float(obj['properties']["median"])  
            frq_zonal.extend([(year, nuts_id_value, median_value)])
        
    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f'Total execution time for zonal stats {total_time:.4f} seconds')
    
    # convert to pandas   
    df = pd.DataFrame(frq_zonal, columns=['year', 'NUTS_ID', 'med'])
    
    # save as parquet
    print("Saving to parquet")
    df.to_parquet(os.path.join(params.generic["result_path"],'historical_zonal_stats_NUTS{0}.parquet'.format(params.historical_data['nuts_level'])), 
                               engine='fastparquet')
    
    print("Done!")