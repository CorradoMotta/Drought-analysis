# multi_processing.py
#====================
# Author:         Corrado Motta
# Date:           02/2023

#-----------------------------------------------------
# Script that makes use of the multiprocessing
# pool class to run the zonal statistics (median) method in 
# parallel for different input models, rcp and years.
#-----------------------------------------------------

# LIST OF IMPORTS

# python module to computer statistics
from rasterstats import zonal_stats
# to suppress warnings on large loops
import warnings
warnings.filterwarnings("ignore")
# to calculate execution time
import time
# to create the final table
import pandas as pd
# to work with ndarrays
import numpy as np
# to work with os miscellaneous
import os
# to pass the Affine transformation
from affine import Affine
# to work with multi processing in python
from multiprocessing import Process
from multiprocessing.pool import Pool
# to read conf file
from conf import conf_ini
# itertools utilities
from itertools import repeat

# method to perform zonal statistics
def zonal_stats_proc(comp_range, params):
   
    """ This method performs zonal statistics.

    Args:
        comp_range (tuple) : Tuple containing two integers, one for the starting and one
        for the ending indexes.
        params (conf_ini): Object of class conf_ini containing all the configuration params.
    """
    
    # create the affine transformation.
    frq_aff = Affine(*params.projections_data['affine'])
    
    frq_zonal = []
    print("Starting zonal statistics with range:", comp_range)

    # for each file npy in the range
    for element in os.listdir(params.projections_data['npy_path'])[comp_range[0]:comp_range[1]]:
        
        # extract filename and path
        npy = os.path.join(params.projections_data['npy_path'],element)

        # check if it is actually a file
        if(os.path.isfile(npy)):

            # create correspondent numpy array
            myArray = np.load(npy)

            # flip left to right
            myArray=np.fliplr(myArray)

            # rotate 90 degrees
            myArray=np.rot90(myArray)

            # calculate stats
            tmp_zonal = zonal_stats(params.projections_data['shp_path'], # NUTS shapefile
                                 myArray, # our ndarray
                                 stats="median", # we are interested only in median values
                                 affine=frq_aff, # affine transformation
                                 geojson_out=True # to add info from shp fields
                                )

            # add all medians to the list
            model = element[22:-17]
            rcp = int(element[-13:-11])
            year = int(element[-10:-6])
            frq_zonal.extend([(model, rcp, year, obj['properties']["NUTS_ID"], float(obj['properties']["median"])) for i,obj 
                              in enumerate(tmp_zonal) if obj['properties']["median"] is not None])
            
    # return the result    
    return frq_zonal
 
# main
if __name__ =="__main__":
    
    # read configuration file
    params = conf_ini("../conf/conf.ini")
    
    # calculate sequences to split the method calls 
    sequences = []
    ticks = 6
    unit = len(os.listdir(params.projections_data['npy_path']))/ticks
    tot = int(len(os.listdir(params.npy_path))/unit)
    
    for nr in range(ticks):
        if(nr == tot -1):
            sequences.append((int(nr*unit),int((len(os.listdir(params.projections_data['npy_path']))))))
        else:
            sequences.append((int(nr*unit),int(((nr+1)*unit)-1)))
       
    print("The identified sequences are",sequences)
    print("Analysis performed at NUTS-{0} level".format(params.projections_data['nuts_level']))
    
    # Create processes pool and register execution time
    results = []
    start_time = time.perf_counter()
    
    with Pool(processes = ticks) as pool:
        # execute tasks in order
        for result in pool.starmap(zonal_stats_proc, zip(sequences, repeat(params))):
            results.extend(result)
    
    # convert to pandas
    df = pd.DataFrame(results, columns=['model','rcp','year', 'NUTS_ID', 'med'])
    
    # save as parquet
    print("Saving to parquet")
    df.to_parquet(os.path.join(params.generic['result_path'],'zonal_stats_NUTS{0}.parquet'.format(params.projections_data['nuts_level'])), 
                  engine='fastparquet')
    
    # calculate total time
    end_time = time.perf_counter()
    total_time = end_time - start_time
    
    print(f'Execution time {total_time:.4f} seconds')
    print("Done!")