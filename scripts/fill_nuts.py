# fill_nuts.py
#=============
# Author:         Corrado Motta
# Date:           02/2023

#-----------------------------------------------------
# In this script fills empty NUTS_3 regions by going 
# backwards up to NUTS_0 units, until it finds a median 
# value to use for such area. 
# In case it does not find any valid value, it uses a 
# nan number (set to -999).
# Inputs shall be file containing the median values
# at all NUTS units available.
#-----------------------------------------------------

# LIST OF IMPORTS

# for tabular operation
import pandas as pd
# for os operations
import os
# for 
import re
# to read conf file
from conf import conf_ini

# main
if __name__ =="__main__":
    
    my_conf = conf_ini("../conf/conf.ini")
    
    # import parquet file for NUTS_3
    prj_path = os.path.join(my_conf.generic["db_path"],"zonal_stats_NUTS3_tot.parquet")
    prj_df_3 = pd.read_parquet(prj_path, engine='fastparquet')
    
    # import all other parquet files and store in a dict
    prj_path = os.path.join(my_conf.generic["db_path"],"zonal_stats_NUTS0_tot.parquet")
    prj_df_1 = pd.read_parquet(prj_path, engine='fastparquet').set_index(['model', 'rcp', 'year', 'NUTS_ID'])
    prj_path = os.path.join(my_conf.generic["db_path"],"zonal_stats_NUTS1_tot.parquet")
    prj_df_2 = pd.read_parquet(prj_path, engine='fastparquet').set_index(['model', 'rcp', 'year', 'NUTS_ID'])
    prj_path = os.path.join(my_conf.generic["db_path"],"zonal_stats_NUTS2_tot.parquet")
    prj_df_0 = pd.read_parquet(prj_path, engine='fastparquet').set_index(['model', 'rcp', 'year', 'NUTS_ID'])
    
    # add NUTS_CODE 0,1,2 fields to the NUTS_3 parquet table
    # Create new columns in NUTS-3 for all of them
    prj_df_3["NUTS_ID_2"] = prj_df_3["NUTS_ID"].str[:-1]
    prj_df_3["NUTS_ID_1"] = prj_df_3["NUTS_ID"].str[:-2]
    prj_df_3["NUTS_ID_0"] = prj_df_3["NUTS_ID"].str[:-3]
    
    # split in two dfs depending on the med value. The missing data is set as -999.
    mask = prj_df_3.med == -999
    prj_df_3_OK = prj_df_3[~mask]
    prj_df_3_NOT_OK = prj_df_3[mask]
    
    # add all dfs to a dict to simplify operations later on
    db_dict = {
      0: prj_df_0,
      1: prj_df_1,
      2: prj_df_2,
      3: prj_df_3
    }
    
    # iterrate over ALL rows
    for i, x in prj_df_3_NOT_OK.iterrows():
        nuts_level = 3
        while(nuts_level):
            to_subs = 4 - nuts_level
            nuts_level -= 1 
            new_med = db_dict[nuts_level].loc[(x.model,x.rcp,x.year, x.NUTS_ID[:-to_subs])].med
            if(new_med!=-999): break
        # bit slower than making a new column
        prj_df_3_NOT_OK.loc[i, 'med'] = new_med
        
    # concatenate back
    final_df = pd.concat([prj_df_3_OK,prj_df_3_NOT_OK]).reset_index(drop=True)
    # drop fields added for the analysis
    final_df.drop(['NUTS_ID_2', 'NUTS_ID_1','NUTS_ID_0'], axis=1,inplace=True)
    # save back to parquet
    final_df.to_parquet(os.path.join(my_conf.generic["result_path"],'zonal_stats_NUTS_3_filled.parquet', engine='fastparquet')