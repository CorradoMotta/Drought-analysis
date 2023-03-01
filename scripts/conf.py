# conf.py
#========
# Author:         Corrado Motta
# Date:           02/2023

# to parse the configuration file
import configparser
# os miscellanous
import os

# class to store params from conf file
class conf_ini:

    """ This class stores all values read by a standard configuration file.

    Args:
        file_path (str) : Path to the ini configuration file.
    """
    def __init__(self, file_path):
        
        self.file_path = file_path
        self.read_config = self.read_conf()
        
        self.generic = self.generic()
        self.historical_data = self.historical_data()
        self.projections_data = self.projections_data()
        self.drought_analysis = self.drought_analysis()
        
    def read_conf(self):
        
        """ Reads all parameters from configuration files using config parser. 
        Returns the object.
        """        
        
        read_config = configparser.ConfigParser()
        read_config.read(self.file_path)
        return read_config
    
    def generic(self):
        
        """ Store generic configuration parameters.
        """  
         
        generic_dict ={
            "shp_path": self.read_config.get("generic","shp_path"),
            "db_path": self.read_config.get("generic","db_path"),
            "result_path": self.read_config.get("generic","result_path")
        }
        
        return generic_dict
      
    def historical_data(self):
        
        """ Store configuration parameters for the historical_data script
        """  
        
        nuts_level = self.read_config.get("historical_data","nuts")
        hist_dict = {
            "nc_path": self.read_config.get("historical_data", "nc_path"),
            "nuts_level": nuts_level,
            "shp_path": self.set_shp(nuts_level),
            "crs": self.read_config.get("historical_data", "crs")}
        
        return hist_dict
    
    def projections_data(self):
        
        """ Store configuration parameters for the projections_data script
        """  
        
        nuts_level = self.read_config.get("projections_data","nuts")
        proj_dict = {
            "npy_path": self.read_config.get("projections_data", "npy_path"),
            "nuts_level": nuts_level,
            "shp_path": self.set_shp(nuts_level),
            "affine": tuple(map(float, self.read_config.get("projections_data", "affine").split(',')))}
        
        return proj_dict
    
    def fill_nuts(self):
        pass
    
    def drought_analysis(self):  
        
        """ Store configuration parameters for the drought_analysis script
        """  
        
        md = dict(self.read_config.items('drought_analysis'))
        
        input_filter={
            "eu_only": self.check_boolean(md['eu_only']),
            "loss_threshold": float(md["loss_threshold"]),
            "year_threshold": int(md["year_threshold"]),
            "events": [word.strip().lower() for word in md["events"].split(',')]}
            
        proj_dict = {
            "prj_median_threshold": float(md['prj_median_threshold']),
            "peseta_inputs": self.check_boolean(md['peseta_inputs']),
            "filter": input_filter}
        
        return proj_dict
        
    def set_shp(self, nuts_level):
        
        """ Set shapefile path according to nuts level
        
        Args:
            nuts_level (str): The nuts level of interests.
        """
        
        return os.path.join(self.generic['shp_path'], nuts_level) + ".shp"
      
      
    def check_boolean(self, bool_string):
        
        """ Checks that the input booleans in the conf file are valid.
        
        Args:
            bool_string (str): The boolean to check .
        Returns:
            bool: The correspondent python boolean.
        Raises:
            AssertionError: If boolean in the conf file is not valid.
        """

        try:
            assert bool_string.lower() == "true" or bool_string.lower() == "false", "Boolean values in the conf file are missing or mispelled. Only <True> and <False> strings are accepted. <{0}> is not valid.".format(bool_string)
        except AssertionError as err:
            raise
        else:
            if (bool_string.lower() == "true"):
                return True
            else:
                return False