# -*- coding: utf-8 -*-
"""
Created on Mon Aug  2 13:45:40 2021

@author: gerardo
"""
## Librerias ---

import os 
from pathlib import Path
import os.path
import json 
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

## constantes ## ---
conf_file = '/conf_file/clean_bookings.json'

# -----------------------------------------------------------------------------
## Functions ## ---
# -----------------------------------------------------------------------------

def get_config(item, key):
    
    path_ = os.path.dirname(os.path.realpath('__file__'))   
    #path_ = os.path.dirname(os.path.realpath(__file__)) # esto te lo dara en src
    path = Path(path_) # Esto lo convierte en un objeto que puede ser recorrido.
    path_padre = path.parent # Aqui tenemos la carpeta padre de src
    conf_filepath = str(path_padre) + conf_file  
    with open(conf_filepath, 'r') as jfile:
        config_dict = json.load(jfile)
    
    if item == 'source':    
        
       ### Get Source values ###
        r_path = config_dict['source']['path']
        r_dataset = config_dict['source']['dataset']
        r_format_ = config_dict['source']['format']
        
        return r_path, r_dataset, r_format_
    
    elif item == 'transforms':

       ### Get transforms requirements ###
        r_transforms_list = config_dict['transforms']
        fields = []
        for x in r_transforms_list:
            if x['transform'] == key:
                fields = x['fields']
                break
            else:
                pass
        return fields 
    
    elif item == 'sink':
        
       ### Get Sink values ###        
        r_path = config_dict['sink']['path']
        r_dataset = config_dict['sink']['dataset']
        r_format_ = config_dict['sink']['format']
        
        return r_path, r_dataset, r_format_

# -----------------------------------------------------------------------------

