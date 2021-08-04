# -*- coding: utf-8 -*-
"""
Created on Mon Aug  2 13:45:40 2021

@author: gerardo
"""
from pathlib import Path
import os.path
import os 
import pandas as pd
from datetime import datetime, date
#import time

#------------------------------------------------------------------------------------------------------
## Ctes ## ---
#------------------------------------------------------------------------------------------------------


#------------------------------------------------------------------------------------------------------

def load_file(f_path ):
    
    path_ = os.path.dirname(os.path.realpath('__file__'))   
    #path_ = os.path.dirname(os.path.realpath(__file__)) # esto te lo dara en src
    path = Path(path_) # Esto lo convierte en un objeto que puede ser recorrido.
    path_padre = path.parent # Aqui tenemos la carpeta padre de src
    conf_filepath = str(path_padre) +'/'+ f_path 
    
    print('+++++++ Path Cargado +++++++ \n')
    df_work = pd.read_csv(conf_filepath)
    
    return df_work
#---------------------------------------------------------------------------------
 
def safe_file(out_folder_, f_path, df_output_):

    path_ = os.path.dirname(os.path.realpath('__file__'))   
    #path_ = os.path.dirname(os.path.realpath(__file__)) # esto te lo dara en src
    path = Path(path_) # Esto lo convierte en un objeto que puede ser recorrido.
    path_padre = path.parent # Aqui tenemos la carpeta padre de src
    dir_ =str(path_padre) +'/'+ out_folder_
    conf_filepath = str(path_padre) +'/'+ f_path 
    
    if not os.path.exists(dir_):
        os.mkdir(dir_)
    try:
        df_output_.to_parquet(conf_filepath)
        print('+++++++ Guardado el file en:  ' +  conf_filepath)
        df_output_.to_csv(dir_+'/bookings_cleaned_ctrl.csv')
        
    except:
        print('+++++++ NO se guardo el file en:  ' +  conf_filepath)
#---------------------------------------------------------------------------------
    

def f_birthdate_to_age(df_birthdate, fields):
    
    try:
        # Transforma la columna transformado a datetime 
        df_birthdate[fields[0]['field']] = pd.to_datetime(df_birthdate[fields[0]['field']])  
        # aplica una funcion edad el campo
        df_birthdate[fields[0]['new_field']] = df_birthdate[fields[0]['field']].apply(cal_age)
        print('Transformacion realizada \n', )
        print(df_birthdate[fields[0]['new_field']].head())
        return df_birthdate, 1
    except ValueError as err:
        print('el campo especificado no es una fecha \n', err  )
        return err, 0

#---------------------------------------------------------------------------------

def cal_age(nac):

    td = date.today()
    cumpleaños = nac.replace(year=td.year)

    if cumpleaños > td:

        return td.year - nac.year - 1
    else:
        

        return td.year - nac.year   
    


#---------------------------------------------------------------------------------

def f_hot_encoding(df_hot, fields_hot):

    if df_hot[fields_hot[0]].dtypes == type(object()):
        df_hot= pd.get_dummies(df_hot, columns=[fields_hot[0]], prefix=["is"])
        print('Transformacion Hot Encoding realizada \n', )
        return df_hot, 1
    else:
        print('el campo especificado no es un objeto \n' )
        return '', 0


#---------------------------------------------------------------------------------
        
def f_fill_empty_values(df_empty, fields_empty):

    for x in fields_empty:
        if x['value'] in ['mean', 'median','mode']:
            if x['value'] == 'median':
                df_empty[x['field']].fillna(df_empty[x['field']].median(), inplace = True)
            elif x['value'] == 'mean':
                df_empty[x['field']].fillna(df_empty[x['field']].mean(), inplace = True)
            else: 
                df_empty[x['field']].fillna(df_empty[x['field']].mode(), inplace = True)
            print('Transformacion Empty realizada \n', )    
        else:
            df_empty[x['field']].fillna(x['value'], inplace = True)
        
    return df_empty, 1
            
#---------------------------------------------------------------------------------

