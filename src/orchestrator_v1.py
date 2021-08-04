# -*- coding: utf-8 -*-
"""
Created on Mon Aug  2 13:45:40 2021

@author: gerardo
"""

import connection_v1 as co
import functions as fn
import datetime

#------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    
    t1 = datetime.datetime.now()
    print('********\n') 
    print('Inicio - ' + str(t1))
    print('********\n') 
    
    # Solicitar datos de archivo 
    folder, name_file, ext_file  = co.get_config('source', '')
    last_part_path = folder+name_file+'.'+ext_file 
    
    #Load File
    df = fn.load_file(last_part_path)
    
    #Trnasformacion birthdate_to_age
    fields_t1  = co.get_config('transforms', 'birthdate_to_age')
    
    df_t1, flg_t1 = fn.f_birthdate_to_age(df, fields_t1)
    
    #Trnasformacion hot_encoding
    fields_t2  = co.get_config('transforms', 'hot_encoding')
    
    df_t2, flg_t2 = fn.f_hot_encoding(df_t1,fields_t2)
    
    #Trnasformacion fill_empty_values
    fields_t3  = co.get_config('transforms', 'fill_empty_values')
    
    df_output, flg_t3 = fn.f_fill_empty_values(df_t2,fields_t3 )
    
    # Solicitar datos de archivo 
    out_folder, out_name_file, out_ext_file  = co.get_config('sink', '')
    last_part_path_output = out_folder+out_name_file+'.'+out_ext_file 
    
    #Guardar File
    
    fn.safe_file(out_folder, last_part_path_output, df_output)
    
    t2 = datetime.datetime.now()
    print('tiempo total de ejecucion : ' + str(t2-t1) + ' \n')
    print('Fin **** ')
    print(' ********\n')



    
    
    
  
    
    
    
    