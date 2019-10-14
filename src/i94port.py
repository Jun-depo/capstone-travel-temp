#!/usr/bin/env python3
# coding: utf-8

import pandas as pd
import numpy as np
import re
from helper import remove_extra_quote, extract_city, extract_state, extract_city2, extract_state2
import sys

def i94port_table(input_data, output_data):
    """
    The function is for creating i94port_table that contains city information for i94port entry_ciies.   
    It requires a lot of data cleaning.  The data size is very small. Pandas is good for cleaning this table 
    because of its flexibility. This table will merge with airport table that will create i94port2. 
    
    """
    
    # "i94port.txt" are from I94_SAS_Labels_Descriptions.SAS using text editor.
    df_i94port = pd.read_csv(input_data + "i94port.txt", sep="\t=\t", header=None, engine='python')
    
    # the data was double quotation makers. Remove one set. 
    df_i94port.loc[:,0] = df_i94port.loc[:,0].apply(remove_extra_quote)
    df_i94port.loc[:,1] = df_i94port.loc[:,1].apply(remove_extra_quote)
    # Rename the columns
    df_i94port.columns = ["i94port_code", "city_state"]
    
    # Select "city_state" column data to not contain "No PORT Code" and "Collapsed" as values
    df_i94port=df_i94port[df_i94port.loc[:,"city_state"].apply(lambda x: x[:12]!="No PORT Code" and x[:9]!="Collapsed")]
    
    # Create city columns by extracting city from "city_state" column.
    df_i94port["city"]=df_i94port.loc[:, "city_state"].apply(extract_city)
    
    # Create state columns by extracting state from "city_state" column.
    df_i94port["state"] = df_i94port.loc[:, "city_state"].apply(extract_state)
    
    # Remove "," from the previous extraction step. 
    df_i94port["city"] = df_i94port["city"].apply(lambda x: x[:-1] if x[-1]==',' else x)
    
    # The "state" column contains 2 letter state code.  Select length of the column data as 2.  
    df_i94port2a=df_i94port[df_i94port["state"].apply(lambda x: len(x)==2)]

    # clean the data len("state" column) don't equal 2.  "state" column contains 2 letter US state_codes. 
    df_i94port2b=df_i94port[df_i94port["state"].apply(lambda x: len(x)!=2)]
    
    # In these case, State data was not extracted into the state column in the previous steps. They need to be extracted by the sencond function extract_state2()
    df_i94port2b.loc[df_i94port2b["state"]=="(BPS)", "state"] = df_i94port2b.loc[df_i94port2b["state"]=="(BPS)", 
                                                                                "city"].apply(extract_state2)
    df_i94port2b.loc[df_i94port2b["state"]=="#ARPT", "state"] = df_i94port2b.loc[(df_i94port2b["state"]=="#ARPT"), 
                                                                                "city"].apply(extract_state2)
    
    # select data with len("state" column) equal to 2 after extract_state2() function extraction
    df_i94port2c = df_i94port2b[df_i94port2b["state"].apply(lambda x: len(x)==2)]
    
    # Also extract city with extract_city2 function
    df_i94port2c["city"] = df_i94port2c["city"].apply(extract_city2)

    # Further clean the data len("state") that are not 2. 'DIST. OF COLUMBIA' data is important, as it's a major port city for international entries into USA.
    df_i94port2d = df_i94port2b[df_i94port2b["state"].apply(lambda x: len(x)!=2)]
    df_i94port2d.loc[462, "city"]="DERBY LINE"
    df_i94port2d.loc[462, "state"] = "VT"
    df_i94port2d.loc[473, "state"] ="VT"
    df_i94port2d.loc[473, "city"] ="SWANTON"
    df_i94port2d.loc[428, "city"] = "PASO DEL NORTE"
    df_i94port2d.loc[428, "state"] = "TX"
    df_i94port2d.loc[478, "state"]="DC"
    df_i94port2d.loc[478, "city"]='DIST. OF COLUMBIA'

    ### select data len("state") equal to 2 after above steps.  The remaining len() not equaling to 2 data are foreign countries
    df_i94port2e = df_i94port2d[df_i94port2d["state"].apply(lambda x: len(x)==2)]

    ### combine all the useful dataframes from previous steps
    df_i94port_comb = pd.concat([df_i94port2a, df_i94port2c, df_i94port2e])

    # cleaning data where "city" column string data contain "/". Extract city data 
    df_i94port_comb.loc[119, "city"] = "IOWA CITY"
    df_i94port_comb.loc[208, "city"] = 'ST PAUL'
    df_i94port_comb.loc[258, "city"] = 'RALEIGH'
    df_i94port_comb.loc[293, "city"] = "NEWARK"
    df_i94port_comb.loc[307, "city"] = "RENO"

    # clean "city" column data that contains " - ". Extract city data after string.split() 
    df_i94port_comb["city"] = df_i94port_comb["city"].apply(lambda x: x.split(" - ")[1] if " - " in x else x)

    # Get state_code from "i94state_code.txt" for checking state_code in df_i94port_comb 
    df_states = pd.read_csv(input_data + "i94state_code.txt", sep="=", header=None)
    df_states.columns = ["state_code", "state_name"]
    df_states["state_code"] = df_states["state_code"].apply(remove_extra_quote)
    df_states["state_name"] = df_states["state_name"].apply(remove_extra_quote)

    # select columns for df_i94port_comb to create df_i94port_comb2.
    df_i94port_comb2 = df_i94port_comb[["i94port_code", "city", "state"]]
    # Rename the column
    names = ['i94port_code', 'city', 'state_code']
    df_i94port_comb2.columns = names

    # check state_code of df_i94port_comb2 to see if they are real state_code. The previous steps select length of string data as 2.
    # create state code list
    state_code_ls = set(df_states["state_code"])
    # print out 2 letter codes that are not in state code list.  "MX" and "5)" are not in the list. "MX" is Mexico. 
    for i in df_i94port_comb2["state_code"]:
        if i not in state_code_ls:
            print(i)

    # de-select data with "state_code" of "MX" (Mexico) 
    df_i94port_comb2 = df_i94port_comb2[df_i94port_comb2["state_code"]!="MX"]

    # clean the with "state_code" of "5)" 
    df5= df_i94port_comb2[df_i94port_comb2["state_code"] =="5)"]
    df_i94port_comb2.loc[463, "state_code"] ="VT"
    df_i94port_comb2.loc[463, "city"] ="DERBY LINE"

    # Merge df_i94port_comb2 and df_states to add "state_name" to the dataframe
    df_i94port_comb3 = df_i94port_comb2.merge(df_states, left_on="state_code", right_on="state_code")
    df_i94port_comb3 = df_i94port_comb3.drop_duplicates()

    df_i94port_comb3.to_csv(output_data + "i94port_pd.csv", index=False)
    print("Write i94port_pd table")

if __name__ == "__main__":
    # Getting input_data, output_data from Bash commmand inside airflow BashOperator.
    input_data = sys.argv[1]
    output_data = sys.argv[2]  
    
    # run the function to create table
    i94port_table(input_data, output_data)
