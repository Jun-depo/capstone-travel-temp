
def lon_coordinates(s):
    """
    extract longitude from coordinates column of "airport-codes_csv.csv"
    input:
    s: string from each row of the column
    """
    lon = s.split(", ")[0]   
    return lon

def lat_coordinates(s):
    """
    extract latitude from coordinates column of "airport-codes_csv.csv"
    s: string from each row of the column
    """
    lat = s.split(", ")[1]   
    return lat


def convert_latitude(s):
    """
    Convert latitude to DD format. eg. 32.01N to 32.01, 32.01S to -32.01 for Latitude column from "Global_city_temperatures.csv"
    s: string from each row of the column
    """
    if s[-1] == "N":
        return s[:-1]
    elif s[-1] == "S":
        return "-" + s[:-1]

def convert_longitude(s):
    """
    Convert longitude to DD format. eg. 100.01E to 100.01, 100.01W to -100.01 for Longitude column from "Global_city_temperatures.csv"
    s: string from each row of the column
    """
    if s[-1] == "E": 
        return s[:-1]
    elif s[-1] == "W": 
        return "-"+ s[:-1]    

def extract_month(dt):
    return dt[5:7]

def remove_extra_quote(st):
    return st.replace("'", "")

def clean_column_names(name_lst):
    """
    change column names by replacing " " and "-" with "_".   
    " " and "-" in column names can cause errors in running some programs
    Params:
        name_lst: a list containing column names
    """
    column_names = []
    for i in name_lst:
        a = i.lower()
        a = a.replace(" ", "_")
        a = a.replace("-", "_")
        column_names.append(a)
    return column_names


def remove_last2_from_colname(df):
    """
    Remove "2" if "2" is the last chracter of a column name.
    Input: Spark dataframe
    """
    new_colnames = []
    for i in df.columns:
        if i[-1]=="2":
            new_colnames.append(i[:-1])
        else:
            new_colnames.append(i) 
    df = df.toDF(*new_colnames)    
    return df
            
def counting_null_number_inColumns(df):
    """
    The function for countiong null values in each column.
        Params: 
        df: a spark dataframe
    """
    # df.columns provide a list of column names of df dataframe
    for i in df.columns:
        name = i + " is null"
        num = df.select(i).where(name).count()
        print(i, ": ", num)

        
def change_data_type(col_lst, df, datatype):
    """
    change datatype of Spark df columns
    Params:
        col_lst: A list of column (names) with corresponding datatype need to be changed.
        df: Spark dataframe for the datatype change.  
        datatype: datatype needs to convert to
    """
    for i in col_lst:
        a = "cast(" + i + " as " + datatype + ")"+ i +'2'
        df = df.selectExpr("*", a).drop(i)
    return df

def remove_row_with_str(st_lst, df, colname):
    """
    remove rows containing certain word string in a column (colname).
    Params:
        st_lst: a list of word strings 
        df: dataframe containing columns
        colname: column name
    """
    for i in st_lst:
        row_with_str = udf(lambda x: i in x)
        df = df.withColumn(colname+"2", row_with_str(colname))
        df = df.where(col(colname+"2") != True)
        df = df.drop(colname+"2")
    return df

def split_city_state(st):
    """
    Split columns containing both city and state data
    input: 
        st: a string
    """
    a = st.split()
    final_st = ""
    for i in a[:-1]:
        final_st += i + " "
    return final_st, a.pop()        

def remove_end_comma_space(st):
    """
    remove comma or space when the last element of a string is a comma or space.
    Params: 
        st: a string
    """
    if st[-1] == "," or st[-1] == " ":
        return st[:-1]
    else:
        return st
    
def get_lon(coord):
    """
    Extract longitude info from coordinate column in airport-codes_csv.csv
    Params: 
        coord: the coordinate of geo-location on earth
    """
    lon, lat = coord.split(",")
    return lon

def get_lat(coord):
    """
    Extract latitude info from coordinate column in airport-codes_csv.csv
    Params: 
       coord: the coordinate of geo-location on earth
    """
    lon, lat = coord.split(",")
    return lat

def extract_state(st):
    """
    function for extracting 2 letter state code for i94port_table.
    input: a string from "city_state" column
    """
    items = st.split()
    st=items[-1]    
    return st 

def extract_city(ct):
    """
    function for extracting city for i94port_table.
    Params: 
        st: a string from "city_state" column
    """
    items = ct.split()
    city =""    
    for i in items[:-1]:
        city += i + " "        
    return city[:-1] # to remove the last space (added)

def extract_state2(st):
    """
    another function for extracting 2 letter state code for i94port_table 
    Params: 
        st: a string from "city" column as "city" column" contains both city and state_code info 
    after applying extract_state and extract_city finctions.
    """
    idx = st.find(", ")
    if len(st[idx:])==4:
        return st[idx+2:]

def extract_city2(st):
    """
    function for extracting city for i94port_table.
    Params: 
        st: a string from "city" column (as "city" column" contains both city and state_code info 
    after applying extract_state and extract_city finctions).
    """
    idx = st.find(", ")
    return st[:idx]

def change_allowed_stay_date(x):
    """
    The function is for processing datetime data in "dtaddto" column of immigration table
    input: 
        x: a date string 
    """
    if x != "D/S" and x == datetime.strptime(x, '%m%d%Y').strftime('%m%d%Y'):
        return datetime.strptime(x, '%m%d%Y').strftime('%Y-%m-%d')
    elif x == "D/S": 
        return x 
    else:    
        return False
    