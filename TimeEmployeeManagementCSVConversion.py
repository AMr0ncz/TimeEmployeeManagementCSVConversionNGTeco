import os, re
import pandas as pd
import csv
from datetime import time, timedelta

"""
Loads the csv files
"""
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 150)

input_dir = "input"
output_dir = "output"
inputfiles, outputfiles = [], []
regex = re.compile('[^0-9:]')


for dir_, _, files in os.walk(input_dir):
    for file_name in files:
        inputfile_name = os.path.abspath(input_dir+"/"+file_name)
        new_filename= file_name[:-4]+"_output.csv"
        outputfile_name = os.path.abspath(output_dir+"/"+new_filename)
        inputfiles.append(inputfile_name.replace("\\","/"))
        outputfiles.append(outputfile_name.replace("\\","/"))

print(inputfiles)
print("\n")
print(outputfiles)


def load_in_csv_file(inputfile):
    allrows =[]
    with open(inputfile) as file:
        reader = csv.reader(file)
        for row in reader:
            allrows.append(row)

   
    df = pd.DataFrame(allrows)
    df = df.fillna(value="")
    
    iter_through_df(df)
    df = convert_IN_OUT_Time(df)
    print(df)
    print("\n")
    return df

def iter_through_df(df):
    columns = df.columns.tolist()
    for indx, i in df.iterrows():
        for c in columns:
            if( "." in i[c]):
                df.iat[indx, c] = float(df.iat[indx, c]) 

                
def convert_IN_OUT_Time(df):
    timestamp = 1
    total_hours, total_minutes = 0, 0
    columns = df.columns.tolist()
    for indx, i in df.iterrows():
        for c in columns:
            try:
                if(":" in  i[c]):
                    if(timestamp == 1):
                        first = i[c]
                        timestamp+=1
                        first = regex.sub('', first)
                        first_hours = first[:2].replace(":", "")
                        first_minutes = first[-2:].replace(":", "")
                        # create a time object with the microsecond granularity
                        first_time = time(int(first_hours), int(first_minutes), 0,0)
                        # get the hour and minute
                        hour = first_time.hour
                        minute = first_time.minute
                    else:
                        second = i[c]
                        timestamp = 1
                        second = regex.sub('', second)
                        second_hours = second[:2].replace(":", "")
                        second_minutes = second[-2:].replace(":", "")
                        second_time = time(int(second_hours), int(second_minutes), 0, 0)
                        # get the hour and minute
                        second_hour = second_time.hour
                        second_minute = second_time.minute
                        delta = abs((second_time.hour - first_time.hour)*60 + second_time.minute - first_time.minute + (second_time.second - first_time.second)/60.0)
                        delta_hours = int(delta / 60)
                        delta_minutes = int(  delta - (delta_hours * 60))
                        delta = time(delta_hours, delta_minutes, 0, 0)
                        total_hours += delta.hour
                        total_minutes += delta.minute
                        df.iat[indx, c+1] = delta
                        df.iat[indx, c+2] = delta
                        
                if("Total Hours" in  i[c]):
                    if(total_minutes > 60 ):
                        new_min = total_minutes % 60
                        new_hours = total_hours + int(total_minutes / 60)
                    else:
                        new_min = total_minutes 
                        new_hours = total_hours + int(total_minutes / 60)
                    
                    new_total_hours = timedelta(hours=new_hours, minutes=new_min)
                    df.iat[indx, c+5] =  new_total_hours #str(new_hours+":"+new_min)
                    
            except TypeError:
                continue
    return df

def saveCSV(df, targetDestination):
    targetDestination = targetDestination.replace(input_dir, output_dir )
    df.to_csv(os.path.abspath(targetDestination), sep=';', date_format='%Y%m%d', decimal="." )
    

for infile, outfile in zip(inputfiles, outputfiles):

    fileDF = load_in_csv_file(infile)

    saveCSV(fileDF, outfile)