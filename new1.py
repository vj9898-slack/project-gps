import sys  # Why are you importing this?  What reason do you need this?
import datetime  # Why do you need this?  How is it used?
import pandas as pd  # What is this used for?
import copy
import time
# import xlwt
# Why use this?
# How does PANDAS help us?# -*- coding: utf-8 -*-


import pickle
from threading import *
import glob
import os
# import pandas as pd
from multiprocessing.pool import ThreadPool


# Print usage if we don't have enough args.
# =============================================================================
# def usage() :
#     if len(sys.argv) < 3:
#         print('Usage: python3 GPS_to_KML.py GPS_Filename.txt KML_Filename.kml')
#         exit()
# 
#     # Get the filename args.
#     gps_filename	= sys.argv[1]
#     kml_filename	= sys.argv[2]
# 
# 
# # Open the gps data file to read.
# gps_file	= open(gps_filename, 'r')
# # Create an empty dictionary for the gps data.
# gps_info	= {}
# 
# =============================================================================

# A function to read the file and fill a dictionary with datapoint information:
#
# This should be given a file name and return a data structure.
# One might want to use pynema instead.
#
def get_gps_data(gps_filename):
    gps_info = {}
    for i in gps_filename:

        gps_file = open(i, 'r')
        # Create an empty dictionary for the gps data.

        dater = i.split("/")[-1]

        year = dater[:4]
        month = dater[5:7]
        day = dater[8:10]

        final = year + "-" + month + "-" + day + " "

        # For every line in the gps data file.
        for line in gps_file:
            # If this line is an RMC line.
            try:
                if line.split(',')[0] == "$GPRMC":
                    # Split the line on commas and set each value to a variable (based on its location per the protocol).
                    try:
                        label, timestamp, validity, latitude, lat_dir, longitude, long_dir, knots, course, \
                        datestamp, variation, var_dir, checksum = line.split(',')
                    # If this line is missing a value or has too many, assume it's an error and skip the line.
                    except ValueError:
                        continue

                    # If the validity is 'V' (per the protocol, a gps error), skip the line.
                    if validity == 'V':
                        continue

                    # Get the timestamp into a datetime object.
                    # Start by getting the HHMMSS part, the part before the '.'.
                    timestamp_split = timestamp.split('.')[0]
                    # Join every two characters with ':' (that is, get it into HH:MM:SS form), and then append the MS part.
                    time_str = ':'.join(
                        [timestamp_split[index:index + 2] for index in range(0, len(timestamp_split), 2)]) + \
                               ':' + timestamp.split('.')[1]
                    time_str = final + time_str

                    # Use strptime to convert the string we just put together into a datetime object.
                    date_time_obj = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S:%f')

                    # Convert the latitude into degrees.
                    # This math might be wrong.
                    degrees = int(float(latitude) / 100)
                    mins = float(latitude) - (degrees * 100)
                    # N is positive, S is negative.
                    if lat_dir == 'N':
                        fixed_latitude = degrees + (mins / 60)
                    else:
                        fixed_latitude = -degrees + (-mins / 60)

                    # Convert the longitude into degrees.
                    degrees = int(float(longitude) / 100)
                    mins = float(longitude) - (degrees * 100)
                    # E is positive, W is negative.
                    if long_dir == 'E':
                        fixed_longitude = degrees + (mins / 60)
                    else:
                        fixed_longitude = -degrees + (-mins / 60)

                    # Add this line's information to the dictionary, with the timestamp as the key.
                    # Using the timestamp means that multiple lines from the same time (e.g. if there's also GGA from this time)
                    # will be combined.
                    # If it's already in the dictionary, we want to update the entry instead of setting to keep from overriding
                    #    any GGA information from that time (such as altitude).
                    if timestamp in gps_info:
                        gps_info[date_time_obj].update({'datetime': date_time_obj, 'latitude': round(fixed_latitude, 6),
                                                        'longitude': round(fixed_longitude, 6), 'knots': float(knots),
                                                        'course': course, 'variation': variation})
                    else:
                        gps_info[date_time_obj] = {'datetime': date_time_obj, 'latitude': round(fixed_latitude, 6),
                                                   'longitude': round(fixed_longitude, 6),
                                                   'knots': knots, 'course': course, 'variation': variation}

                # If this line is a GGA line.
                elif line.split(',')[0] == "$GPGGA":
                    # If the GPS quality indicator is not zero, this line should be valid.

                    if line.split(',')[6] != '0':
                        # Split the line on commas and set each value to a variable (based on its location per the protocol).
                        try:
                            label, timestamp, latitude, lat_dir, longitude, long_dir, gps_quality, num_satellites, horiz_dilution, \
                            antenna_alt, antenna_alt_units, geoidal, geo_units, age_since_update, checksum = line.split(
                                ',')
                        # If this failed, that means something is wrong with this line.
                        except ValueError:
                            continue

                        # Convert the latitude into degrees.
                        degrees = int(float(latitude) / 100)
                        mins = float(latitude) - (degrees * 100)
                        # N is positive, S is negative.
                        if lat_dir == 'N':
                            fixed_latitude = degrees + (mins / 60)
                        else:
                            fixed_latitude = -degrees + (-mins / 60)

                        # Convert the longitude into degrees.
                        degrees = int(float(longitude) / 100)
                        mins = float(longitude) - (degrees * 100)
                        # E is positive, W is negative.
                        if long_dir == 'E':
                            fixed_longitude = degrees + (mins / 60)
                        else:
                            fixed_longitude = -degrees + (-mins / 60)
                        timestamp_split = timestamp.split('.')[0]
                        # Join every two characters with ':' (that is, get it into HH:MM:SS form), and then append the MS part.
                        time_str = ':'.join(
                            [timestamp_split[index:index + 2] for index in range(0, len(timestamp_split), 2)]) + \
                                   ':' + timestamp.split('.')[1]
                        # Use strptime to convert the string we just put together into a datetime object.
                        time_str = final + time_str

                        # Use strptime to convert the string we just put together into a datetime object.
                        date_time_obj = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S:%f')

                        # Add this line's information to the dictionary, with the timestamp as the key.
                        # Using the timestamp means that multiple lines from the same time
                        # (e.g. if there's also RMC from this time) will be combined.
                        # If it's already in the dictionary, we want to update the entry instead of setting to keep
                        #   from overriding any RMC information from that time (such as course).
                        if timestamp in gps_info:
                            gps_info[date_time_obj].update(
                                {'datetime': date_time_obj, 'latitude': round(fixed_latitude, 6),
                                 'longitude': round(fixed_longitude, 6), 'knots': float(knots),
                                 'course': course, 'variation': variation})
                        else:
                            gps_info[date_time_obj] = {'datetime': date_time_obj, 'latitude': round(fixed_latitude, 6),
                                                       'longitude': round(fixed_longitude, 6),
                                                       'knots': knots, 'course': course, 'variation': variation}

                # We don't care about any other protocols.
                else:
                    continue
            except:
                continue
    return gps_info


#
# A function to remove redundant entries from the dictionary (to lower the number of points while preserving the line).
#
# For example, if the car is traveling in a straight line, then
# delete the middle point.
# Or, maybe don't.
# Perhaps there is a bitter method.
def remove_redundant_GPS_points(gps):
    # Get a list of all of the keys for the dictionary of GPS info.
    gps_info = gps
    key_list = list(gps_info.keys())
    # Iterate through the list of keys.
    for key_index in range(0, len(key_list) - 1):
        # Get the coordinates of the current and next entries in the dictionary.
        this_latitude = gps_info[key_list[key_index]]['latitude']
        next_latitude = gps_info[key_list[key_index + 1]]['latitude']
        this_longitude = gps_info[key_list[key_index]]['longitude']
        next_longitude = gps_info[key_list[key_index + 1]]['longitude']

        # If the coordinates are the same (rounded down a bit, to account for GPS skew)
        # meaning we haven't moved, remove one of the values.
        if round(this_latitude, 5) == round(next_latitude, 5) and round(this_longitude, 5) == round(next_longitude, 5):
            gps_info.pop(key_list[key_index])
            continue

        # If we didn't remove the data for the key right before this one, and this isn't the first key.
        if key_index != 0 and key_list[key_index - 1] in gps_info:

            # Get coordinates of the preceding entry.
            last_latitude = gps_info[key_list[key_index - 1]]['latitude']
            last_longitude = gps_info[key_list[key_index - 1]]['longitude']

            # Get the latitude and longitude differences in both directions.
            lat_diff_from_last = abs(this_latitude - last_latitude)
            long_diff_from_last = abs(this_longitude - last_longitude)
            lat_diff_from_next = abs(this_latitude - next_latitude)
            long_diff_from_next = abs(this_longitude - next_longitude)

            # If our speed is constant and we're moving in a straight line, remove the unnecessary point in between.
            tolerance = 0.01  # This is WAY TOO BIG!!
            if abs(lat_diff_from_last - lat_diff_from_next) <= tolerance and \
                    abs(long_diff_from_last - long_diff_from_next) <= tolerance:
                gps_info.pop(key_list[key_index])
                continue

            # If the last entry has speed data (some lines may not if they were only GGA).
            if 'knots' in gps_info[key_list[key_index - 1]]:

                # Get the speed of the last entry.
                last_speed = float(gps_info[key_list[key_index - 1]]['knots'])

                # Remove the entry if the difference in latitude or longitude is too high compared to the speed.
                # This manages errors where a GPS reading is way off (e.g. in the middle of the ocean.)
                if long_diff_from_last / 10 > last_speed or lat_diff_from_next / 10 > last_speed:
                    gps_info.pop(key_list[key_index])
                    continue
    df = pd.DataFrame(gps_info)
    return gps_info, df.transpose()


def write_out_KML_file():
    # Open it to write (such that it will be created if it does not already exist).
    kml_file = open(kml_filename, 'w+')
    # Write the heading and style information.
    kml_file.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
    kml_file.write("<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n")
    kml_file.write("<Document>\n")
    kml_file.write("<Style id=\"cyanPoly\">\n"
                   "\t<LineStyle>\n"
                   "\t\t<color>ffffff00</color>\n"
                   "\t\t<width>6</width>\n"
                   "\t</LineStyle>\n"
                   "\t<PolyStyle>\n"
                   "\t\t<color>ffffff00</color>\n"
                   "\t</PolyStyle>\n"
                   "</Style>\n"
                   "<Placemark><styleUrl>#cyanPoly</styleUrl>\n"
                   "<LineString>\n"
                   "<Description>Speed in Knots, instead of altitude.</Description>\n"
                   "\t<extrude>1</extrude>\n"
                   "\t<tesselate>1</tesselate>\n"
                   "\t<altitudeMode>absolute</altitudeMode>\n"
                   "<coordinates>\n")

    # Every line is guaranteed to have a latitude and longitude.
    # If one does not have an altitude, it will just use the last altitude we had.
    last_alt = 0
    # Iterate through all of the entries we didn't prune.
    for line in gps_info.values():
        if 'altitude' in line:
            last_alt = line['altitude']
        # Write the coordinates to the file.
        kml_file.write('\t' + str(line['longitude']) + ',' + str(line['latitude']) + ',' + str(last_alt) + '\n')

    # Closing up the file.
    kml_file.write("        </coordinates>\n")
    kml_file.write("      </LineString>\n")
    kml_file.write("    </Placemark>\n")
    kml_file.write("  </Document>\n")
    kml_file.write("</kml>\n")


#
#  Here is the main routine:
# 
def get_filenames(files_path):
    """
    Find all the CSV files and folders in the file directory
    :param files_path: file path
    :return: return data
    """
    # files = []
    #
    # for filename in glob.glob(files_path + "/*/*", recursive=True):
    #
    #     # if os.path.isdir(filename):
    #
    #     #     filename=filename+"\*"
    #     #     for f in glob.iglob(filename, recursive=True):
    #
    #     if os.path.isfile(filename):  # filter dirs
    #         if ".txt" in filename and "gps" in filename:
    #             files.append(filename)

    filelist = []

    for root, dirs, files in os.walk(files_path): # walk through every sub directory and files and every level
        for file in files:
            # append the needed file name to the list
            if ".txt" in file and "gps" in file:
                filelist.append(os.path.join(root, file))

    # print all the file names
    # for name in filelist:
    #     print(name)

    return filelist

"""
Sort the dataframe by datetime (in this case) in ascending order
"""
def sortDataframe(df):
    # print(type(df.datetime[0]))
    df = df.sort_values(by='datetime')
    return df


"""
Calculate delta i.e. difference in datetime values between current row and next row
"""
def calculateDelta(df):
    shiftDatetime = df['datetime'].shift() # shift the datetime column by 1
    # print(shiftDatetime)

    # calculate delta between current datetime and previous datetime
    df['deltaSeconds'] = (df['datetime'] - shiftDatetime).dt.total_seconds()
    df['deltaMinutes'] = (df['datetime'] - shiftDatetime).dt.total_seconds() / 60
    df['deltaHours'] = (df['datetime'] - shiftDatetime).dt.total_seconds() / 60 / 60
    df['deltaDays'] = (df['datetime'] - shiftDatetime).dt.total_seconds() / 60 / 60 / 24

    # get the first index value
    firstIndex = df.first_valid_index()

    # change the delta value for the first index to zero
    df.at[firstIndex, 'deltaSeconds'] = 0
    df.at[firstIndex, 'deltaMinutes'] = 0
    df.at[firstIndex, 'deltaHours'] = 0
    df.at[firstIndex, 'deltaDays'] = 0

    # df['differenceInSeconds'] = (df['delta']).dt.total_seconds()

    # print(df.head)

    return df



def myApproach(df):
    dfNew = df[['date', 'datetime', 'latitude', 'longitude', 'deltaMinutes']]

    df_group_15_45_mins = dfNew.groupby(['latitude', 'longitude'])#.agg({'datetime': ['min','max']})

    print(df_group_15_45_mins)
    print("-------------")

    dfNew = df[['datetime', 'latitude', 'longitude']]
    df_group_24_hours = dfNew.groupby(['latitude', 'longitude']).agg({'datetime': ['min', 'max']})

    print(df_group_24_hours)


"""
A function to calculate cumulative delta for the places where same latitude and longitude values are present.
Meaning if the person is in the same place in consecutive entries, we add their delta (amount of time they stay there).
df is the old dataframe.
dfNew is the dataframe with cumulative delta.
"""
def calculateCumulativeDelta(df):

    print('Cumulative Delta Calculation Begin')

    # to keep track of cumulative delta for same lat and long
    df['cumulativeDelta'] = 0

    # df = df.head(10)

    # new dataframe to hold values with final cumulative delta for the same lat and long in consecutive datetime entries
    dfNew = pd.DataFrame(columns = ['datetime', 'latitude', 'longitude', 'knots', 'course', 'variation',
       'date', 'deltaSeconds', 'deltaMinutes', 'deltaHours', 'deltaDays', 'cumulativeDelta'])

    # Next 10 lines - this is an example to test the working of this function

    # tempDict = {'datetime': [1,1,1,2,2,3,4,5],
    #     'latitude': [20,20,20,20,21,21,23,25],
    #     'longitude': [20,20,20,20,20,20,30,30],
    #     'deltaSeconds': [0,1,1,2,1,4,7,3],
    #     'cumulativeDelta': [0,0,0,0,0,0,0,0]}
    #
    # df = pd.DataFrame(tempDict)
    #
    # dfNew = pd.DataFrame(columns=['datetime','latitude','longitude','deltaSeconds','cumulativeDelta'])

    length = len(df) # no of rows in dataframe
    newDfCount = 0 # no of entries in new dataframe
    i = 0
    getOut = 0 # variable to get out of both loops
    while i < length:
        j = i + 1
        while j <= length:
            print("i = " + str(i), end = "----")
            print("j = " + str(j), end = "----")
            if getOut == 1:
                break

            if j == length: # if we have just passed the final row
                # put j-1 (the final row) in dfNew

                if df.loc[j - 1, 'cumulativeDelta'] > 15*60:
                    dfNew.loc[newDfCount, 'datetime'] = df.loc[j - 1, 'datetime']
                    dfNew.loc[newDfCount, 'latitude'] = df.loc[j - 1, 'latitude']
                    dfNew.loc[newDfCount, 'longitude'] = df.loc[j - 1, 'longitude']
                    dfNew.loc[newDfCount, 'knots'] = df.loc[j - 1, 'knots']
                    dfNew.loc[newDfCount, 'course'] = df.loc[j - 1, 'course']
                    dfNew.loc[newDfCount, 'variation'] = df.loc[j - 1, 'variation']
                    dfNew.loc[newDfCount, 'date'] = df.loc[j - 1, 'date']
                    dfNew.loc[newDfCount, 'deltaSeconds'] = df.loc[j - 1, 'deltaSeconds']
                    dfNew.loc[newDfCount, 'deltaMinutes'] = df.loc[j - 1, 'deltaMinutes']
                    dfNew.loc[newDfCount, 'deltaHours'] = df.loc[j - 1, 'deltaHours']
                    dfNew.loc[newDfCount, 'deltaDays'] = df.loc[j - 1, 'deltaDays']
                    dfNew.loc[newDfCount, 'cumulativeDelta'] = df.loc[j - 1, 'cumulativeDelta']
                    newDfCount += 1

                # get out of both loops
                getOut = 1

            elif j < length:

                # if lat and long at i and j rows are the same, add their delta to the cumulative count
                if df.loc[i, "latitude"] != df.loc[j, "latitude"] or df.loc[i, "longitude"] != df.loc[j, "longitude"]:
                    # put j-1 entry in dfNew
                    if df.loc[j - 1, 'cumulativeDelta'] > 15 * 60:
                        dfNew.loc[newDfCount,'datetime'] = df.loc[j-1,'datetime']
                        dfNew.loc[newDfCount,'latitude'] = df.loc[j-1,'latitude']
                        dfNew.loc[newDfCount, 'longitude'] = df.loc[j - 1, 'longitude']
                        dfNew.loc[newDfCount, 'knots'] = df.loc[j - 1, 'knots']
                        dfNew.loc[newDfCount, 'course'] = df.loc[j - 1, 'course']
                        dfNew.loc[newDfCount, 'variation'] = df.loc[j - 1, 'variation']
                        dfNew.loc[newDfCount, 'date'] = df.loc[j - 1, 'date']
                        dfNew.loc[newDfCount, 'deltaSeconds'] = df.loc[j - 1, 'deltaSeconds']
                        dfNew.loc[newDfCount, 'deltaMinutes'] = df.loc[j - 1, 'deltaMinutes']
                        dfNew.loc[newDfCount, 'deltaHours'] = df.loc[j - 1, 'deltaHours']
                        dfNew.loc[newDfCount, 'deltaDays'] = df.loc[j - 1, 'deltaDays']
                        dfNew.loc[newDfCount, 'cumulativeDelta'] = df.loc[j - 1, 'cumulativeDelta']
                        newDfCount += 1

                    # i = j
                    i = j

                    # j = i + 1
                    j = i + 1

                    # cumulative delta at i = delta at i
                    # df.loc[i, 'cumulativeDelta'] = df.loc[i, 'deltaMinutes']

                    # cumulative delta at i = 0
                    df.loc[i, 'cumulativeDelta'] = 0 # this person has stayed here for zero time

                elif df.loc[i, "latitude"] == df.loc[j, "latitude"] and df.loc[i, "longitude"] == df.loc[j, "longitude"]:
                    # add delta to cumulative count and update cumulative delta - at j
                    df.loc[j, 'cumulativeDelta'] += df.loc[j - 1, 'cumulativeDelta'] + df.loc[j, 'deltaSeconds']

                    # j = j + 1
                    j = j + 1

            print("newDfCount = " + str(newDfCount), end = "----\n\n")

        if getOut == 1:
            break

    return df, dfNew



# Call the functions to run the program overall.
#     if len(sys.argv) < 3:
# 	usage() 
# 	quit()

root = "data" # folder where all input data files and folders are kept

files = get_filenames(root) # function to get all the files names in the root folder above
# print(files)

d = get_gps_data(files) # extract data from gps files
d1 = copy.deepcopy(d) # make a copy
print('Instead of removing redundant GPS points.  Identify where the car is spending most of its time.')

# print(d)
datadict,df = remove_redundant_GPS_points(d)
# print(df.head)


df = sortDataframe(df) # sort the dataframe by datetime in ascending order
print()
print('df imported and sorted on datetime')
print()

df['date'] = df['datetime'].dt.date
# print(df['date'])

df = calculateDelta(df) # calculate delta between previous datetime and current datetime value
# print(df.shape)

df = df.reset_index(drop=True) # reset the indices
print()
print('delta calculated')
print()

# myApproach(df)


start_time = time.time()

# a function to calculate cumulative delta for the places where same latitude and longitude values are present
# meaning if the person is in the same place in consecutive entries, we add their delta (amount of time they stay there)
# df is the old dataframe
# dfNew is the dataframe with cumulative delta
df, dfNew = calculateCumulativeDelta(df)

endTime = time.time()

print("------------------")
print("Cumulative Delta function finished in: " + str(endTime - start_time))
print("------------------")

dfNew.to_csv("dfNew.csv") # new dataframe

df15minOrMore = dfNew[dfNew['cumulativeDelta'] > 15*60] # data values with > 15 minutes
df15minOrMore.to_csv("df15minOrMore.csv")

df45minOrMore = dfNew[dfNew['cumulativeDelta'] > 45*60] # data values with > 45 minutes
df45minOrMore.to_csv("df45minOrMore.csv")

df24hoursOrMore = dfNew[dfNew['cumulativeDelta'] > 24*60*60] # data values with > 24 hours
df24hoursOrMore.to_csv("df24hoursOrMore.csv")


print("----")
print(dfNew.columns)


# print('Instead of emitting a KML file with a path, emit a KML file with LOCATIONS on it.')
# write_out_KML_file()
