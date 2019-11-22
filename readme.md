# sparkts

## Context 

This library tries to implement some operators to manipulate time series in Spark

Before the operators, we have a part for reading, and indexing time series which is still a work in progress.

Two main files exist at this level:

1. io_functions.ipynb
2. operators.ipynb

## IO Functions

This file contains the functions to read the raw time series and to save the processed time series partitioned by a time interval.

### read_store()
This function reads the raw data, process them depending on some parameters and then stores them.
The signature is:
```python
def read_and_store(input_path, output_path, inferSchema=False, header=True, timestampFormat='yyyy-MM-dd HH:mm:ss+ss',
                   timestampColumnName='timestamp', inner_granularity='1 minute', 
                   fill_skipped=True, fill_mode='null', global_granularity='1 month'):
```
* input_path is the path to the input file (for now only CSV files are supported)
* output_path is the path to the output folder
* inner_granularity, this attribute hints the function about the smallest interval of time between two observations
* global_granularity, this attribute decides on how much time we would like to partition our data
* fill_skipped, this should optimally be True, and it will allow the function to fill missing values, if it is set to False then missing values are not filled, the thing that will leave unequal intervals between observations
* fill_mode, how we would like to fill the missing values, null means that we only put null, other methods that could be implemented include imputations with mode, average, frequent items, etc.

The values of the attributes inner and global granularities should be of the format: *n time*, where *n* is an integer and *time* is one of:
- second
- minute
- hour
- week
- month
- year

This method could simply be used like:
```python
read_and_store('part1.csv', 'part_null', timestampColumnName='time', global_granularity='1 day')
```

### unpack()
After processing and storing the time series, we can use the `unpack()` function to read the data into Spark.
This is the signature of the method:
```python
def unpack(base_dir, paths, drop_all_filled_nulls=True):
```
The function will unpack the time series and reconstruct the whole data depending on the meta-information (inner and global granularities)

- base_dir is the directory where the files exist
- paths is a list of the partitions that we want to read from the base_dir
- drop_all_filled_nulls is set to True by default, and it instructs the function to drop all nulls that where filled during the processing phase

## Operators
For now 6 Operators are implemented

1. Temporal Selection: ex. `temporal_selection('TSel[activity==Rue]', df)`, this will return only the records that satisfy the condition
2. Window Selection: ex. `window_selection('WSel[01/06/2019,30/07/2019]', df)`, this will select only the data within the specifc dates, it tries to load form disk only data related to these dates
3. Temporal Projection: ex. `temporal_projection('TProj[PM10*2]', df)`, this will transform the PM10 attribute into PM10 * 2
4. Shift: ex. `shift('Shift[1 hour]', df)`, this will put the current data, and next to them the data after 1 hour for comparison purposes
5. Temporal Aggregation: ex. `temporal_aggregation('TAgg[1 day,avg(PM10)]', df)`, this will give the average value of the PM10 attribute for each day (fixed window)
6. Window Aggregation: ex. `window_aggregation('WAgg[1 day,avg(PM10)]', df)`, this will give the average value of the PM10 for each 24 hours through a sliding window

## Future Works
Still some spatial operators need to be developed like spatial aggregation (for example, for each area, compute the highest value of a specific attribute)
