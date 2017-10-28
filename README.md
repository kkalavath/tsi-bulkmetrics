# tsi-bulkmetrics-csvimport

### Description

This script assists in creating metrics and loading measurement data into TrueSight Intelligence from a CSV file.

- tsi-bulkmetrics-csvimport.py - main script
- metric.json - metric configuration file

### Pre-requisites
- **Only works with Python 3.x** (Python 2.x compatibility coming soon...)
- Libraries: pandas, requests, argparse

### What the script does

- Creates TSI metrics from the command line by parsing the metric.json file
- Loads measurement data in bulk (DEFAULT: 500 measures at once) from a CSV file
- It reads the source, metric name, timestamp and measurement column from a CSV file
- It stages the data to send measurements from the same source in sequence.

### There are some limitations, however

- It is single threaded.

### Examples
#### Create a Metric
```
python3 tsi-bulkmetrics-csvimport.py metric -f metric.json -e myemail@email.com -k my-api-key-goes-here
```
#### Send Measurements
```
python3 tsi-bulkmetrics-csvimport.py measures -s source -m metric -a MyApp -e myemail@email.com -k my-api-key-goes-here -tscol myts -valcol metric_name -f /path/to/measurements.xlsx

```
#### Options for Creating a Metric
```
usage: tsi-bulkmetrics-csvimport.py metric [-h] -k APIKEY -e EMAIL -f METRICFILE

Parameters:
  -h, --help            show this help message and exit
  -k APIKEY, --apikey APIKEY
                        TrueSight Intelligence API Key
  -e EMAIL, --email EMAIL
                        TrueSight Intelligence Account Email
  -f METRICFILE, --metricfile METRICFILE
                        File containing metric JSON definition
```

#### Options for Sending Measurements
```
usage: tsi-bulkmetrics-csvimport.py measures [-h] -k APIKEY -e EMAIL -f MEASURESFILE -s
                                   SOURCE -m METRICNAME -a APPID
                                   [-tscol TSCOL] [-valcol VALCOL]

Parameters:
  -h, --help            show this help message and exit
  -k APIKEY, --apikey APIKEY
                        TrueSight Intelligence API Key
  -e EMAIL, --email EMAIL
                        TrueSight Intelligence Account Email
  -f MEASURESFILE, --measuresfile MEASURESFILE
                        Excel file containing measurement data
  -s SOURCE,            Column name of the source data. 
                        Measurement source column (e.g. sym)
  -m METRICNAME,        Column name of the metric. 
                        Name of Metric column (e.g. metric)
  -a APPID, --appid APPID
                        TrueSight Intelligence App ID
  -tscol TSCOL          Column name of timestamp data. DEFAULT: ts
  -valcol VALCOL        Column name of measure data. DEFAULT: value
```
