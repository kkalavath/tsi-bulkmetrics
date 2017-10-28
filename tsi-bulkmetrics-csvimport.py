import json
import pandas as pd
import time
import requests
import argparse
import csv

METRICAPI = "https://api.truesight.bmc.com/v1/metrics"
MEASUREMENTSAPI = "https://api.truesight.bmc.com/v1/measurements"
EVENTAPI = "https://api.truesight.bmc.com/v1/events"
BATCH = 500

def getArgs():

    parser = argparse.ArgumentParser(description='TrueSight Intelligence - Bulk Measures Ingestion')
    subparsers = parser.add_subparsers(help='You must choose one of these options', dest='command')

    # Metric options
    parser_metric = subparsers.add_parser('metric', help='Options for Creating a Metric')
    parser_metric._optionals.title = 'Parameters'
    parser_metric.add_argument('-k','--apikey', help='TrueSight Intelligence API Key', required=True)
    parser_metric.add_argument('-e','--email', help='TrueSight Intelligence Account Email', required=True)
    parser_metric.add_argument('-f','--metricfile', help='File containing metric JSON definition', required=True)
    parser_metric.set_defaults(func=create_metric)

    # Measurement options
    parser_measures = subparsers.add_parser('measures', help='Options for Sending Measurements')
    parser_measures._optionals.title = 'Parameters'
    parser_measures.add_argument('-k', '--apikey', help='TrueSight Intelligence API Key', required=True)
    parser_measures.add_argument('-e', '--email', help='TrueSight Intelligence Account Email', required=True)
    parser_measures.add_argument('-f','--measuresfile', help='Excel file containing measurement data', required=True)
    parser_measures.add_argument('-s', '--source', help='Measurement source (e.g. MyServer)', required=True)
    parser_measures.add_argument('-m', '--metricname', help='Name of Metric (e.g. MY_COOL_METRIC)', required=True)
    parser_measures.add_argument('-a', '--appid', help='TrueSight Intelligence App ID', required=False)
    parser_measures.add_argument('-tscol', help='Column name of timestamp data. DEFAULT: ts', default="ts", required=False)
    parser_measures.add_argument('-valcol', help='Column name of measure data. DEFAULT: value', default="value", required=False)
    parser_measures.set_defaults(func=send_measures)

    ## Troubleshooting

    # parser.add_argument('-t', '--test', help='Test mode: print output, but do not create metric or send measures', action="store_true", required=False)

    args = parser.parse_args()

    return args

def create_metric(args):

    #print(args)

    # Try opening the metric.json file
    try:
        with open(args.metricfile) as data_file:
            metric = json.load(data_file)
    except FileNotFoundError:
        print('ERROR: There was an error opening the metric JSON file. Please check the path and file name.')
        exit(1)
    # If successful, call the API to create the metric
    else:
        print("Creating metric...")
        print(json.dumps(metric, indent=4))
        r = requests.post(METRICAPI, data=json.dumps(metric), headers={'Content-type': 'application/json'}, auth=(args.email, args.apikey))
        print("Metric Status: %s - %s" % (r.status_code,r.reason))

    return True

def parse_data(args):

    # Open the measures Excel file
    df = pd.read_csv(args.measuresfile)
	
    # Iterate and create tuples
    data = []
    for index, row in df.iterrows():
        tup = (row[args.tscol], row[args.valcol],row[args.source], row[args.metricname])
        data.append(tup)

    #sort data and return if not already ordered by time
    #sorted(data, key=lambda tup: tup[0])
    #return sorted(data, key=lambda tup: tup[0])
    return data

def create_batch(data,args):

    measures = []
    measuresbatch = []
    measurecount = 0 # reader handles header
    batchcount = 1
    prevsource = ""
    currsource = ""
    samesource = False
    numbatches = 0

    print("Total number of measures: %s" % len(data))

    # Iterate through measure data
    for item in data:

        #print("Measure num: %s of %s" % (measurecount,len(data)))
        #print(item)
        currsource = item[2] + "_" + item[3]
        if(prevsource == ""):
            samesource = True
        elif (prevsource == currsource):
            samesource = True
        else:
            samesource = False
           

        # Create JSON for each measurement
        measure = [
            item[2],  # source
            item[3],  # metric name, identifier in Pulse.
            float(item[1]),  # measure
            int(item[0]),  # timestamp
            {"app_id": args.appid}  # metadata
        ]
        
        if(samesource):
            # Append measurement JSON to list
            measures.append(measure)

            # Determine batch info/position and append measures list object to batch after batch limit is reached
            if measurecount == len(data):
                numbatches += 1
                print("Creating final batch..%s"  % (str(numbatches)))
                measuresbatch.append(measures)
            elif batchcount < BATCH:
                batchcount = batchcount + 1
            else:
                batchcount = 1
                numbatches += 1
                print("Creating batch...%s"  % (str(numbatches)))
                measuresbatch.append(measures)
                measures = []
                # possible sleep here...
        else:
            numbatches += 1
            print("Creating batch....%s"  % (str(numbatches)))
            measuresbatch.append(measures)
            #Reset the batch
            batchcount = 1
            measures = []
            # Append measurement JSON with new source to list
            measures.append(measure)
            if measurecount == len(data):
                numbatches += 1
                print("Creating final batch....%s"  % (str(numbatches)))
                measuresbatch.append(measures)

        measurecount = measurecount + 1
        prevsource = currsource

    return measuresbatch


def send_measures(args):

    tds=str(time.ctime(time.time()))
    print("Main thread started @ ", tds)
    
    #print(args)
    #print(args.email)

    # Parse the measure data
    data = parse_data(args)

    # Create the payload
    payload = create_batch(data, args)

    # For each chunk of data, POST to the API
    for chunk in payload:
        #print("..................")
        #print(chunk)

        try:
            #print(chunk)
            r = requests.post(MEASUREMENTSAPI, data=json.dumps(chunk), headers={'Content-type': 'application/json'}, auth=(args.email, args.apikey))
        except requests.exceptions.RequestException as e:
            print(e)
            exit(1)
        else:
            #print(json.dumps(chunk,indent=4))
            print("Response: %s - %s" % (r.status_code, r.reason))
        finally:
            #print("Taking a break for 1 second...")
            time.sleep(1)

    tde=str(time.ctime(time.time()))
    print("Main thread ended @ ", tde)
    return True


def main():

    args = getArgs()
    #print(args)

    if args.command is not None:
        r = args.func(args)
    else:
        r = False
        print("Usage: python tsi-bulkmetrics.py [metric | measures] -h")

    if r:
        exit(0)
    else:
        exit(1)

if __name__ == "__main__":
    main()
