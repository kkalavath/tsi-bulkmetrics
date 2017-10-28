import json
import pandas as pd
import time
import requests
import argparse
import csv
import threading

METRICAPI = "https://api.truesight.bmc.com/v1/metrics"
MEASUREMENTSAPI = "https://api.truesight.bmc.com/v1/measurements"
EVENTAPI = "https://api.truesight.bmc.com/v1/events"
BATCH = 100
exitFlag = 0
NAP = 0.5


# with open('param.json') as json_data:
#     parms = json.load(json_data)

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

class myThread (threading.Thread):
   def __init__(self, threadID, name, data, email, apikey):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
      self.data = data
      self.email = email
      self.apikey = apikey
      
   def run(self):
       print ("Starting " + self.name)
       worker(self.name,self.data,self.email,self.apikey)
      

def	sendrequest(data,email,apikey):
	r = requests.post(MEASUREMENTSAPI, data=json.dumps(data), headers={'Content-type': 'application/json'}, auth=(email, apikey))
	return r

	  
def worker(tdname,data,email,apikey):
    measuresbatch = []
    measures = []
    tdstart = str(time.ctime(time.time()))
    #print(data)
    batchcount = 1
    numbatches = 0
    count = 1
    
    if(len(data) > BATCH):
        for item in data:
            measures.append(item)
            #print(count)
            if count == len(data):
                numbatches += 1
                #print("Creating final batch..%s"  % (str(numbatches)))
                measuresbatch.append(measures)
            elif batchcount < BATCH:
                batchcount = batchcount + 1
            else:
                batchcount = 1
                numbatches += 1
                #print("Creating batch...%s"  % (str(numbatches)))
                measuresbatch.append(measures)
                measures = []
            count = count + 1
    else:
        measuresbatch.append(data)
        numbatches += 1
            
    #print("Sending as %s chunk/s..." %(numbatches))        

    for item in measuresbatch:
        
        try:
            r = sendrequest(item,email,apikey)
            if(numbatches > 0):
                time.sleep(NAP)
        except requests.exceptions.RequestException as e:
            print(item)
            print(e)
            exit(1)
        else:
            #print(json.dumps(data,indent=4))
            #print("Response: %s - %s" % (r.status_code, r.reason))
            if( r.status_code != 200):
                print("Response: %s - %s" % (r.status_code, r.reason))
                print(item)
        
    tdend = str(time.ctime(time.time()))
    print(" %s complete -> sent %s chunks: %s - %s" % (tdname, numbatches, tdstart, tdend))
    
	
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

    csize = 10 ** 5
    # Open the measures Excel file
    for chunk in pd.read_csv(args.measuresfile, chunksize=csize):
        # Iterate and create tuples
        data = []
        for index, row in chunk.iterrows():
            tup = (row[args.tscol], row[args.valcol],row[args.source], row[args.metricname])
            data.append(tup)
        # Sort on the source if not already
        #dchunk = sorted(data, key=lambda tup: tup[2])
        #Create the payload
        payload = create_batch(data, args)
    return 

def create_batch(data,args):

    measures = []
    measuresbatch = []
    measurecount = 0 # loader handles header
    batchcount = 1
    numbatches = 0
    threads = []
    threadId = 1
    prevsource = ""
    currsource = ""
    samesource = False
    source = ""

    print("Total number of measures: %s" % len(data))
    # Iterate through measure data
    for item in data:

        #print("Measure num: %s of %s" % (measurecount,len(data)))
        #print(item)
        
        #Check if header
        if(item[0] == args.tscol):
            print(item)
            measurecount += 1
            continue

        currsource = item[2] + "_" + item[3]
        if(prevsource == ""):
            samesource = True
        elif (prevsource == currsource):
            samesource = True
        else:
            samesource = False

        #Check for Value corruption
        itemVal = item[1]
        if(itemVal == "0n"):
            print("Updated On")
            itemVal = 0

        #Append appid to source
        source = args.appid + "_" + item[2]
        
        # Create JSON for each measurement
        measure = [
            source,  # source
            item[3],  # metric name, identifier in Pulse.
            float(itemVal),  # measure
            int(item[0]),  # timestamp
            {"app_id": args.appid}  # metadata
        ]

        if(samesource):
            # Append measurement JSON to list
            measures.append(measure)

            # Determine batch info/position and append measures list object to batch after batch limit is reached
            if measurecount == len(data):
                numbatches += 1
                #print("Creating final batch..%s"  % (str(numbatches)))
                measuresbatch.append(measures)
        else:
            numbatches += 1
            #print("Creating batch....%s"  % (str(numbatches)))
            measuresbatch.append(measures)
            measures = []
            # Append measurement JSON with new source to list
            measures.append(measure)
            if measurecount == len(data):
                numbatches += 1
                #print("Creating final batch....%s"  % (str(numbatches)))
                measuresbatch.append(measures)

        measurecount = measurecount + 1
        prevsource = currsource

    print("Sending %s batches..." %(numbatches))

    for chunk in measuresbatch:
        # For each chunk of data, POST to the API
        #Create new threads
        tname = "Thread" + str(threadId);
        t = myThread(threadId, tname, chunk, args.email, args.apikey)
        threads.append(t)
        t.start()
        #print("Taking a break for 1 second...")
        #time.sleep(1)
        threadId += 1

    print("Waiting for other threads...")

    for t in threads:
        t.join()
    
    return measuresbatch


def send_measures(args):

    tds=str(time.ctime(time.time()))
    print("Main thread started @ ", tds)
   
    # Parse the measure data
    data = parse_data(args)
    
    tdp=str(time.ctime(time.time()))
    print("Parsed data completed @ ", tdp)
    # Create the payload
    #payload = create_batch(data, args)

    tde=str(time.ctime(time.time()))
    print(" Main thread complete: %s - %s" % (tds, tde))

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
