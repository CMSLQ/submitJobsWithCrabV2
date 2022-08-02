#!/usr/bin/env python2
from optparse import OptionParser
from prettytable import PrettyTable
import sys
import collections
import re

try:
    import CRABClient
    from dbs.apis.dbsClient import DbsApi
    from dbs.exceptions.dbsClientException import dbsClientException
except ImportError:
    print
    print "ERROR: Could not load dbs APIs.  Please source the crab3 setup:"
    # print "source /cvmfs/cms.cern.ch/crab3/crab.sh"
    print("source /cvmfs/cms.cern.ch/common/crab-setup.sh")
    exit(-1)


def GetDatasetQueryString(primaryDataset):
    datasetQuery = "/" + primaryDataset + "/"
    secondary = acqEra if acqEra is not None else processedDatasetName
    datasetQuery += secondary + "*/"
    if "Run20" in secondary:
        datasetQuery += "NANOAOD"
    else:
        datasetQuery += "NANOAODSIM"
    return datasetQuery


def GetPrimaryDataset(dataset):
    if "/" in dataset:
        primaryDataset = dataset.split("/")[1]
    else:
        primaryDataset = dataset
    return primaryDataset


def GetSummary(dataset, acqEra):
    apiQueries = 0
    try:
        api = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader/')
        details = api.listDatasetArray(dataset=dataset, detail=True)
        apiQueries += 1
        listToReturn = []
        for datasetDict in details:
            if acqEra is not None and re.search(acqEra.replace("*", ".*"), datasetDict["acquisition_era_name"]) is None:
                continue
            # if processedDatasetName is not None and datasetDict["processed_ds_name"] != processedDatasetName:
            if processedDatasetName is not None and re.search(processedDatasetName.replace("*", ".*"), datasetDict["processed_ds_name"]) is None:
                print "DID NOT FIND", processedDatasetName, " in", datasetDict["processed_ds_name"], "for dataset:", datasetDict["dataset"]
                continue
            datasetName = datasetDict["dataset"]
            validity = datasetDict["dataset_access_type"]
            if validity != "VALID":
                # invalid matching datasets aren't necessarily a problem (e.g., if there are multiple matching datasets)
                errorMessages.append("WARN: ignored matching dataset: '"+datasetName+"' which has non-VALID dataset_access_type='"+validity+"'")
                if verbose:
                    print errorMessages[-1]
                continue
            if verbose:
                print "Found dataset:", datasetName, "; dataset_access_type=", validity
                # print api.listFileSummaries(dataset=datasetName, validFileOnly=1)
            fileSummaries = api.listFileSummaries(dataset=datasetName, validFileOnly=1)
            apiQueries += 1
            if len(fileSummaries) <= 0:
                errorMessages.append("ERROR: matching valid dataset: '"+datasetName+"' has empty fileSummaries:\n"+"\n".join(fileSummaries))
                if verbose:
                    print errorMessages[-1]
                continue
            datasetDict.update(fileSummaries[0])
            listToReturn.append(datasetDict)
    except dbsClientException, ex:
        print "Caught API Exception %s: %s " % (ex.name, ex)
        exit(-1)
    return listToReturn, apiQueries


def CheckDataset(matchingDataset):
    matchName = matchingDataset["dataset"]
    numEvents = matchingDataset["num_event"]
    if numEvents < minNumEvents:  # arbitrary
        errorMessages.append("ERROR: matching dataset:", matchName, "has only", numEvents, ", less than", minNumEvents, " required!")
        if verbose:
            print errorMessages[-1]
        return False
    if verbose:
        print "INFO: found matching dataset:", matchName, "with valid status and", numEvents, "events."
    return True


def QueryPrimaryDataset(primaryDataset):
    matchingDatasetDictList = []
    if verbose:
        print
        print "INFO: Examining primary dataset:", primaryDataset
        print "INFO: Find matching NANOAOD dataset in acquisition era", acqEra
    datasetQuery = GetDatasetQueryString(primaryDataset)
    summary, apiQueries = GetSummary(datasetQuery, acqEra)
    if verbose:
        print summary
    if len(summary) <= 0:
        errorMessages.append("ERROR: could not find a matching dataset for query: "+datasetQuery)
        if verbose:
            print errorMessages[-1]
        return {}, apiQueries
    # skips if multiple dataset matches
    # if len(summary) > 1:
    #     errorMessages.append("ERROR: found multiple matching datasets for primary dataset '"+primaryDataset+"':")
    #     for myDict in summary:
    #         errorMessages.append("\t"+myDict["dataset"])
    #     if verbose:
    #         print "ERROR: found multiple matching datasets:"
    #         for myDict in summary:
    #             print "\t"+myDict["dataset"]
    #     continue
    for dataset in summary:
        if(CheckDataset(dataset)):
            matchingDatasetDictList.append(dataset)
    return matchingDatasetDictList, apiQueries


####################################################################################################
# Configurables
####################################################################################################
minNumEvents = 20000
unitsPerJobMC = 2
unitsPerJobData = 500


parser = OptionParser()

parser.add_option(
    "-d",
    "--datasets",
    dest="datasets",
    help="datasets to check (comma-separated list)",
    metavar="DATASETS",
    default=None,
)
parser.add_option(
    "-f",
    "--file",
    dest="filename",
    help="filename containing datasets to check",
    metavar="FILENAME",
    default=None,
)
parser.add_option(
    "-e",
    "--era",
    dest="era",
    help="acquisition era of dataset (e.g., RunIISummer16NanoAODv6)",
    metavar="ERA",
    default=None,
)
parser.add_option(
    "-p",
    "--processedDatasetName",
    dest="processedDatasetName",
    help="processed dataset name (e.g., Run2018D-02Apr2020-v1)",
    metavar="PROCESSEDDATASETNAME",
    default=None,
)
parser.add_option(
    "-v",
    "--verbose",
    dest="verbose",
    help="increase log verbosity",
    metavar="VERBOSE",
    default=False,
    action="store_true",
)
parser.add_option(
    "-o",
    "--outputFile",
    dest="outputFile",
    help="filename of output datasetlist",
    metavar="OUTPUTFILE",
    default=None,
)
parser.add_option(
    "-b",
    "--bareList",
    dest="bareList",
    help="print dataset name only in output file",
    metavar="BARELIST",
    default=False,
    action="store_true",
)

(options, args) = parser.parse_args()

if options.datasets is None and options.filename is None:
    print "ERROR: must specify dataset with -d or --dataset or filename with -f or --file"
    print
    parser.print_help()
    exit(-1)
if options.era is None and options.processedDatasetName is None:
    print "ERROR: must specify an acquisition era and/or a processedDatasetName"
    print
    parser.print_help()
    exit(-1)
acqEra = options.era
processedDatasetName = options.processedDatasetName
verbose = options.verbose

if options.datasets is not None:
    datasetpath = options.datasets
    # print datasetpath
    if ',' in datasetpath:
        datasetList = datasetpath.split(',')
    else:
        datasetList = [datasetpath]
else:
    datasetList = list()
    with open(options.filename, "r") as theFile:
        for line in theFile:
            split = line.split()
            if len(split) <= 0:
                continue
            if "#" in split[0]:  # skip comments
                continue
            dataset = split[0]
            datasetList.append(dataset)

# make list of primary and unique primary datasets
primaryDatasets = list()
for fullDataset in datasetList:
    primaryDatasets.append(GetPrimaryDataset(fullDataset))
primaryDatasetsUnique = collections.OrderedDict.fromkeys(primaryDatasets)

matchingDatasets = []
errorMessages = []
totalApiQueries = 0

# find the matching datasets
print "INFO: Querying DBS for matches with", len(primaryDatasetsUnique.keys()), "unique primary dataset(s)...",
sys.stdout.flush()
for dataset in primaryDatasetsUnique.keys():
    returnedDatasets, apiQueries = QueryPrimaryDataset(dataset)
    matchingDatasets.extend(returnedDatasets)
    totalApiQueries += apiQueries
print "\bDone."
print "INFO: Total DBS API queries:", totalApiQueries
print

# write updated dataset list file
if options.outputFile is not None:
    with open(options.outputFile, "w") as theFile:
        for datasetDict in matchingDatasets:
            datasetName = datasetDict["dataset"]
            theFile.write(datasetName)
            if not options.bareList:
                if "Run20" in datasetName:
                    theFile.write("  -1  "+str(unitsPerJobData)+"\n")
                else:
                    theFile.write("  -1  "+str(unitsPerJobMC)+"\n")
            else:
                theFile.write("\n")
    print "wrote", len(matchingDatasets), "matching datasets to", options.outputFile

# print summary table for a few datasets
columnNames = ["Dataset name", "Events"]
t = PrettyTable(columnNames)
t.float_format = "4.3"
t.align["Dataset name"] = "l"
t.align["Events"] = "r"
print
for i, datasetDict in enumerate(matchingDatasets):
    if i < 10 or i > len(matchingDatasets)-10:
        t.add_row([datasetDict["dataset"], datasetDict["num_event"]])
    if i == 10:
        t.add_row(["...", "..."])
    i += 1
print t

# print any error messages
if len(errorMessages):
    for msg in errorMessages:
        print msg
    exit(-2)
