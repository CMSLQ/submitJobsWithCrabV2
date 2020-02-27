#!/usr/bin/env python

import subprocess
import os
import sys
import string
from optparse import OptionParser
from datetime import datetime
import shutil
from multiprocessing import Process, Queue
try:
    from CRABClient.UserUtilities import config, getUsernameFromSiteDB
except ImportError:
    print
    print 'ERROR: Could not load CRABClient.UserUtilities.  Please source the crab3 setup:'
    print 'source /cvmfs/cms.cern.ch/crab3/crab.sh'
    exit(-1)
try:
    cmsswBaseDir = os.environ['CMSSW_BASE']
except KeyError as e:
    print('Could not find CMSSW_BASE env var; have you set up the CMSSW environment?')
    exit(-1)
# now we should be able to import all the crab stuff
from CRABAPI.RawCommand import crabCommand
from httplib import HTTPException
import utils


def crabSubmit(config):
    try:
        crabCommand('submit', config = config)
        #crabCommand('submit', 'dryrun', config = config)
    except HTTPException, hte:
        print '-----> there was a problem. see below.'
        print hte.headers
        print 'quit here'
        q.put(-1)
    q.put(0)
    
def validateOptions(options):
    error = False
    if options.localStorageDir is None:
        error = True
    elif options.inputList is None:
        error = True
    if error:
        print 'You are missing one or more required options: d, i'
        parser.print_help()
        exit(-1)
    if options.prevJsonFile is not None and options.jsonFile is None:
        print 'It does not make sense to specify a previously used/analyzed JSON file without specifying a new JSON file, since with this option specified, the difference between the new and old JSON is taken as the lumi mask.'
        exit(-1)


def makeDirAndCheck(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)
    else:
        # in practice, this doesn't happen because of the seconds in the name, but always good to check
        print 'ERROR: directory %s already exists. Not going to overwrite it.' % dir
        exit(-2)


def CheckProxy():
    proc = subprocess.Popen(['voms-proxy-info','--all'],stderr=subprocess.PIPE,stdout=subprocess.PIPE)
    out,err = proc.communicate()
    #print 'output----->',output
    #print 'err------>',err
    if 'Proxy not found' in err or 'timeleft  : 00:00:00' in out:
        # get a proxy
        print 'you have no valid proxy; let\'s get one via voms-proxy-init:'
        # this will suppress the stderr; maybe that's not so good, but I get some error messages at the moment
        #with open(os.devnull, "w") as f:
        #  proc2 = subprocess.call(['voms-proxy-init','--voms','cms','--valid','168:00'],stderr=f)
        proc2 = subprocess.call(['voms-proxy-init','--voms','cms','--valid','168:00'])
 

def checkStoragePath(storagePath):
    print 'will store (example):',storagePath
    #print '\twhich has length:',len(storagePath)
    if len(storagePath) > 255:
      print
      print 'we might have a problem with output path lengths too long (if we want to run crab over these).'
      print 'example output will look like:'
      print storagePath
      print 'which has length:',len(storagePath)
      print 'cowardly refusing to submit the jobs; exiting'
      exit(-3)
    #else:
    #  print
    #  print 'will use storage path like:',storagePath


# to feed additional files into the crab sandbox if needed
additionalInputFiles = []
#rootTupleTestDir = os.getenv('CMSSW_BASE')+'/src/Leptoquarks/RootTupleMakerV2/test/'
# just feed both in, even though we only need one at a time
#additionalInputFiles.append(rootTupleTestDir+'Summer16_23Sep2016V4_MC.db')
#additionalInputFiles.append(rootTupleTestDir+'Summer16_23Sep2016AllV4_DATA.db')
additionalInputFiles.extend(['keepAndDrop.txt','utils.py','doSkim_stockNanoV5.py',
    cmsswBaseDir+'/src/PhysicsTools/NanoAODTools/scripts/haddnano.py'] #hadd nano will not be needed once nano tools are in cmssw
)

##############################################################
# RUN
##############################################################
#---Option Parser
#--- TODO: WHY PARSER DOES NOT WORK IN CMSSW ENVIRONMENT? ---#
usage = "Usage: %prog [options] "
#XXX TODO FIX/UPDATE THIS MESSAGE
usage+="\nSee https://twiki.cern.ch/twiki/bin/view/CMS/ExoticaLeptoquarkShiftMakeRootTuplesV22012 for more details "
usage+="\nExample1 (NORMAL MODE): %prog -d `pwd`/RootNtuple -i inputList.txt"
usage+="\nExample2 (NORMAL MODE + RUN SELECTION): %prog -d `pwd`/RootNtuple -i inputList.txt -r 132440-200000 "
usage+="\nExample3 (JSON MODE): %prog -d `pwd`/RootNtuple -i inputList.txt -j [JSON.txt or URL, https://cms-service-dqm.web.cern.ch/cms-service-dqm/CAF/certification/Collisions12/8TeV/Prompt/Cert_190456-208686_8TeV_PromptReco_Collisions12_JSON.txt]"
usage+="\nExample4 (PREV JSON MODE): %prog -d `pwd`/RootNtuple -i inputList.txt -j [JSON.txt or URL, https://cms-service-dqm.web.cern.ch/cms-service-dqm/CAF/certification/Collisions12/8TeV/Prompt/Cert_190456-208686_8TeV_PromptReco_Collisions12_JSON.txt] -p [lumiSummary.json from crab report from previous processing of same dataset]"

parser = OptionParser(usage=usage)

parser.add_option("-d", "--localStorageDir", dest="localStorageDir",
                  help="the directory localStorageDir is where the local job info is kept",
                  metavar="INDIR")

parser.add_option("-v", "--tagName", dest="tagName",
                  help="tagName of postproc package",
                  metavar="TAGNAME",
                  default="")

parser.add_option("-i", "--inputList", dest="inputList",
                  help="list of all datasets to be used (full path required)",
                  metavar="LIST")

parser.add_option("-e", "--eosDir", dest="eosDir",
                  help="EOS directory (start with /store...) to store files (used for Data.outLFNDirBase); otherwise EXO LJ group dir used with userName",
                  metavar="EOSDIR")

parser.add_option("-j", "--json", dest="jsonFile",
                  help="JSON file with selected lumi sections",
                  metavar="JSONFILE")

parser.add_option("-r", "--run range", dest="runRange",
                  help="selected run range",
                  metavar="RUNRANGE")

parser.add_option("-p", "--previousJSON json", dest="prevJsonFile",
                  help="previous lumiSummary.json from crab",
                  metavar="PREVJSON")

parser.add_option("-s", "--site siteName", dest="storageSite",
                  help="storage site",
                  metavar="STORAGESITE",
                  default="T2_CH_CERN")

(options, args) = parser.parse_args()

# validate options
validateOptions(options)

# time: YYYYMMDD_HHMMSS
date = datetime.now()
#dateString = date.strftime("%Y%m%d_%H%M%S")
# I like this better, but does it break anything?
dateString = date.strftime("%Y%b%d_%H%M%S")

if options.tagName:
    topDirName = 'lqNanoPostProc_'+options.tagName+'_'+dateString
else:
    topDirName = 'lqNanoPostProc_'+dateString
productionDir = options.localStorageDir+'/'+topDirName
cfgFilesDir = productionDir+'/cfgfiles'
outputDir = productionDir+'/output'
workDir = productionDir+'/workdir'
localDirs = [productionDir,cfgFilesDir,outputDir,workDir]
print 'Making local directories:'
for dir in localDirs:
  print '\t',dir
  makeDirAndCheck(dir)
print

localInputListFile = productionDir+'/inputList.txt'
shutil.copy2(options.inputList,localInputListFile)

# check if we have a proxy
CheckProxy()

# setup general crab settings
# from https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRABClientLibraryAPI
#TODO: this will work for MC. Need to update to run over data.
# notes on how the output will be stored: see https://twiki.cern.ch/twiki/bin/view/CMSPublic/Crab3DataHandling
#  <lfn-prefix>/<primary-dataset>/<publication-name>/<time-stamp>/<counter>[/log]/<file-name> 
#   LFNDirBase /                 / datasetTagName   / stuff automatically done   / from outputFile defined below
config = config()
config.General.requestName   = topDirName # overridden per dataset
config.General.transferOutputs = True
config.General.transferLogs = False
# We want to put all the CRAB project directories from the tasks we submit here into one common directory.
# That's why we need to set this parameter (here or above in the configuration file, it does not matter, we will not overwrite it).
config.General.workArea = productionDir
#
config.JobType.pluginName  = 'Analysis'
#config.JobType.maxMemoryMB = 3000
# this will make sure jobs only run on sites which host the data.
# See: https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#What_is_glideinWms_Overflow_and
# postprocessing jobs take forever (and can exceed max wall clock time) otherwise
config.Debug.extraJDL = ['+CMS_ALLOW_OVERFLOW=False']
# feed in any additional input files
if len(additionalInputFiles) > 0:
    config.JobType.inputFiles = []
    config.JobType.inputFiles.extend(additionalInputFiles)
config.JobType.psetName  = 'PSet.py'
config.JobType.scriptExe = 'crab_script.sh'
config.JobType.sendPythonFolder	 = True
config.Data.inputDataset = '' # overridden per dataset
config.Data.inputDBS = 'global'
config.Data.splitting = 'Automatic' # below this is set to LumiBased for data, FileBased for MC
config.Data.totalUnits = -1 # overridden per dataset, but doesn't matter for Automatic splitting
# no publishing
config.Data.publication = False
config.Data.outputDatasetTag = 'LQ' #overridden for data
#This is for EXO group space
if options.tagName:
    config.Data.outLFNDirBase = '/store/group/phys_exotica/leptonsPlusJets/LQ/%s/nanoPostProc' % (getUsernameFromSiteDB()) + options.tagName + '/'
else:
    config.Data.outLFNDirBase = '/store/group/phys_exotica/leptonsPlusJets/LQ/%s/nanoPostProc' % (getUsernameFromSiteDB()) + '/'
#This is for Higgs group space
#config.Data.outLFNDirBase = '/store/group/phys_higgs/HiggsExo/HH_bbZZ_bbllqq/%s/' % (getUsernameFromSiteDB()) + options.tagName + '/'
#This is for personal user space (beware quotas)
#config.Data.outLFNDirBase = '/store/user/%s/' % (getUsernameFromSiteDB()) + topDirName + '/'
if options.eosDir is not None:
  # split of /eos/cms if it is there
  if options.eosDir.startswith('/eos/cms'):
    options.eosDir = options.eosDir.split('/eos/cms')[-1]
  # require /store unless it's CERNBOX
  if options.storageSite!='T2_CH_CERNBOX' and not options.eosDir.startswith('/store'):
    print 'eosDir must start with /eos/cms/store or /store and you specified:',options.eosDir
    print 'quit'
    exit(-1)
  outputLFN=options.eosDir
  if not outputLFN[-1]=='/':
    outputLFN+='/'
  if options.tagName:
    outputLFN+=options.tagName+'/'
  if not getUsernameFromSiteDB() in outputLFN:
    outputLFN.rstrip('/')
    #config.Data.outLFNDirBase = outputLFN+'/%s/' % (getUsernameFromSiteDB()) + topDirName + '/'
    # make the LFN shorter, and in any case, the timestamp is put in by crab
    if options.tagName:
      config.Data.outLFNDirBase = outputLFN+'/%s/' % (getUsernameFromSiteDB()) + options.tagName + '/'
    else:
      config.Data.outLFNDirBase = outputLFN+'/%s/' % (getUsernameFromSiteDB()) + '/'
  else:
    config.Data.outLFNDirBase = outputLFN
print 'Using outLFNDirBase:',config.Data.outLFNDirBase
config.Site.storageSite = options.storageSite

# look at the input list
# use DAS to find the dataset names.
# Example:
#   das_client.py --query="dataset=/LQToUE_M-*_BetaOne_TuneCUETP8M1_13TeV-pythia8/*/MINIAODSIM"
with open(localInputListFile, 'r') as f:
  for line in f:
    split = line.split()
    if len(split) <= 0:
      continue
    if '#' in split[0]: # skip comments
      #print 'found comment:',line
      continue
    if len(split) < 3:
      print 'inputList line is not properly formatted:',line
      exit(-3)
    dataset = split[0]
    nUnits = int(split[1]) #also used for total lumis for data
    nUnitsPerJob = int(split[2]) # used for files/dataset for MC and LS per data

    datasetTag,datasetName,primaryDatasetName,secondaryDatasetName,isData = utils.GetOutputDatasetTagAndModifiedDatasetName(dataset)
    outputFile = utils.GetOutputFilename(dataset,not isData)
    config.Data.outputDatasetTag=datasetTag
    config.Data.inputDataset = dataset
    print
    print 'Consider dataset {0}'.format(dataset)

    if not isData:
      config.Data.splitting = 'FileBased'
    else:
      config.Data.splitting = 'LumiBased'
    
    # get era
    # see, for example: https://twiki.cern.ch/twiki/bin/viewauth/CMS/PdmVAnalysisSummaryTable
    # secondaryDatasetName looks like 'Run2015D-PromptReco-v3'
    if 'Summer16' in secondaryDatasetName or 'Run2016' in secondaryDatasetName:
      year=2016
    elif 'Fall17' in secondaryDatasetName or 'Run2017' in secondaryDatasetName:
      year=2017
    elif 'Autumn18' in secondaryDatasetName or 'Run2018' in secondaryDatasetName:
      year=2018
    else:
      print 'ERROR: could not determine year from secondaryDatasetName "{0}" from datasetName "{1}"'.format(secondaryDatasetName,datasetName)
      exit(-4)
    # get dataRun
    dataRun = 'X'
    if isData:
        dataRun = secondaryDatasetName[secondaryDatasetName.find('Run')+7:secondaryDatasetName.find('Run')+8]

    config.JobType.scriptArgs = ['dataset='+config.Data.inputDataset,'ismc='+str(not isData),'era='+str(year),'dataRun='+dataRun]
    config.JobType.outputFiles = [outputFile]
    config.Data.unitsPerJob = nUnitsPerJob

    thisWorkDir = workDir+'/'+datasetName
    storagePath=config.Data.outLFNDirBase+primaryDatasetName+'/'+config.Data.outputDatasetTag+'/'+'YYMMDD_hhmmss/0000/'+outputFile.replace('.root','_9999.root')
    #print 'make dir:',thisWorkDir
    makeDirAndCheck(thisWorkDir)
    checkStoragePath(storagePath)
    
    config.General.requestName = datasetName
    config.Data.totalUnits = nUnits
    # computing JSON mask
    if options.jsonFile is not None:
      if options.prevJsonFile is not None:
        print 'Using the subtraction between previous json and new json; WARNING: if lumis changed from good in previous to bad in new json, this will not remove them'
        from WMCore.DataStructs.LumiList import LumiList
        prevJsonLumiList = LumiList(url=options.prevJsonFile) if 'http:' in options.prevJsonFile else LumiList(filename=options.prevJsonFile)
        currentJsonLumiList = LumiList(url=options.jsonFile) if 'http:' in options.jsonFile else LumiList(filename=options.jsonFile)
        newLumiList = currentJsonLumiList - prevJsonLumiList
        newLumiList.writeJSON('newJSON_minus_oldJSON.json')
        config.Data.lumiMask = 'newJSON_minus_oldJSON.json'
      else:
        config.Data.lumiMask = options.jsonFile

    if options.runRange is not None:
      config.Data.runRange = runRange

    # and submit
    print config.JobType.scriptArgs
    print 'submit to crab. output from crab submit follows:'
    sys.stdout.write("\033[1;34m")
    #crabSubmit(config)
    # workaround for cmssw multiple-loading problem
    # submit in subprocess
    q = Queue()
    p = Process(target=crabSubmit, args=(config,))
    p.start()
    p.join()
    if q.get()==-1:
      exit(-1)
    sys.stdout.write("\033[0;0m")
    print 'Done with this dataset.'
    

print 'Done!' 
exit(0)



