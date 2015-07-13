#!/usr/bin/env python

import subprocess
import os
import sys
import string
from optparse import OptionParser
import re
from datetime import datetime
import shutil
from multiprocessing import Process
#from ROOT import *

# first setup the crab stuff by "sourcing" the crab3 setup script if needed
# NB: env only prints exported variables.
# use 'set -a && source [script] && env' to export all vars
if not 'crab3' in sys.path:
  command = ['bash', '-c', 'set -a && source /cvmfs/cms.cern.ch/crab3/crab.sh && env']
  proc = subprocess.Popen(command, stdout = subprocess.PIPE)
  for line in proc.stdout:
    (key, _, value) = line.partition("=")
    os.environ[key] = value.strip('\n') # without this, things get messed up
    # if it's the python path, update the sys.path
    if key=='PYTHONPATH':
      valueSplit = value.split(':')
      for v in valueSplit:
        sys.path.append(v)
  proc.communicate()
  newSysPath = sys.path

# now we should be able to import the crab stuff
from CRABClient.UserUtilities import config, getUsernameFromSiteDB
from CRABAPI.RawCommand import crabCommand
from httplib import HTTPException


def crabSubmit(config):
    try:
        crabCommand('submit', config = config)
    except HTTPException, hte:
        print hte.headers
    
def validateOptions(options):
  error = False
  if options.localStorageDir is None:
    error = True
  elif options.tagName is None:
    error = True
  elif options.inputList is None:
    error = True
  elif options.cmsswCfg is None:
    error = True
  #elif options.userName is None:
  #  if options.eosDir is None:
  #    error = True

  if error:
    print 'You are missing one or more required options: d, v, i, c, n'
    parser.print_help()
    exit(-1)


def makeDirAndCheck(dir):
  if not os.path.exists(dir):
    os.makedirs(dir)
  else:
    # in practice, this doesn't happen because of the seconds in the name, but always good to check
    print 'ERROR: directory %s already exists. Not going to overwrite it.' % dir
    exit(-2)


##############################################################
# RUN
##############################################################
#---Option Parser
#--- TODO: WHY PARSER DOES NOT WORK IN CMSSW ENVIRONMENT? ---#
usage = "Usage: %prog [options] "
#XXX TODO FIX/UPDATE THIS MESSAGE
usage+="\nSee https://twiki.cern.ch/twiki/bin/view/CMS/ExoticaLeptoquarkShiftMakeRootTuplesV22012 for more details "
usage+="\nExample1 (NORMAL MODE): %prog -d `pwd`/RootNtuple -v V00-03-07-DATA-xxxxxx-yyyyyy -i inputList.txt -c rootTupleMaker_CRAB_DATA_2012_53X_cfg.py "
usage+="\nExample2 (NORMAL MODE + RUN SELECTION): %prog -d `pwd`/RootNtuple -v V00-03-07-DATA-xxxxxx-yyyyyy -i inputList.txt -c rootTupleMaker_CRAB_DATA_2012_53X_cfg.py -r 132440-200000 "
usage+="\nExample3 (JSON MODE): %prog -d `pwd`/RootNtuple -v V00-03-07-DATA-xxxxxx-yyyyyy -i inputList.txt -c rootTupleMaker_CRAB_DATA_2012_53X_cfg.py -j [JSON.txt or URL, https://cms-service-dqm.web.cern.ch/cms-service-dqm/CAF/certification/Collisions12/8TeV/Prompt/Cert_190456-208686_8TeV_PromptReco_Collisions12_JSON.txt]"

parser = OptionParser(usage=usage)

parser.add_option("-d", "--localStorageDir", dest="localStorageDir",
                  help="the directory localStorageDir is where the local job info is kept",
                  metavar="INDIR")

parser.add_option("-v", "--tagName", dest="tagName",
                  help="tagName of RootNTupleMakerV2",
                  metavar="TAGNAME")

parser.add_option("-i", "--inputList", dest="inputList",
                  help="list of all datasets to be used (full path required)",
                  metavar="LIST")

parser.add_option("-c", "--cfg", dest="cmsswCfg",
                  help="CMSSW template cfg",
                  metavar="CMSSWCFG")

parser.add_option("-e", "--eosDir", dest="eosDir",
                  help="EOS directory (full path) to store files; otherwise EXO LJ group dir used with userName",
                  metavar="EOSDIR")

parser.add_option("-j", "--json", dest="jsonFile",
                  help="JSON file with selected lumi sections",
                  metavar="JSONFILE")

parser.add_option("-r", "--run range", dest="runRange",
                  help="selected run range",
                  metavar="RUNRANGE")

(options, args) = parser.parse_args()

# validate options
validateOptions(options)

# check if we have a proxy
proc = subprocess.Popen(['voms-proxy-info','--all'],stderr=subprocess.PIPE,stdout=subprocess.PIPE)
output,err = proc.communicate()
#print 'output----->',output
#print 'err------>',err
if 'Proxy not found' in err:
  # get a proxy
  print 'you have no proxy; let\'s get one via voms-proxy-init:'
  # this will suppress the stderr; maybe that's not so good, but I get some error messages at the moment
  #with open(os.devnull, "w") as f:
  #  proc2 = subprocess.call(['voms-proxy-init','--voms','cms','--valid','168:00'],stderr=f)
  proc2 = subprocess.call(['voms-proxy-init','--voms','cms','--valid','168:00'])

# time: YYYYMMDD_HHMMSS
date = datetime.now()
#dateString = date.strftime("%Y%m%d_%H%M%S")
# I like this better, but does it break anything?
dateString = date.strftime("%Y%b%d_%H%M%S")

topDirName = options.tagName+'_'+dateString

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

# setup general crab settings
# FIXME need this fix for some reason. luckily this option exists, because the crab script is borked!
#os.environ['CRAB3_CACHE_FILE'] = str(os.path.expanduser('~')).rstrip('\n')+'/.crab3'
#print os.environ['CRAB3_CACHE_FILE']
# from https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRABClientLibraryAPI
#TODO: this will work for MC. Need to update to run over data.
# notes on how the output will be stored: see https://twiki.cern.ch/twiki/bin/view/CMSPublic/Crab3DataHandling
#  <lfn-prefix>/<primary-dataset>/<publication-name>/<time-stamp>/<counter>[/log]/<file-name> 
#   LFNDirBase /                 / requestName      / stuff automatically done   / from outputFile defined below
config = config()
config.General.requestName   = topDirName # overridden per dataset
config.General.transferOutputs = True
config.General.transferLogs = True
# We want to put all the CRAB project directories from the tasks we submit here into one common directory.
# That's why we need to set this parameter (here or above in the configuration file, it does not matter, we will not overwrite it).
config.General.workArea = productionDir
#
config.JobType.pluginName  = 'Analysis'
config.JobType.psetName    = '' # overridden per dataset
#
config.Data.inputDataset = '' # overridden per dataset
config.Data.inputDBS = 'global'
config.Data.splitting = 'FileBased' #LumiBased for data
config.Data.unitsPerJob = 1 # overridden per dataset
config.Data.totalUnits = -1 # overridden per dataset
config.Data.publication = False
#config.Data.publishDataName = 'GenSim-noPU-721p4-START72_V1'
# FIXME: Change to leptonsPlusJets group area at some point
#config.Data.outLFNDirBase = '/store/group/phys_exotica/leptonsPlusJets/'
config.Data.outLFNDirBase = '/store/user/%s/' % (getUsernameFromSiteDB()) + topDirName + '/'
#TODO add eosDir option
config.Site.storageSite = 'T2_CH_CERN'

print 'Using outLFNDirBase:',config.Data.outLFNDirBase
# look at the input list
# use DAS to find the dataset names.
# Example:
#   das_client.py --query="dataset=/LQToUE_M-*_BetaOne_TuneCUETP8M1_13TeV-pythia8/*/MINIAODSIM"
with open(localInputListFile, 'r') as f:
  for line in f:
    split = line.split()
    if '#' in split[0]: # skip comments
      #print 'found comment:',line
      continue
    if len(split) < 3:
      print 'inputList line is not properly formatted:',line
      exit(-3)
    dataset = split[0]
    nUnits = int(split[1]) #also used for total lumis for data
    nUnitsPerJob = int(split[2])
    #print 'dataset=',dataset
    datasetName = dataset[1:len(dataset)].replace('/','__')
    #print 'datasetName:',datasetName
    thisWorkDir = workDir+'/'+datasetName
    #print 'make dir:',thisWorkDir
    makeDirAndCheck(thisWorkDir)
    outputFile = datasetName+'.root'

    with open(options.cmsswCfg,'r') as config_file:
      config_txt = config_file.read()
    newCmsswConfig = cfgFilesDir+'/'+datasetName+'_cmssw_cfg.py'
    print 'creating',newCmsswConfig,'...'
    
    # substitute the output filename at the end
    config_txt += '\nprocess.TFileService.fileName = "'+outputFile+'"\n'
    with open(newCmsswConfig,'w') as cfgNew_file:
      cfgNew_file.write(config_txt)
   
    # now make the crab3 config
    config.General.requestName = datasetName
    config.JobType.psetName = newCmsswConfig
    config.Data.inputDataset = dataset 
    config.Data.unitsPerJob = nUnitsPerJob
    config.Data.totalUnits = nUnits
    if options.jsonFile is not None:
      config.Data.lumiMask = options.jsonFile
    if options.runRange is not None:
      config.Data.runRange = runRange
    # and submit
    print 'submit!'
    #crabSubmit(config)
    # workaround for cmssw multiple-loading problem
    # submit in subprocess
    p = Process(target=crabSubmit, args=(config,))
    p.start()
    p.join()
    

print 'Done!' 
exit(0)



