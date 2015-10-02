#!/usr/bin/env python

import subprocess
import os
import sys
import string
from optparse import OptionParser
import re
from datetime import datetime
import shutil
from multiprocessing import Process,Queue
try:
  from CRABClient.UserUtilities import config, getUsernameFromSiteDB
except ImportError:
  print
  print 'ERROR: Could not load CRABClient.UserUtilities.  Please source the crab3 setup:'
  print 'source /cvmfs/cms.cern.ch/crab3/crab.sh'
  exit(-1)
# now we should be able to import all the crab stuff
from CRABAPI.RawCommand import crabCommand
from httplib import HTTPException


# this prints a bunch of ugly stuff. just check to make sure user has sourced the crab setup first, as above
# first setup the crab stuff by "sourcing" the crab3 setup script if needed
# NB: env only prints exported variables.
# use 'set -a && source [script] && env' to export all vars
#if not 'crab3' in sys.path:
#  command = ['bash', '-c', 'set -a && source /cvmfs/cms.cern.ch/crab3/crab.sh && env']
#  proc = subprocess.Popen(command, stdout = subprocess.PIPE)
#  for line in proc.stdout:
#    (key, _, value) = line.partition("=")
#    os.environ[key] = value.strip('\n') # without this, things get messed up
#    # if it's the python path, update the sys.path
#    if key=='PYTHONPATH':
#      valueSplit = value.split(':')
#      for v in valueSplit:
#        sys.path.append(v)
#  proc.communicate()
#  newSysPath = sys.path



def crabSubmit(config):
    try:
        crabCommand('submit', config = config)
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
  elif options.cmsswCfg is None:
    error = True

  if error:
    print 'You are missing one or more required options: d, i, c'
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
                  help="EOS directory (start with /store...) to store files (used for Data.outLFNDirBase); otherwise EXO LJ group dir used with userName",
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

# time: YYYYMMDD_HHMMSS
date = datetime.now()
#dateString = date.strftime("%Y%m%d_%H%M%S")
# I like this better, but does it break anything?
dateString = date.strftime("%Y%b%d_%H%M%S")

# find tag name if not given
if options.tagName==None:
  print 'no tagname given; will ask git for the Leptoquarks/RootTupleMakerV2 tag'
  rootTupleMakerDir = os.getenv('CMSSW_BASE')+'/src/Leptoquarks/RootTupleMakerV2'
  proc = subprocess.Popen('revparse=`git rev-parse HEAD` && git name-rev --tags --name-only $revparse',stderr=subprocess.PIPE,stdout=subprocess.PIPE,shell=True,cwd=rootTupleMakerDir,env=dict())
  out,err = proc.communicate()
  if len(err) > 0:
    print 'something went wrong trying to get the git tag:',err
    print 'please specify tagname manually with -v'
    exit(-1)
  else:
    options.tagName=out.strip()
    print 'using tagname "'+options.tagName+'"'

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

# check if we have a proxy
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

# setup general crab settings
# from https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRABClientLibraryAPI
#TODO: this will work for MC. Need to update to run over data.
# notes on how the output will be stored: see https://twiki.cern.ch/twiki/bin/view/CMSPublic/Crab3DataHandling
#  <lfn-prefix>/<primary-dataset>/<publication-name>/<time-stamp>/<counter>[/log]/<file-name> 
#   LFNDirBase /                 / publishDataName  / stuff automatically done   / from outputFile defined below
config = config()
config.General.requestName   = topDirName # overridden per dataset (= tagName_dateString)
config.General.transferOutputs = True
config.General.transferLogs = False
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
# no publishing
config.Data.publication = False
config.Data.publishDataName = 'LQRootTuple'
config.Data.outLFNDirBase = '/store/group/phys_exotica/leptonsPlusJets/RootNtuple/RunII/%s/' % (getUsernameFromSiteDB()) + topDirName + '/'
#config.Data.outLFNDirBase = '/store/user/%s/' % (getUsernameFromSiteDB()) + topDirName + '/'
if options.eosDir is not None:
  # split of /eos/cms if it is there
  if options.eosDir.startswith('/eos/cms'):
    options.eosDir = options.eosDir.split('/eos/cms')[-1]
  if not options.eosDir.startswith('/store'):
    print 'eosDir must start with /eos/cms/store or /store and you specified:',options.eosDir
    print 'quit'
    exit(-1)
  outputLFN=options.eosDir
  if not getUsernameFromSiteDB() in outputLFN:
    outputLFN.rstrip('/')
    config.Data.outLFNDirBase = outputLFN+'/%s/' % (getUsernameFromSiteDB()) + topDirName + '/'
  else:
    config.Data.outLFNDirBase = outputLFN
print 'Using outLFNDirBase:',config.Data.outLFNDirBase
config.Site.storageSite = 'T2_CH_CERN'

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
    nUnitsPerJob = int(split[2])
    #print 'dataset=',dataset
    datasetName = dataset[1:len(dataset)].replace('/','__')
    #print 'datasetName:',datasetName
    thisWorkDir = workDir+'/'+datasetName
    #print 'make dir:',thisWorkDir
    makeDirAndCheck(thisWorkDir)
    outputFile = dataset[1:dataset.find('_Tune')]
    #print 'outputFile:',outputFile
    #TODO FIXME: handle the DiLept ext1 vs non ext case specially?
    storagePath=config.Data.outLFNDirBase+dataset+'/'+config.Data.publishDataName+'/'+'YYMMDD_hhmmss/0000/'+outputFile+'_999.root'
    #print 'will store (example):',storagePath
    #print '\twhich has length:',len(storagePath)
    if len(storagePath) > 255:
      print
      print 'we might have a problem with output path lengths too long (if we want to run crab over these).'
      print 'example output will look like:'
      print storagePath
      print 'which has length:',len(storagePath)
      print 'cowardly refusing to submit the jobs; exiting'
      exit(-2)

    if not os.path.isfile(options.cmsswCfg):
      # try relative path
      relPath = os.getenv('CMSSW_BASE')+'/src/'+options.cmsswCfg
      if os.path.isfile(relPath):
        options.cmsswCfg = relPath
      else:
        print 'cannot find CMSSW cfg:',options.cmsswCfg,'; also looked for:',relPath
        print 'quit'
        exit(-1)

    with open(options.cmsswCfg,'r') as config_file:
      config_txt = config_file.read()
    newCmsswConfig = cfgFilesDir+'/'+datasetName+'_cmssw_cfg.py'
    print 'creating',newCmsswConfig,'...'
    
    # substitute the output filename at the end
    config_txt += '\nprocess.TFileService.fileName = "'+outputFile+'.root"\n'
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
    q = Queue()
    p = Process(target=crabSubmit, args=(config,))
    p.start()
    p.join()
    if q.get()==-1:
      exit(-1)
    

print 'Done!' 
exit(0)



