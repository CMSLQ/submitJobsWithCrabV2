from WMCore.Configuration import Configuration
from CRABClient.UserUtilities import config, getUsernameFromSiteDB
import os
import utils

try:
    cmsswBaseDir = os.environ['CMSSW_BASE']
except KeyError as e:
    print('Could not find CMSSW_BASE env var; have you set up the CMSSW environment?')
    exit(-1)

config = Configuration()

config.section_("General")
config.General.requestName = 'NanoPostDYJIncAMCNLO'
config.General.transferLogs=True

config.section_("Data")
config.Data.inputDataset = '/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer16NanoAODv6-PUMoriond17_Nano25Oct2019_102X_mcRun2_asymptotic_v7_ext2-v1/NANOAODSIM'
#config.Data.inputDBS = 'phys03'
config.Data.inputDBS = 'global'
config.Data.splitting = 'FileBased'
#config.Data.splitting = 'EventAwareLumiBased'
config.Data.unitsPerJob = 1
config.Data.totalUnits = -1
#config.Data.lumiMask = jsonFile
#config.Data.outLFNDirBase = '/store/group/phys_exotica/leptonsPlusJets/RootNtuple/RunII/%s/' % (getUsernameFromSiteDB())
config.Data.outLFNDirBase = '/store/group/phys_exotica/leptonsPlusJets/LQ/scooper/2016nanoPostProc/'
config.Data.publication = False
config.Data.outputDatasetTag = 'NanoPostDYJIncAMCNLO'

config.section_("JobType")
config.JobType.pluginName = 'Analysis'
config.JobType.psetName = 'PSet.py'
#config.JobType.pyCfgParams = ['dataset='+config.Data.inputDataset]
config.JobType.scriptExe = 'crab_script.sh'
config.JobType.scriptArgs = ['dataset='+config.Data.inputDataset]
config.JobType.inputFiles = ['keepAndDrop.txt','utils.py','doSkim_stockNanoV5.py',cmsswBaseDir+'/src/PhysicsTools/NanoAODTools/scripts/haddnano.py'] #hadd nano will not be needed once nano tools are in cmssw
config.JobType.outputFiles = [utils.GetOutputFilename(config.Data.inputDataset,True)]
config.JobType.sendPythonFolder	 = True

config.section_("Site")
config.Site.storageSite = "T2_CH_CERN"
#config.section_("User")
#config.User.voGroup = 'dcms'

