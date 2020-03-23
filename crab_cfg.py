from WMCore.Configuration import Configuration
import os
import utils

try:
    cmsswBaseDir = os.environ["CMSSW_BASE"]
except KeyError as e:
    print("Could not find CMSSW_BASE env var; have you set up the CMSSW environment?")
    exit(-1)

config = Configuration()

config.section_("General")
config.General.requestName = "testNanoPost"
config.General.transferLogs = False

config.section_("Data")
config.Data.inputDataset = "/TTJets_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIFall17NanoAODv6-PU2017_12Apr2018_Nano25Oct2019_new_pmx_102X_mc2017_realistic_v7-v1/NANOAODSIM"
# config.Data.inputDBS = 'phys03'
config.Data.inputDBS = "global"
config.Data.splitting = "FileBased"
# config.Data.splitting = 'EventAwareLumiBased'
config.Data.unitsPerJob = 1
config.Data.totalUnits = -1
# config.Data.lumiMask = jsonFile
# config.Data.outLFNDirBase = '/store/group/phys_exotica/leptonsPlusJets/RootNtuple/RunII/%s/' % (getUsernameFromSiteDB())
config.Data.outLFNDirBase = (
    "/store/group/phys_exotica/leptonsPlusJets/LQ/scooper/test2017nanoPostProc/"
)
config.Data.publication = False
config.Data.outputDatasetTag = "TTJetsIncAMCNLO"

config.section_("JobType")
config.JobType.pluginName = "Analysis"
config.JobType.psetName = "PSet.py"
# config.JobType.pyCfgParams = ['dataset='+config.Data.inputDataset]
config.JobType.scriptExe = "crab_script.sh"
config.JobType.scriptArgs = [
    "dataset=" + config.Data.inputDataset,
    "ismc=True",
    "era=2017",
    "dataRun=X",
]
config.JobType.inputFiles = [
    "keepAndDrop.txt",
    "utils.py",
    "doSkim_stockNanoV5.py",
    cmsswBaseDir + "/src/PhysicsTools/NanoAODTools/scripts/haddnano.py",
]  # hadd nano will not be needed once nano tools are in cmssw
config.JobType.outputFiles = [utils.GetOutputFilename(config.Data.inputDataset, True)]
config.JobType.sendPythonFolder = True

config.section_("Site")
config.Site.storageSite = "T2_CH_CERN"

# this will make sure jobs only run on sites which host the data.
# See: https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#What_is_glideinWms_Overflow_and
config.section_("Debug")
config.Debug.extraJDL = ["+CMS_ALLOW_OVERFLOW=False"]

# config.section_("User")
# config.User.voGroup = 'dcms'
