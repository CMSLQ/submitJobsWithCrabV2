#!/usr/bin/env python2
import argparse
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import (
    PostProcessor,
)

# this takes care of converting the input files from CRAB
from PhysicsTools.NanoAODTools.postprocessing.framework.crabhelper import (
    inputFiles,
    runsAndLumis,
)

from PhysicsTools.NanoAODTools.postprocessing.modules.btv.btagSFProducer import *
from PhysicsTools.NanoAODTools.postprocessing.modules.jme.jetmetHelperRun2 import *
from PhysicsTools.NanoAODTools.postprocessing.modules.common.puWeightProducer import *

# from PhysicsTools.NanoAODTools.postprocessing.modules.common.pdfWeightProducer import *
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import (
    PostProcessor,
)
from PhysicsTools.NanoAODTools.postprocessing.modules.common.PrefireCorr import PrefCorr

from Leptoquarks.submitJobsWithCrabV2.eventCounterHistogramModule import *

import utils

parser = argparse.ArgumentParser("")
parser.add_argument(
    "-isMC",
    "--ismc",
    type=lambda x: (str(x).lower() == "true"),
    default=True,
    dest="isMC",
    help="",
)
# parser.add_argument('-isMC','--mc', dest='isMC', action='store_true')
# parser.add_argument('-isData','--data', dest='isMC', action='store_false')
# parser.set_defaults(isMC=True)
# parser.add_argument('-jobNum', '--jobNum', type=int, default=1, help="")
parser.add_argument("-era", "--era", type=str, default="2016", help="")
parser.add_argument("-dataRun", "--dataRun", type=str, default="X", help="")
# parser.add_argument('-haddFileName', '--haddFileName', type=str, default="tree.root", help="")
# parser.add_argument('-inputList', '--inputList', type=str, default="", help="")
parser.add_argument(
    "-dataset", "--dataset", type=str, default="/myTestDataset/testSecondary/NANOAODSIM", help=""
)
args = parser.parse_args()
print "args = ", args
isMC = args.isMC
era = args.era
dataRun = args.dataRun
# haddFileName = args.haddFileName
# inputList = args.inputList
dataset = args.dataset

print "isMC =", isMC, "era =", era, "dataRun =", dataRun, "dataset=", dataset

if isMC:
    jetmetCorrector = createJMECorrector(isMC=True, dataYear=era, jesUncert="All", redojec=True)
else:
    jetmetCorrector = createJMECorrector(isMC=False, dataYear=era, runPeriod=dataRun)

modulesToRun = []
modulesToRun.append(jetmetCorrector())
modulesToRun.append(PrefCorr())
# modulesToRun.append( pdfWeightProducer() )
jsonFile = None

# files=utils.GetFileList(inputList)
# print 'files=',files

if isMC:
    if era == "2016":
        modulesToRun.extend(
            [puAutoWeight_2016(), btagSFProducer("Legacy2016", "deepcsv")]
        )
    elif era == "2017":
        modulesToRun.extend([puAutoWeight_2017(), btagSFProducer("2017", "deepcsv")])
    elif era == "2018":
        modulesToRun.extend([puAutoWeight_2018(), btagSFProducer("2018", "deepcsv")])
else:
    if era == "2016":
        # jsonFile='/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/ReReco/Final/Cert_271036-284044_13TeV_ReReco_07Aug2017_Collisions16_JSON.txt'
        pass
    elif era == "2017":
        # jsonFile='/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/ReReco/Cert_294927-306462_13TeV_EOY2017ReReco_Collisions17_JSON.txt'
        pass
    elif era == "2018":
        # jsonFile='/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/ReReco/Cert_314472-325175_13TeV_17SeptEarlyReReco2018ABC_PromptEraD_Collisions18_JSON.txt'
        pass
    else:
        print "ERROR: Did not understand the given era!  Should be one of 2016,2017,2018. Quitting."
        exit(-1)

modulesToRun.append(eventCounterHistogramModule())

# Require lead electron SCEt > 35 GeV to keep the event
# preselection="(Electron_caloEnergy[0]/cosh(Electron_scEta[0]))>35"
# for stock, use regular Pt
#preselection = "Electron_pt[0] > 35"
# preselection="Electron_pt[0] > 100"
preselection="Electron_pt[0] > 45"
# preselection="Electron_pt[0]>45 || Muon_pt[0]>45 || (Muon_pt[0]>15 && Muon_pt[1]>8 && nJet>2 && Jet_pt[0]>23) || (Electron_pt[0]>17 && Electron_pt[1]>12 && nJet>2 && Jet_pt[0]>23)"
# remove preselection, as preselection is done in the custom2016SkimModule
keepAndDrop = "keepAndDrop.txt"

# for crab
haddFileName = utils.GetOutputFilename(dataset, isMC)
p = PostProcessor(
   ".",
   inputFiles(),
   cut=preselection,
   outputbranchsel=keepAndDrop,
   modules=modulesToRun,
   provenance=True,
   fwkJobReport=True,
   jsonInput=runsAndLumis(),
   haddFileName=haddFileName,
)
# interactive testing
# inputList = 'inputList_test2017MC.txt'
# p = PostProcessor(
#     ".",
#     utils.GetFileList(inputList),
#     cut=preselection,
#     outputbranchsel=keepAndDrop,
#     branchsel=keepAndDrop,
#     modules=modulesToRun,
#     provenance=True,
#     fwkJobReport=True,
#     jsonInput=runsAndLumis(),
#     haddFileName=haddFileName,
# )

p.run()
