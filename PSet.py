#this fake PSET is needed for local test and for crab to figure the output filename
#you do not need to edit it unless you want to do a local test using a different input file than
#the one marked below
import FWCore.ParameterSet.Config as cms
import FWCore.ParameterSet.VarParsing as VarParsing
#import utils

process = cms.Process('NANO')

#options = VarParsing.VarParsing ('analysis')
#options.register ('dataset',
#        '/varparsing/default/dataset', # default value
#        VarParsing.VarParsing.multiplicity.singleton,
#        VarParsing.VarParsing.varType.string,
#        "Full name of the dataset")
#options.register ('isMC',
#        1, # default value
#        VarParsing.VarParsing.multiplicity.singleton,
#        VarParsing.VarParsing.varType.int,
#        "MC or data?")
#options.parseArguments()
#
process.source = cms.Source("PoolSource", fileNames = cms.untracked.vstring(),
#   lumisToProcess=cms.untracked.VLuminosityBlockRange("254231:1-254231:24")
)
process.source.fileNames = [
    '../../NanoAOD/test/lzma.root' ##you can change only this line
]
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(1))
##process.output = cms.OutputModule("PoolOutputModule", fileName = cms.untracked.string('tree.root'))
#process.output = cms.OutputModule(
#    "PoolOutputModule",
#     fileName = cms.untracked.string(utils.GetOutputFilename(options.dataset,options.isMC))
#)
#process.out = cms.EndPath(process.output)
#
