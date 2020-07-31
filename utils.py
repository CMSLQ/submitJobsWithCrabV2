#!/usr/bin/env python


def GetOutputDatasetTag(dataset):
    return GetOutputDatasetTagAndModifiedDatasetName(dataset)[0]


def GetModifiedDatasetName(dataset):
    return GetOutputDatasetTagAndModifiedDatasetName(dataset)[1]


def GetOutputDatasetTagAndModifiedDatasetName(dataset):
    outputDatasetTag = "LQ"  # overridden for data
    datasetNoSlashes = dataset[1: len(dataset)].replace("/", "__")
    # datasetNameNoSlashes looks like SinglePhoton__Run2015D-PromptReco-v3
    # so split to just get Run2015D-PromptReco-v3
    # and use that as the outputDatasetTag to get it into the EOS path
    primaryDatasetName = datasetNoSlashes.split("__")[0]
    secondaryDatasetName = datasetNoSlashes.split("__")[1]
    # print 'primaryDatasetName={}'.format(primaryDatasetName)
    # print 'secondaryDatasetName={}'.format(secondaryDatasetName)
    datasetName = datasetNoSlashes
    datasetName = (
        datasetName.split("__")[0] + "__" + datasetName.split("__")[1]
    )  # get rid of part after last slash
    # thisWorkDir = workDir+'/'+datasetName
    isData = "Run20" in datasetName
    if not isData:
        datasetName = datasetName.split("__")[0]
    #  config.Data.splitting = 'FileBased'
    #  config.Data.unitsPerJob = nUnitsPerJob
    # else:
    #  config.Data.outputDatasetTag=secondaryDatasetName
    #  config.Data.splitting = 'Automatic'
    # Handle the ext1 vs non ext case specially
    # print 'datasetName={}'.format(datasetName)
    if not isData:
        if "ext" in dataset:
            extN = dataset[dataset.find("_ext") + 4]
            datasetName = datasetName + "_ext" + extN
            outputDatasetTag = "LQ_ext" + extN
        if "new_pmx" in dataset:
            datasetName = datasetName + "_newPMX"
            outputDatasetTag = "LQ-newPMX"
        if "backup" in dataset:
            datasetName = datasetName + "_backup"
            outputDatasetTag = "LQ_backup"
        # This is for DY 10-50 which has a v1 and v2, and an ext1
        if "-v2" in dataset:
            datasetName = datasetName + "-v2"
            outputDatasetTag = "LQ-v2"
        elif "-v1" in dataset:
            datasetName = datasetName + "-v1"
            outputDatasetTag = "LQ-v1"
    else:
        outputDatasetTag = secondaryDatasetName
    return (
        outputDatasetTag,
        datasetName,
        primaryDatasetName,
        secondaryDatasetName,
        isData,
    )


def GetOutputFilename(dataset, isMC):
    outputFileNames = []
    outputFileNames.append(dataset[1: dataset.find("_Tune")])
    outputFileNames.append(dataset[1: dataset.find("_13TeV")])
    outputFileNames.append(dataset.split("/")[1])
    # get the one with the shortest filename
    outputFile = sorted(outputFileNames, key=len)[0]
    # special case for ZToEE samples
    if 'ZToEE' in dataset:
        outputFile = dataset.split("/")[1].replace('TuneCP5_','').replace('13TeV-','')
    if not isMC:
        outputFile = outputFile + "_" + GetOutputDatasetTag(dataset)
    if "ext" in dataset:
        extN = dataset[dataset.find("_ext") + 4]
        outputFile = outputFile + "_ext" + extN
    if "new_pmx" in dataset:
        outputFile = outputFile + "_newPMX"
    if "backup" in dataset:
        outputFile = outputFile + "_backup"
    return outputFile + ".root"


def GetFileList(inputList):
    fileList = []
    if len(inputList) > 0:
        with open(inputList, "r") as filelist:
            for line in filelist:
                fileList.append(line.strip())
    else:
        # fileList.append('/afs/cern.ch/user/s/scooper/work/private/cmssw/1100pre10/AddTrigObjMatchToNano/src/PhysicsTools/NanoAOD/test/myNanoProdMc2016_trigObj_NANO.root')
        # fileList.append('/afs/cern.ch/user/s/scooper/work/private/cmssw/1100pre9/MinimalStockNanoChanges2/src/PhysicsTools/NanoAOD/test/myNanoProdMc2016_NANO_2.root')
        # fileList.append('root://cms-xrd-global.cern.ch//store/mc/RunIISummer16NanoAODv6/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/NANOAODSIM/PUMoriond17_Nano25Oct2019_102X_mcRun2_asymptotic_v7_ext2-v1/100000/E65D285B-B38F-F14A-AE07-87A7FDCF11E7.root')
        fileList.append("/tmp/scooper/FF746568-EC2F-8E41-8BF2-840FA8E95F9A.root")
    return fileList
