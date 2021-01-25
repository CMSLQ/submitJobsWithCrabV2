import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True

from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module

class eventCounterHistogramProducer(Module):
    def __init__(self):
        pass
    
    def beginJob(self):
        pass
    
    def endJob(self):
        pass

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.passedEvents = 0
        self.h_count = ROOT.TH1F("EventCounter","Event Counter",4,-0.5,3.5)
        self.h_count.GetXaxis().SetBinLabel(1,"all events")
        self.h_count.GetXaxis().SetBinLabel(2,"passed")
        self.h_count.GetXaxis().SetBinLabel(3,"sum of amc@NLO weights")
        self.h_count.GetXaxis().SetBinLabel(4,"sum of TopPt weights")
        maxInt = 16777216 # for TH1F (max exact float representation of int = 2^[mantissaBits+1] = 2^24)
        entries = inputTree.GetEntriesFast()
        if entries >= maxInt:
            raise RuntimeError("Had more entries in the tree ["+str(entries)+"] than we can store in the eventCounter histogram bin ["+str(maxInt)+"]!")
        self.h_count.SetBinContent(1,entries)
        runsTree = inputFile.Get("Runs")
        runsTree.GetEntry(0)
        if runsTree.GetBranch("genEventSumw"):
            self.h_count.SetBinContent(3,runsTree.GetBranch("genEventSumw").GetLeaf("genEventSumw").GetValue())
        elif runsTree.GetBranch("genEventSumw_"):
            self.h_count.SetBinContent(3,runsTree.GetBranch("genEventSumw_").GetLeaf("genEventSumw_").GetValue())
        # Can't work with data, turning off error
        #else:
        #    raise RuntimeError("Count not find the genEventSumw branch; tried genEventSumw and genEventSumw_")


    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.h_count.SetBinContent(2,self.passedEvents)
        prevdir = ROOT.gDirectory
        outputFile.cd()
        self.h_count.Write()
        prevdir.cd()        

    def analyze(self, event):
        self.passedEvents+=1
        return True

eventCounterHistogramModule = lambda : eventCounterHistogramProducer() 
