import sys
import subprocess as sp

d_reco_era = { "Run2012A-recover-06Aug2012-v" : "2012A",
               "Run2012A-13Jul2012-v"         : "2012A",        
               "Run2012B-13Jul2012-v"         : "2012B",        
               "Run2012C-24Aug2012-v"         : "2012C",        
               "Run2012C-PromptReco-v2"       : "2012C" }

d_era_datasets = { "2012A": [ "SingleElectron", "SingleMu", "TauPlusX", "MuEG", "Photon" ],
                   "2012B": [ "SingleElectron", "SingleMu", "TauPlusX", "MuEG", "SinglePhoton", "DoublePhotonHighPt" ],
                   "2012C": [ "SingleElectron", "SingleMu", "TauPlusX", "MuEG", "SinglePhoton", "DoublePhotonHighPt" ] } 

d_reco_folder = { "Run2012A-recover-06Aug2012-v" : "Inputs_ReReco_2012A_06Aug2012",      
                  "Run2012A-13Jul2012-v"         : "Inputs_ReReco_2012A_13Jul2012",
                  "Run2012B-13Jul2012-v"         : "Inputs_ReReco_2012B_13Jul2012",
                  "Run2012C-24Aug2012-v"         : "Inputs_ReReco_2012C_24Aug2012", 
                  "Run2012C-PromptReco-v2"       : "Inputs_PromptReco_2012C_v2"    } 

recos = d_reco_era.keys() 
eras  = d_reco_era.values()
eras  = list ( set ( eras ) ) 
folders = d_reco_folder.values() 
folders = list ( set ( folders ) ) 

for reco in recos: 
    
    era      = d_reco_era     [ reco ] 
    datasets = d_era_datasets [ era  ]
    folder   = d_reco_folder  [ reco ] 
    
    print reco

    for dataset in datasets : 
        
        dataset_wildcard = "/" + dataset + "/*" + reco + "*/AOD"        
        search_command   = "dbsql find dataset where dataset like " + dataset_wildcard
        search_results   = sp.Popen ( search_command, shell=True, stdout=sp.PIPE ).communicate()[0].split("\n")
        
        dataset_candidates = []
        for result in search_results:
            if dataset not in result: continue
            else : dataset_candidates.append ( result.strip() ) 

        if len ( dataset_candidates ) != 1: 
            print "Error!  I found more than one dataset with this wildcard:", dataset_wildcard
            for candidate in dataset_candidates:
                print "\t", candidate
            sys.exit() 
        
        dataset_name = dataset_candidates[0]
        file_name    = folder + "/" + "InputList_" + dataset + ".txt"

        file = open ( file_name , "w" ) 
        line = dataset_name + " -1 100\n"
        file.write ( line ) 
        file.close() 
        
        print "\t", file_name, "\t", line
