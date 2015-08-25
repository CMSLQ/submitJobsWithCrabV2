import os, sys, datetime
import subprocess as sp

# pass folder with InputLists or single inputlist on command line
if len(sys.argv) < 2:
  print 'ERROR: please specify directory with input lists or a single input list as argument'
  exit(-1)

cmdInput=sys.argv[1]

if(os.path.isdir(cmdInput)):
  txt_file_names = [folder+'/'+filename for filename in os.listdir(folder) if filename.startswith("InputList") and filename.endswith(".txt")]
else:
  txt_file_names = [cmdInput]

verbose = True

t0_site_ses = [ "srm-cms.cern.ch" ]
t1_site_ses = [ "srm-cms.cern.ch",
                "cmssrm-fzk.gridka.de",
                "srmcms.pic.es",
                "ccsrm.in2p3.fr",
                "storm-fe-cms.cr.cnaf.infn.it",
                "srm2.grid.sinica.edu.tw",
                "srm-cms.gridpp.rl.ac.uk",
                "srm-cms-disk.gridpp.rl.ac.uk",
                "cmssrm.fnal.gov",
                "cmssrmdisk.fnal.gov" ]

                  

d_fileName_missingDatasets = {} 
d_fileName_tier01Datasets = {}
d_fileName_isReady = {} 

datasets_that_need_to_be_moved_to_T2 = []
missingDatasets = [] 

if verbose:
    print "I will examine these txt files:"
    for txt_file_name in txt_file_names : 
        print "\t", txt_file_name
    print "\n\n"

n_txt_file_names = len ( txt_file_names ) 

for i_txt_file_name, txt_file_name in enumerate(txt_file_names):

    if verbose: print i_txt_file_name + 1, "/", n_txt_file_names, "\t:\t", txt_file_name

    d_fileName_tier01Datasets  [ txt_file_name ] = []
    d_fileName_missingDatasets [ txt_file_name ] = []
    d_fileName_isReady         [ txt_file_name ] = True
    
    txt_file = open ( txt_file_name, "r" ) 
    
    for line in txt_file:
        
        line = line.strip()
        if line == "" : continue
        if line.startswith('#'): continue
        
        dataset = line.split()[0] 
        
        if verbose: print "\t",dataset

        sites_command = 'das_client.py --limit=0 --query="site dataset='+dataset+'"'
        
        sites_command_output = sp.Popen ( sites_command, shell=True, stdout=sp.PIPE ).communicate()[0].split("\n")

        t23_sites = [] 
        t01_sites = []

        for entry in sites_command_output: 
            if "Using DBS"   in entry : continue
            if "-----------" in entry : continue
            if "Showing" in entry: continue
            if entry.strip() == ""    : continue
            if entry.strip() == "site": continue
            
            site_se = entry.strip() 

            if site_se not in t0_site_ses and site_se not in t1_site_ses : 
                t23_sites.append ( site_se ) 
            else :
                t01_sites.append ( site_se ) 

        if   len ( t23_sites ) != 0: 
            for site in t23_sites: 
                if verbose: print "\t\t", site
        elif len ( t01_sites ) != 0:
            if verbose: print "Request move to T2:", dataset
            d_fileName_tier01Datasets  [ txt_file_name ].append ( dataset ) 
            d_fileName_isReady         [ txt_file_name ] = False
            datasets_that_need_to_be_moved_to_T2.append ( dataset )                 
        else : 
            if verbose: print "\t\tNO SITES"
            d_fileName_missingDatasets [ txt_file_name ].append ( dataset ) 
            d_fileName_isReady         [ txt_file_name ] = False
            missingDatasets.append ( dataset ) 

out_file_name = "check_availability_" + str(datetime.date.today().year) + "_" + str(datetime.date.today().month) + "_" + str(datetime.date.today().day) + ".log"

out_file = open ( out_file_name, "w" ) 

my_string = "\n\nThe following files are READY:" 
print my_string 
out_file.write ( my_string + "\n" )

for txt_file_name in txt_file_names : 
    if d_fileName_isReady[txt_file_name]: 
        my_string = "\t" + txt_file_name
        print my_string
        out_file.write ( my_string + "\n" )

my_string = "\n\nThe following files are NOT READY:"
print my_string
out_file.write ( my_string + "\n" )

for txt_file_name in txt_file_names : 
    if not d_fileName_isReady[txt_file_name]: 
        
        if len ( d_fileName_missingDatasets [ txt_file_name ] ) != 0 :
            my_string = "\t" + txt_file_name +"\n" "\t\t" + "waiting for these datasets to complete:"
            print my_string
            out_file.write ( my_string + "\n" )
            for missing_dataset in d_fileName_missingDatasets [ txt_file_name ]:
                my_string = "\t\t"  + missing_dataset
                print my_string
                out_file.write ( my_string + "\n" )

        if len ( d_fileName_tier01Datasets  [ txt_file_name ] ) != 0 :
            my_string = "\t" + txt_file_name +"\n" "\t\t" + "waiting for these datasets to be moved to a T2 or T3:" 
            print my_string
            out_file.write ( my_string + "\n" )
            for tier01_dataset in d_fileName_tier01Datasets [ txt_file_name ]:
                my_string = "\t\t"  + tier01_dataset
                print my_string
                out_file.write ( my_string + "\n" )
    
if len ( datasets_that_need_to_be_moved_to_T2 ) != 0: 
    
    my_string = "\n\nThe following datasets are available on T0 or T1 only\nRequest that they be moved to a T2:"
    print my_string
    out_file.write ( my_string + "\n" )

    for dataset in datasets_that_need_to_be_moved_to_T2: 
        my_string = "\t\t" + dataset
        print my_string
        out_file.write ( my_string + "\n" )
    
if len ( missingDatasets ) != 0:
    
    my_string = "\n\nStill waiting for these datasets to complete:" 
    print my_string
    out_file.write ( my_string + "\n" )
    
    for dataset in missingDatasets:
        my_string = "\t\t" + dataset
        print my_string
        out_file.write ( my_string + "\n" )

my_string = "\n\n"
print my_string
out_file.write ( my_string + "\n" )

out_file.close()
