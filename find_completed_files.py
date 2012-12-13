#!/usr/bin/python

import sys
import subprocess as sp

folder = sys.argv[1]
cfg_files_folder = folder + "/cfgfiles/"
find_command = "find " + folder + " -name \"crab.cfg\""
crab_cfg_file_paths = sp.Popen (find_command, shell=True,stdout=sp.PIPE).communicate()[0].split()

crab_commands = []

for crab_cfg_file_path in crab_cfg_file_paths:

    crab_cfg_file = open ( crab_cfg_file_path, "r" )
    crab_dir = crab_cfg_file_path.replace("crab.cfg","")
    
    xml_file_path = crab_cfg_file_path.replace ("crab.cfg","arguments.xml")
    number_of_jobs_actual_command = "cat " + xml_file_path + " | grep MaxEvents | wc -l"
    number_of_jobs = int (sp.Popen ( number_of_jobs_actual_command, shell=True, stdout=sp.PIPE).communicate()[0])
    
    crab_cfg_data = crab_cfg_file.read()
    crab_cfg_file.close()
     
    user_remote_dir       = "/eos/cms/store/" + crab_cfg_data.split("user_remote_dir=/")[1].split("\n")[0].strip()
    output_file_prefix    = crab_cfg_data.split("output_file=")[1].split(".root")[0]

    files_in_remote_dir_command = "/afs/cern.ch/project/eos/installation/0.2.5/bin/eos.select ls " + user_remote_dir
    files_in_remote_dir = sp.Popen ( files_in_remote_dir_command, shell=True,stdout=sp.PIPE).communicate()[0]
    
    missing_job_numbers = []

    for job_number in range (1, number_of_jobs + 1):
        output_file_prefix2 = output_file_prefix + "_" + str (job_number) + "_" 
        if output_file_prefix2 not in files_in_remote_dir:
            missing_job_numbers.append ( job_number ) 
            
    if len ( missing_job_numbers ) != 0:
        crab_command = "crab -c " + crab_dir.replace("/share","") + " -resubmit "
        for job_number in missing_job_numbers:
            print "Can't find:", user_remote_dir + "/"+ output_file_prefix + "_" + str (job_number) + "_*.root" 
            crab_command = crab_command + str ( job_number ) + ","
        crab_command = crab_command[:-1]
        
        print "ERROR: Missing", len ( missing_job_numbers ), output_file_prefix, "files"
        crab_commands.append ( crab_command ) 
        
    else : print "No missing " + output_file_prefix + " files "

if len ( crab_commands ) != 0 :
    output_file = open("jobs.sh" ,"w") 
    for crab_command in crab_commands:
        output_file.write(crab_command + "\n") 
        
    print "\n\n"
    print "I have written", len ( crab_commands   ), "resubmit commands"
    print "\n\n"
    
    print "Execute the following command"
    print "\n\n"
    print "\tsource jobs.sh" 
    print "\n\n"

else: print "Nothing to do!" 
