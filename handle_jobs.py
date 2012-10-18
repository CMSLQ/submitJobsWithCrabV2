import sys

input_file_path = "statusCrab.log"
input_file = open ( input_file_path, "r" ) 

data_all = input_file.read()    
data_bySample = data_all.split("crab. Working options:")

resubmit_commands   = []
get_output_commands = []

crab_directories = []

d_crabDirectory_listOfRawRowsToResubmit  = {}
d_crabDirectory_listOfRawRowsToGetOutput = {}
d_crabDirectory_listOfJobNumbersToResubmit = {}
d_crabDirectory_listOfJobNumbersToGetOutput = {}

for raw_data in data_bySample[1:]:
    
    crab_directory = raw_data.split("working directory")[1].split("crab:")[0].strip()
    raw_job_list = raw_data.split("crab:")[2].strip()
    
    rows = [] 
    job_numbers = []
    column_titles = []

    d_jobNumber_columnTitle_value = {} 
    d_jobNumber_rawRow = {} 

    crab_directories.append ( crab_directory ) 
    d_crabDirectory_listOfRawRowsToResubmit  [crab_directory] = []
    d_crabDirectory_listOfRawRowsToGetOutput [crab_directory] = []

    for line in raw_job_list.split("\n"):
        if "---" in line: continue

        raw_row = line.strip()
        job_number = raw_row.split()[0]

        d_columnTitle_value = {} 

        if "STATUS" in raw_row: 
            row = raw_row.split()
            column_titles = row 
            continue
        if   len ( raw_row.split() ) == 7:
            row = raw_row.split()
        elif len ( raw_row.split() ) == 4:
            row = raw_row.split() + [ "NULL", "NULL", "NULL" ] 
        elif len ( raw_row.split() ) == 5:
            row = raw_row.split()[0:4] + [ "NULL", "NULL" ] + raw_row.split()[-1:]
        else:
            print "Error!  Don't know how to process row =", row
            sys.exit()

        rows.append ( row ) 
        job_numbers.append ( job_number ) 
        d_jobNumber_columnTitle_value[job_number] = dict ( zip ( column_titles, row ) )
        d_jobNumber_rawRow [ job_number ] = raw_row
    
    job_numbers_to_resubmit   = ""
    job_numbers_to_get_output = ""

    for job_number in job_numbers:
        
        exe_exit_code = d_jobNumber_columnTitle_value[job_number]["ExeExitCode"]
        job_exit_code = d_jobNumber_columnTitle_value[job_number]["JobExitCode"]
        ce_site       = d_jobNumber_columnTitle_value[job_number]["E_HOST"]
        status        = d_jobNumber_columnTitle_value[job_number]["STATUS"]
        raw_row       = d_jobNumber_rawRow [ job_number ]
        
        resubmit   = False
        get_output = False
        
        if    status == "Running"  : continue
        elif  status == "Ready"    : continue
        elif  status == "Scheduled": continue

        elif  status == "Aborted"  : resubmit   = True
        elif  status == "Done"     : get_output = True
        elif  status == "Retrieved":
            if job_exit_code != "0" or exe_exit_code != "0": resubmit = True
        else:
            print "Error! Don't know how to process this status:", status
            sys.exit()
            
        if resubmit  : 
            job_numbers_to_resubmit   = job_numbers_to_resubmit   + "," + job_number
            d_crabDirectory_listOfRawRowsToResubmit[crab_directory].append ( raw_row)
        if get_output: 
            job_numbers_to_get_output = job_numbers_to_get_output + "," + job_number
            d_crabDirectory_listOfRawRowsToGetOutput[crab_directory].append ( raw_row)

    if job_numbers_to_resubmit   != "" : 
        job_numbers_to_resubmit   = job_numbers_to_resubmit  [1:]
        resubmit_command = "crab -c " + crab_directory + " -resubmit " + job_numbers_to_resubmit 
        resubmit_commands.append ( resubmit_command ) 
        
    if job_numbers_to_get_output != "" :
        job_numbers_to_get_output = job_numbers_to_get_output[1:]
        get_output_command = "crab -c " + crab_directory + " -getoutput " + job_numbers_to_get_output 
        get_output_commands.append ( get_output_command ) 

input_file.close()

if len ( resubmit_commands ) + len ( get_output_commands ) == 0: 
    print "Nothing to do!" 
    sys.exit()

print "\n\n"

for crab_directory in crab_directories:
    
    list_of_raw_rows_to_resubmit   = d_crabDirectory_listOfRawRowsToResubmit [crab_directory]
    list_of_raw_rows_to_get_output = d_crabDirectory_listOfRawRowsToGetOutput[crab_directory]

    if len ( list_of_raw_rows_to_resubmit ) + len ( list_of_raw_rows_to_get_output ) == 0 : continue

    print "\n\n"
    print "For CRAB directory:", crab_directory.split("/")[-2]

    if list_of_raw_rows_to_resubmit:
        print "\t" + "jobs to resubmit"
        for raw_row_to_resubmit in list_of_raw_rows_to_resubmit:
            print "\t\t" + raw_row_to_resubmit


    if list_of_raw_rows_to_get_output:
        print "\t" + "jobs to get output"
        for raw_row_to_get_output in list_of_raw_rows_to_get_output:
            print "\t\t" + raw_row_to_get_output


print "\n\n"
print "I have written", len ( resubmit_commands   ), "resubmit commands"
print "\n\n"
print "I have written", len ( get_output_commands ), "get output commands"
print "\n\n"

output_file = open ( "jobs.sh", "w" ) 

if not get_output_commands:
    for resubmit_command in resubmit_commands:
        output_file.write ( resubmit_command + "\n" ) 
else:
    for get_output_command in get_output_commands:
        output_file.write ( get_output_command + "\n" ) 

print "Execute the following command"
print "\n\n"
print "\tsource jobs.sh" 
print "\n\n"

output_file.close()


