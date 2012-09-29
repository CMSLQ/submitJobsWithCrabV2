#!/usr/bin/env perl

#--------------------------------------------------------------
# Francesco Santanastasio  <francesco.santanastasio@cern.ch>
#--------------------------------------------------------------

print "Starting...\n";

use Time::Local;
use Getopt::Std;


## input info

my $statusReport="statusReport.log";
my $prodDir;
my $inputList;

my $IsSubmitJobs=0;
my $IsGetOutput=0;
my $IsKillJobs=0;
my $IsResubmitJobs=0;
my $IsPublishJobs=0;
my $IsReportJobs=0;

getopts('h:d:g:k:r:s:p:t:');

if(!$opt_d) {help();}
#if(!$opt_i) {help();}

if($opt_h) {help();}
if($opt_d) {$prodDir = $opt_d;}
if($opt_s && !$opt_g && !$opt_k && !$opt_r && !$opt_p && !$opt_t) {$IsSubmitJobs = 1;}
if($opt_g && !$opt_k && !$opt_r && !$opt_s && !$opt_p && !$opt_t) {$IsGetOutput = 1;}
if($opt_k && !$opt_g && !$opt_r && !$opt_s && !$opt_p && !$opt_t) {$IsKillJobs = 1;}
if($opt_r && !$opt_g && !$opt_k && !$opt_s && !$opt_p && !$opt_t) {$IsResubmitJobs = 1;}
if($opt_p && !$opt_g && !$opt_k && !$opt_s && !$opt_r && !$opt_t) {$IsPublishJobs = 1;}
if($opt_t && !$opt_g && !$opt_k && !$opt_s && !$opt_r && !$opt_p) {$IsReportJobs = 1;}

$inputList = $opt_d."\/inputList.txt";

## create directories

my $productionDir = $prodDir;
my $workDir = $productionDir."\/"."workdir"; 
my $outputDir = $productionDir."\/"."output"; 


## read input list

open (INPUTLIST, "<$inputList") || die ("...error opening file $inputList $!");
@inputListFile = <INPUTLIST>;
#print @inputListFile;
close(INPUTLIST);


## open file for final status report

open(STATUSREPORT,">$statusReport");


my $fullCrabStatusOutput="statusCrab.log";
system "rm -f $fullCrabStatusOutput";
system "touch $fullCrabStatusOutput";

## loop over datasets in the list

foreach $inputListLine(@inputListFile)
{
    chomp($inputListLine); 
    #print $inputListLine;


    ## split each line

    my ($dataset, $Nevents, $Njobs) = split(/\s+/, $inputListLine);
    my @datasetParts = split(/\//, $dataset);
    shift @datasetParts; #remove the first element of the list which is an empty-space

    #print "$dataset\n";
    #print "@datasetParts\n";
    #print "$datasetParts[1]\n";
    #print "$Nevents\n";
    #print "$Njobs\n";
    #print "\n";

    print "\n";
    print "processing dataset : $dataset ... \n";


    ## create datasetname

    my $datasetName="";
    my $counter=0;
    foreach $name(@datasetParts)
    {
	$counter++;
	if( $counter < scalar(@datasetParts) ) {$datasetName=$datasetName.$name."__";}
	else {$datasetName=$datasetName.$name;}
	    
    }
    $counter=0;


    ## create workdir for this dataset

    my $thisWorkDir=$workDir."/".$datasetName;

    print STATUSREPORT "\n";
    print STATUSREPORT "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";
    print STATUSREPORT "Dataset: $dataset \n";
    print STATUSREPORT "Workdir: $thisWorkDir \n";


    ## get output of crab jobs for this dataset

    print "getting output of crab jobs for dataset $dataset ... \n"; 


    if($IsSubmitJobs==1)
    {
	print "crab -submit all -c $thisWorkDir\n";
	system "crab -submit all -c $thisWorkDir";
    }

    if($IsGetOutput==1)
    {
	print "crab -getoutput all -c $thisWorkDir\n";
	system "crab -getoutput all -c $thisWorkDir";
    }

    if($IsKillJobs==1)
    {
	print "crab -kill all -c $thisWorkDir\n";
	system "crab -kill all -c $thisWorkDir";
    }

    if($IsResubmitJobs==1)
    {
	print "crab -resubmit all -c $thisWorkDir\n";
	system "crab -resubmit all -c $thisWorkDir";
    }

    if($IsPublishJobs==1)
    {
	print "crab -publish all -c $thisWorkDir\n";
	system "crab -publish all -c $thisWorkDir";
    }

    if($IsReportJobs==1)
    {       	
	print "crab -report all -c $thisWorkDir\n";

	### TEMPORARY see https://hypernews.cern.ch/HyperNews/CMS/get/crabFeedback/3173/1/2/2.html
	#system "cp $outputDir/crab_*.xml $thisWorkDir/res";
	###

	system "crab -status all -c $thisWorkDir";
	system "crab -report all -c $thisWorkDir";
    }


    ## check status of crab jobs for this dataset and fill the status report

    print "checking status of crab jobs for dataset $dataset ... \n"; 

    my $tempLog="temp.log";

    print "crab -status -c $thisWorkDir >& $tempLog \n";
    system "crab -status -c $thisWorkDir >& $tempLog";

    system "cat $tempLog \>\> $fullCrabStatusOutput";

    
    open (TEMPLOG, "<$tempLog") || die ("...error opening file $inputList $!");
    @tempLogFile = <TEMPLOG>;
    #print "@tempLogFile\n";
    close(TEMPLOG);
    
    my $printThisLine=0;

    foreach $tempLogFileLine(@tempLogFile)
    {

	chomp ($tempLogFileLine);

	if($tempLogFileLine=~/^(\s){10}(\w){1,}/)
	{
	    print STATUSREPORT "$tempLogFileLine\n";
	}

#	if($printThisLine==1)
#	{
#	    print STATUSREPORT "$tempLogFileLine\n";
#	}

	if($tempLogFileLine=~/\>\>\>\>\>\>\>\>\>/)
	{
	    #print "$tempLogFileLine\n"; 
	    print STATUSREPORT "$tempLogFileLine\n";
	    #$printThisLine=1;
	}

    }

    
    print STATUSREPORT "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";

    #print "rm -f $tempLog\n";
    system "rm -f $tempLog";

    ## temporary until the code is fixed
    system "rm -f $statusReport";
    ####################################

}


close(STATUSREPORT);


print "\n";
print ">>>>>> full output from the commands \"crab -status\" at $fullCrabStatusOutput\n";
#print ">>>>>> summary status report on the full production at $statusReport\n";



#---------------------------------------------------------#

sub help(){
    print "Usage: ./postCreationCommandsWithCrab.pl -d <prodDir> [-s <submitJobs?> -g <getOutput?> -k <killJobs?> -r <resubmitJobs?> -p <publishJobs> -h <help?>] \n";
    print "Example to only get the status: ./postCreationCommandsWithCrab.pl -d /<dir-path>/RootNtuples/V00-03-07-DATA-xxxxxx-yyyyyy \n";
    print "Example to submit: ./postCreationCommandsWithCrab.pl -d /<dir-path>/RootNtuples/V00-03-07-DATA-xxxxxx-yyyyyy -s yes \n";
    print "Example to kill jobs: ./postCreationCommandsWithCrab.pl -d /<dir-path>/RootNtuples/V00-03-07-DATA-xxxxxx-yyyyyy -k yes \n";
    print "Options:\n";
    print "-d <prodDir>:          choose the production directory\n";
    print "-h <yes> :             to print the help \n";
    print "-s <yes>:              submit all jobs\n";
    print "-g <yes>:              getoutput from all jobs\n";
    print "-k <yes>:              kill all jobs\n";
    print "-r <yes>:              resubmit all jobs\n";
    print "-p <yes>:              publish all jobs\n";
    print "-t <yes>:              report all jobs\n";

    print "NOTE: s,g,k,r,p,t cannot be used togheter (they are mutually exclusive) \n";

    die "please, try again...\n";
}

