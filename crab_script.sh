#this is not mean to be run locally
#
echo Check if TTY
if [ "`tty`" != "not a tty" ]; then
  echo "YOU SHOULD NOT RUN THIS IN INTERACTIVE, IT DELETES YOUR LOCAL FILES"
else

  ls -lR .
  echo "ENV..................................."
  env 
  echo "VOMS"
  voms-proxy-info -all
  echo "CMSSW BASE, python path, pwd"
  echo $CMSSW_BASE 
  echo $PYTHON_PATH
  echo $PWD 
  rm -rf $CMSSW_BASE/lib/
  rm -rf $CMSSW_BASE/src/
  rm -rf $CMSSW_BASE/module/
  rm -rf $CMSSW_BASE/python/
  mv lib $CMSSW_BASE/lib
  mv src $CMSSW_BASE/src
  mv module $CMSSW_BASE/module
  mv python $CMSSW_BASE/python
  
  echo Found Proxy in: $X509_USER_PROXY
  # $1 is always the job number
  # see: https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3ConfigurationFile
  # $2 is the dataset name; we pass this in via the scriptArgs CRAB cfg parameter
  # $3 is a 0/1 flag for isMC
  python doSkim_stockNanoV5.py --$2 --$3

fi
