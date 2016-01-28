#echo "CMSSW_BASE=$CMSSW_BASE"
#cd $CMSSW_BASE/src
#scram setup lhapdf
#scram b
#cd -
cmsRun -j FrameworkJobReport.xml -p PSet.py
