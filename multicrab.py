#!/usr/bin/env python2
import os
import sys
import subprocess
from optparse import OptionParser
from httplib import HTTPException

#
# Use commands like ./multicrab.py -c status -d runBetaOneLQ1MC/testTag_2015Jul13_104935/
#   This will check the status of the submitted crab jobs over multiple datasets.

try:
    from CRABAPI.RawCommand import crabCommand
except ImportError:
    print
    print(
        "ERROR: Could not load CRABAPI.RawCommand module.  Please source the crab3 setup:"
    )
    print("source /cvmfs/cms.cern.ch/common/crab-setup.sh")
    exit(-1)


def getOptions():
    """
    Parse and return the arguments provided by the user.
    """
    usage = (
        "usage: %prog -c CMD -d DIR [-o OPT]\nThe multicrab command"
        ' executes "crab CMD OPT" for each task contained in DIR\nUse'
        ' multicrab -h for help"'
    )

    parser = OptionParser(usage=usage)
    parser.add_option(
        "-c",
        "--crabCmd",
        dest="crabCmd",
        help=("The crab command you want to execute for each task in " "the DIR"),
        metavar="CMD",
    )
    parser.add_option(
        "-d",
        "--projDir",
        dest="projDir",
        help="The directory where the tasks are located",
        metavar="DIR",
    )
    parser.add_option(
        "-o",
        "--crabCmdOptions",
        dest="crabCmdOptions",
        help=("The options you want to pass to the crab command CMD" "tasklistFile"),
        metavar="OPT",
        default="",
    )
    parser.add_option(
        "-r",
        "--noAutoResubmit",
        dest="noAutoResubmit",
        help=("don't automatically run the resub commands"),
        metavar="noAutoResub",
        default=False,
        action="store_true",
    )
    parser.add_option(
        "-i",
        "--ignoreCache",
        dest="ignoreMulticrabCache",
        help=("don't use cache file to skip checking status of jobs already done"),
        metavar="ignoreCache",
        default=False,
        action="store_true",
    )

    (options, args) = parser.parse_args()

    if args:
        parser.error("Found positional argument(s) %s." % args)
    if not options.crabCmd:
        parser.error("(-c CMD, --crabCmd=CMD) option not provided")
    if not options.projDir:
        parser.error("(-d DIR, --projDir=DIR) option not provided")
    if not os.path.isdir(options.projDir):
        parser.error("Directory %s does not exist" % options.projDir)

    return options


def main():
    """
    Main
    """
    options = getOptions()

    completedTasksFromCache = []
    if not options.ignoreMulticrabCache:
        # read our cache file (don't check status for completed tasks each time)
        cacheFile = os.path.abspath(options.projDir + "/multicrab.cache")
        if os.path.isfile(cacheFile):
            ourCacheFile = open(cacheFile, "r")
            for line in ourCacheFile:
                completedTasksFromCache.append(line.strip())
            ourCacheFile.close()

    ourCacheFile = open(cacheFile, "a")
    tasksStatusDict = {}
    # Execute the command with its arguments for each task.
    for task in os.listdir(options.projDir):
        task = os.path.join(options.projDir, task)
        if not os.path.isdir(task):
            continue
        # ignore non-crab dirs
        if "workdir" in task or "cfgfiles" in task or "output" in task:
            continue
        if task in completedTasksFromCache:
            print "Don't run command on task, was already completed:", task
            tasksStatusDict[task] = "COMPLETED"
            continue
        print
        print (
            "Executing (the equivalent of): crab %s %s %s"
            % (options.crabCmd, task, options.crabCmdOptions)
        )
        try:
            res = crabCommand(options.crabCmd, task, *options.crabCmdOptions.split())
        except HTTPException, hte:
            sys.stdout.write("\033[1;31m")  # red
            print("-----> there was a problem. see below.")
            print(hte.headers)
            sys.stdout.write("\033[0;0m")  # regular color
            if options.crabCmd != "kill":
                exit(-1)
        # stop here if we're not checking the status
        if options.crabCmd != "status":
            tasksStatusDict[task] = 'unknown, not queried'
            continue
        if "failed" in res["jobsPerStatus"].keys():
            tasksStatusDict[
                task
            ] = "FAILED"  # if there's at least one failed job, count task as FAILED so we resubmit
        else:
            tasksStatusDict[task] = res["status"]
        # print "res['jobsPerStatus'].keys()={}".format(res['jobsPerStatus'].keys())
        # print res
        if tasksStatusDict[task] == "COMPLETED":
            ourCacheFile.write(task + "\n")
    ourCacheFile.close()

    totalTasks = len(tasksStatusDict)
    tasksCompleted = [
        task for task in tasksStatusDict if tasksStatusDict[task] == "COMPLETED"
    ]
    tasksSubmitted = [
        task for task in tasksStatusDict if tasksStatusDict[task] == "SUBMITTED"
    ]
    tasksFailed = [
        task for task in tasksStatusDict if tasksStatusDict[task] == "FAILED"
    ]
    tasksOther = [
        task
        for task in tasksStatusDict
        if task not in tasksCompleted
        and task not in tasksSubmitted
        and task not in tasksFailed
    ]
    print
    print
    print "SUMMARY"
    if len(tasksCompleted) > 0:
        print "Tasks completed:", len(tasksCompleted), "/", totalTasks
    if len(tasksSubmitted) > 0:
        print "Tasks submitted:", len(tasksSubmitted), "/", totalTasks
    if len(tasksFailed) > 0:
        print "Tasks failed:", len(tasksFailed), "/", totalTasks
    if len(tasksOther) > 0:
        print "Tasks with other status:", len(tasksOther), "/", totalTasks
        for task in tasksOther:
            print "\tTask:", task, "\tStatus:", tasksStatusDict[task]
    if len(tasksFailed) > 0:
        print "commands to resubmit failed tasks (or tasks with failed jobs):"
        for task in tasksFailed:
            # resubmitCmd = 'crab resubmit --maxmemory=2500 '+task
            resubmitCmd = "crab resubmit --maxmemory=2000 --maxjobruntime=2000 " + task
            print
            print "\t" + resubmitCmd
            if not options.noAutoResubmit:
                print "Automatically resubmitting..."
                subprocess.call(resubmitCmd.split())


if __name__ == "__main__":
    main()
