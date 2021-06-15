import pandas as pd
import os
import argparse

import application_model as appModel

def process_log_file(logFile, overwrite):

    dir_path = os.path.dirname(os.path.realpath(__file__))
    saveDir  = os.path.join(dir_path, 'results')

    if not os.path.isdir(saveDir):
        os.mkdir(saveDir)
    
    print('\n--Processing log file: ' + logFile)

    fileName = logFile.replace('/', '.').split('.')[-2]
    savePath = os.path.join(saveDir, fileName)

    if os.path.isfile(savePath) and (overwrite == False):
        print('-->Log directory already processed. Use -o option to reprocess and overwrite')
    else:
        try:
            # Save the current result
            if os.path.exists(savePath):
                os.remove(savePath)

            appobj = appModel.sparkApplication(eventlog=logFile)
            appobj.save(savePath)

        except Exception as e:
            print(e)

            
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', help='Log file ')
    parser.add_argument('-o', action='store_true', help="Overwrite. Including this option will cause pre-existing parsed logs to be overwritten. Use to reprocess old logs.")
    args = parser.parse_args()  


    print('\n' + '*'*12 + '  Running the Log Parser for Spark Predictor' + '*'*12 + '\n')
    if args.d == None:
        raise Exception('Must specify log file with -d option')

    logFile = args.d    
    overwrite   = args.o

    logFile = os.path.abspath(logFile)
    process_log_file(logFile, overwrite=overwrite)
