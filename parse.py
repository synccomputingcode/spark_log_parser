from parsing_models.application_model_v2 import sparkApplication

import os
import argparse
            
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', help='Log file ')
    args = parser.parse_args()  

    print('\n' + '*'*12 + '  Running the Log Parser for Spark Predictor' + '*'*12 + '\n')
    if args.d == None:
        raise Exception('Must specify log file with -d option')

    logFile = args.d    
    logFile = os.path.abspath(logFile)



    dir_path = os.path.dirname(os.path.realpath(__file__))
    saveDir  = os.path.join(dir_path, 'results')

    if not os.path.isdir(saveDir):
        os.mkdir(saveDir)
    
    print('\n--Processing log file: ' + logFile)

    fileName = logFile.replace('/', '.').split('.')[-1]
    savePath = os.path.join(saveDir, 'parsed-' + fileName)

    try:
        # Save the current result
        if os.path.exists(savePath):
            os.remove(savePath)

        appobj = sparkApplication(eventlog=logFile)
        appobj.save(savePath)

        print(f'--Log directory saved to: {savePath}')

    except Exception as e:
        print(e)