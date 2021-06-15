import time
import os
import pandas as pd
import numpy as np
import json
import gzip
import colorsys 
import logging

from collections import defaultdict
from matplotlib.patches import Rectangle as rect
import matplotlib.pyplot as plt
from   matplotlib.patches import Patch

import application_model_prev

logging.basicConfig(format='%(levelname)s:%(message)s')

class sparkApplication():
    
    def __init__(self, 
        objfile  = None, # Previously saved object. This is the fastest and best option
        appobj   = None, # application_model object
        eventlog = None, # spark evenlog
        debug    = False
        ):

        if debug == True:
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logging.getLogger().setLevel(logging.WARNING)
            
        self.eventlog = eventlog
        self.existsSQL = False
        self.existsExecutors = False

        if objfile != None: # Load a previously saved sparkApplication Model
            self.load(objfile)
        
        if (appobj!=None) or (eventlog!=None): # Load an application_model or eventlog

            if eventlog!=None:
                t0 = time.time()
                appobj = application_model_prev.ApplicationModel(eventlogpath=eventlog)
                logging.info('Loaded object from spark eventlog [%.2fs]' % (time.time()-t0))
            else:
                logging.info('Loaded object from ApplicationModel object')
            
            # Get sql info if it exists
            if hasattr(appobj, 'sql') and appobj.sql:
                self.getSQLinfo(appobj)
                self.existsSQL = True
            else:
                logging.warning('No sql attribute found.')
                
                
            if hasattr(appobj, 'executors') and appobj.executors:
                self.getExecutorInfo(appobj)
                self.existsExecutors = True
            else:
                logging.warning('Executor attribute not found.')
                
                
            self.getAllJobData(appobj)
            self.getAllTaskData(appobj)
            self.getAllStageData(appobj)
            self.assignTasksToCores()
            self.getRecentEvents()
            #self.crossReferenceData(appobj)
            logging.info('sparkApplication object creation complete')      


    def getRecentEvents(self):

        tcomp = np.concatenate(([0.0], self.taskData['end_time'].values))

        if self.existsSQL:
            tcomp = np.concatenate((tcomp, self.sqlData['start_time'].values, self.sqlData['end_time'].values))

        trecent = []
        for sid in self.stageData.index.values:
            tstart = self.stageData.loc[sid]['start_time']
            trecent.append(tstart - tcomp[tcomp<tstart].max())

        self.stageData['time_since_last_event'] = trecent

        if self.existsSQL:
            trecent = []
            for qid in self.sqlData.index.values:
                tstart = self.sqlData.loc[qid]['start_time']

                tmp = tcomp[tcomp<tstart]

                if len(tmp) == 0:
                    trecent.append(0)
                else:
                    trecent.append(tstart - tcomp[tcomp<tstart].max())
            self.sqlData['time_since_last_event'] = trecent
                
    # This method collects all of the sql information into a dataframe
    def getSQLinfo(self, appobj):

        df = pd.DataFrame([])
        for sqlid, sql in appobj.sql.items():    
            sql_jobs   = []
            sql_stages = []
            sql_tasks  = []
            for jid, job in appobj.jobs.items():

                if ((job.submission_time >= sql['start_time']) and (job.submission_time <= sql['end_time'])) or \
                   ((job.completion_time >= sql['start_time']) and (job.completion_time <= sql['end_time'])):

                    sql_jobs.append(jid)               
                    for sid, stage in job.stages.items():
                        sql_stages.append(sid)

                        for task in stage.tasks:
                            sql_tasks.append(task.task_id)

            df = df.append(pd.DataFrame.from_dict({
                'sql_id':      [sqlid],
                'description': sql['description'], 
                'start_time':  [sql['start_time'] - appobj.start_time], 
                'end_time':    [sql['end_time']   - appobj.start_time],
                'duration':    [sql['end_time'] - sql['start_time']],
                'job_ids':     [sql_jobs],
                'stage_ids':   [sql_stages],
                'task_ids' :   [sql_tasks]
                 }))

        df = df.set_index('sql_id')
        self.sqlData = df
        
    def getExecutorInfo(self, appobj):
        
        cores = []
        execID = []
        for xid, executor in appobj.executors.items():
            execID.append(xid)
            cores.append(executor.cores)
            
        
        df = pd.DataFrame({
            'executor_id': execID,
            'cores': cores,
        })
        
        df = df.sort_values(['executor_id'])
        df = df.set_index('executor_id')
        self.executorData = df
    
    # This method collects all of the job-level data for this application
    # into a dataframe
    def getAllJobData(self, appobj):
        t1 = time.time()
        df = pd.DataFrame([]) 
        refTime = appobj.start_time
        for jid, job in appobj.jobs.items():
            
            stage_ids = []
            for sid, stage in job.stages.items():
                stage_ids.append(sid)
                
            df = df.append(pd.DataFrame.from_dict({
                'job_id'   : [jid],
                'sql_id'   : None,
                'stage_ids': [stage_ids],
                'submission_time': [job.submission_time - refTime],
                'completion_time': [job.completion_time - refTime],
                'duration': [job.completion_time - job.submission_time]
            }))
        df = df.set_index('job_id')
            
        # Get the query-id for each job if it exists
        if self.existsSQL:
            for qid, row in self.sqlData.iterrows():
                for jid in row['job_ids']:
                    df.at[jid,'sql_id'] = qid        

        logging.info('Aggregated job data [%.2f]' % (time.time()-t1))            
        
        self.jobData = df
        
    # This method collects all of the task data for this application into
    # and dataframe
    def getAllTaskData(self, appobj):

        t1 = time.time()
        refTime = appobj.start_time


        tid2qid = defaultdict(lambda:[])
        if self.existsSQL:
            for qid, query in self.sqlData.iterrows():
                for tid in query.task_ids:
                    tid2qid[tid] = qid


        
        task_id    = []
        sql_id     = []
        job_id     = []
        exec_id    = []
        start_time = []
        end_time   = []
        duration   = []

        input_mb   = []
        remote_mb_read = []
        memory_bytes_spilled = []
        disk_bytes_spilled = []

        executor_run_time = []
        executor_deserialize_time = []
        result_serialization_time = []
        gc_time = []
        scheduler_delay = []
        fetch_wait_time = []
        shuffle_write_time = []
        local_read_time = []
        compute_time = []
        task_compute_time = []
        input_read_time = []
        output_write_time = []

        stage_id   = []
        for jid, job in appobj.jobs.items():
            for sid, stage in job.stages.items():
                for task in stage.tasks:
                    sql_id.append(tid2qid[task.task_id])
                    stage_id.append(sid)
                    job_id.append(jid)
                    task_id.append(     task.task_id)
                    exec_id.append(int( task.executor_id))
                    start_time.append(  task.start_time   - refTime)
                    end_time.append(    task.finish_time - refTime)
                    duration.append(    task.finish_time - task.start_time)

                    input_mb.append(             task.input_mb)
                    remote_mb_read.append(       task.remote_mb_read)
                    memory_bytes_spilled.append( task.memory_bytes_spilled)
                    disk_bytes_spilled.append(   task.disk_bytes_spilled)

                    executor_run_time.append(         task.executor_run_time)
                    executor_deserialize_time.append( task.executor_deserialize_time)
                    result_serialization_time.append( task.result_serialization_time)
                    gc_time.append(                   task.gc_time)
                    scheduler_delay.append(           task.scheduler_delay)
                    fetch_wait_time.append(           task.fetch_wait)
                    shuffle_write_time.append(        task.shuffle_write_time)
                    local_read_time.append(           task.local_read_time)
                    compute_time.append(              task.compute_time_without_gc())
                    task_compute_time.append(         task.task_compute_time())
                    input_read_time.append(           task.input_read_time)
                    output_write_time.append(         task.output_write_time)


        df = pd.DataFrame({
            'task_id'    : task_id,
            'sql_id'     : sql_id,
            'job_id'     : job_id,
            'stage_id'   : stage_id,
            'executor_id': exec_id,
            'start_time' : start_time, 
            'end_time'   : end_time,
            'duration'   : duration,

            'input_mb'   : input_mb,
            'remote_mb_read': remote_mb_read,
            'memory_bytes_spilled': memory_bytes_spilled,
            'disk_bytes_spilled'  : disk_bytes_spilled,

            'executor_run_time': executor_run_time,
            'executor_deserialize_time': executor_deserialize_time,
            'result_serialization_time': result_serialization_time,
            'gc_time' : gc_time,
            'scheduler_delay': scheduler_delay,
            'fetch_wait_time': fetch_wait_time,
            'shuffle_write_time': shuffle_write_time,
            'local_read_time': local_read_time,
            'compute_time': compute_time,
            'task_compute_time': task_compute_time,
            'input_read_time' : input_read_time,
            'output_write_time': output_write_time
        })      
        df = df.sort_values(by='task_id')
        df = df.set_index('task_id')

        logging.info('Aggregated task data [%.2fs]' % (time.time()-t1))            

        self.taskData = df
    
    def getAllStageData(self, appobj):
        t1 = time.time()
        df = pd.DataFrame([])       


        sid2qid = defaultdict(lambda:[])
        if self.existsSQL:
            for qid, query in self.sqlData.iterrows():
                for sid in query.stage_ids:
                    sid2qid[sid] = qid
        
        stage_id    = []
        query_id    = []
        job_id      = []
        start_time  = []
        end_time    = []
        duration    = []
        num_tasks   = []
        task_time   = []

        input_mb    = []
        remote_mb_read = []
        memory_bytes_spilled = []
        disk_bytes_spilled = []

        executor_run_time = []
        executor_deserialize_time = []
        result_serialization_time = []
        gc_time = []
        scheduler_delay = []
        fetch_wait_time = []
        local_read_time = []
        compute_time = []
        task_compute_time = []
        input_read_time = []
        output_write_time = []

        task_ids = []
        parents  = []

        for jid, job in appobj.jobs.items():
            for sid, stage in job.stages.items():
                
                # Get the task-ids for this stage
                taskids = []
                for task in stage.tasks:
                    taskids.append(task.task_id)
                
                # Get the task data for this stage
                taskData = self.taskData.loc[taskids]
                
                
                stage_id.append(sid)
                query_id.append(sid2qid[sid])
                job_id.append(jid)
                task_ids.append(taskids)
                parents.append(appobj.dag.parents_dag_dict[sid])

                start_time.append(  taskData['start_time'].min())
                end_time.append(    taskData['end_time'  ].max())
                duration.append(    taskData['end_time'].max() - taskData['start_time'].min())
                num_tasks.append(len(taskData.index))
                task_time.append(   taskData['duration'].sum())

                input_mb.append(             taskData['input_mb'].sum())
                remote_mb_read.append(       taskData['remote_mb_read'].sum())
                memory_bytes_spilled.append( taskData['memory_bytes_spilled'].sum())
                disk_bytes_spilled.append(   taskData['disk_bytes_spilled'].sum())

                executor_run_time.append(         taskData['executor_run_time'].sum())
                executor_deserialize_time.append( taskData['executor_deserialize_time'].sum())
                result_serialization_time.append( taskData['result_serialization_time'].sum())
                gc_time.append(                   taskData['gc_time'].sum())
                scheduler_delay.append(           taskData['scheduler_delay'].sum())
                fetch_wait_time.append(           taskData['fetch_wait_time'].sum())
                local_read_time.append(           taskData['local_read_time'].sum())
                compute_time.append(              taskData['compute_time'].sum())
                task_compute_time.append(         taskData['task_compute_time'].sum())
                input_read_time.append(           taskData['input_read_time'].sum())
                output_write_time.append(         taskData['output_write_time'].sum())

        df = pd.DataFrame({
            'stage_id': stage_id,
            'query_id': query_id,
            'job_id'  : job_id,
            'task_ids': task_ids,
            'parents' : parents,
            
            'start_time': start_time,
            'end_time'  : end_time,
            'duration'  : duration,
            'num_tasks' : num_tasks,
            'task_time' : task_time,

            'input_mb'  : input_mb,
            'remote_mb_read' : remote_mb_read,
            'memory_bytes_spilled' : memory_bytes_spilled,
            'disk_bytes_spilled'   : disk_bytes_spilled,

            'executor_run_time' : executor_run_time,
            'executor_deserialize_time': executor_deserialize_time,
            'result_serialization_time': result_serialization_time,
            'gc_time': gc_time,
            'scheduler_delay': scheduler_delay,
            'fetch_wait_time': fetch_wait_time,
            'local_read_time': local_read_time,
            'compute_time':    compute_time,
            'task_compute_time': task_compute_time,
            'input_read_time':   input_read_time,
            'output_write_time': output_write_time
        })
                 
        logging.info('Aggregated stage data [%.2fs]' % (time.time()-t1))            
        df = df.set_index('stage_id')
        self.stageData = df
    
    def assignTasksToCores(self):
        t1 = time.time()
        
        lastCoreID = 0
        task_ids = []
        core_ids = []
        for xid in np.sort(self.taskData['executor_id'].unique()):
            
            
            tasks = self.taskData.loc[self.taskData['executor_id'] == xid] # Get all tasks for this xid
            tasks = tasks.sort_values(by='start_time') # Sort by start time
            
            
            execCores = self.executorData.loc[xid]['cores']
            coreIDs = np.arange(lastCoreID+1,lastCoreID+1+execCores)
            lastCoreID += execCores
            recentEndTimes = np.array([0.0]*execCores)

            for tid, task in tasks.iterrows():
                
                # Find the earliest slot and place this task there
                idx = np.argmin(recentEndTimes)
                if task['start_time']>= recentEndTimes[idx]-0.005:
                    task_ids.append(tid)
                    core_ids.append(coreIDs[idx])
                    recentEndTimes[idx] = task['end_time']
                else:
                    task_ids.append(tid)
                    core_ids.append(-1)
                                
        df = pd.DataFrame({
            'task_id': task_ids,
            'core_id': core_ids
        })
        
        df = df.sort_values(by='task_id')
        df = df.set_index('task_id')
        
        logging.info('Assigning tasks to cores [%.2fs]' % (time.time()-t1))            

        # Join by task_id (index column)
        self.taskData = self.taskData.join(df)

    # This method will plot stages at the task level
    def plotTasks(self, stage_ids=None, sql_id=None, xlim=None, savepath=None, legend='on'):
    
        if sql_id != None:
            stage_ids = np.sort(self.taskData[self.taskData.sql_id == sql_id]['stage_id'].unique())
            #stage_ids = np.sort(self.sqlData.loc[sql_id].stage_ids)
        elif stage_ids == None:
            logging.info('No stages or query specified, running on all stages')
            #stage_ids = np.sort(self.stageData.index.values)
            stage_ids = np.sort(self.taskData.stage_id.unique())
            
        # Set up the figure window
        fig = plt.figure(figsize=(12,15))
        ax1 = fig.add_axes([0, 0.0, 1.0, 0.75]) # Axis for time bar plot
        ax2 = fig.add_axes([0, 0.8, 1.0, 0.1]) # Axis for time bar plot
        plt.rc('font',size=12)
        
        # Aggregate all the tasks for the input stages
        # Also create a unique color for each stage
        task_ids = []
        legend_elements = []
        colormap = {}
        palette = self.generateColors(len(stage_ids))
        idx = 0
        for sid, col in zip(stage_ids, palette):
            legend_elements.append(Patch(facecolor=col, edgecolor='black', label=str(sid)))
            colormap[sid] = idx
            idx +=1
        tasks = self.taskData[self.taskData.stage_id.isin(stage_ids)]

        # If the user put in a query id then include the query time
        # as another rectangle. Also reference all task start & end time to the start 
        # of the query
        if (sql_id != None) and (self.existsSQL):
            queryDat = self.sqlData.loc[sql_id]
            refTime = queryDat['start_time']
            yroot = 0
            dt    = queryDat['end_time'] - queryDat['start_time']
            ax1.add_patch(rect((0,yroot), dt ,1, facecolor='black',zorder=0))
            ax1.annotate('sql: ' + queryDat['description'] \
                         + ' t0=' + "{:0.2f}".format(queryDat['start_time']),
                         (dt*0.4, 0.1), color='white', weight='bold', fontsize=10)
            xlims = [0, dt]
        else:
            refTime = 0
            xlims = [tasks['start_time'].min(), tasks['end_time'].max()]


        if xlim != None:
            xlims = xlim
            
        # Print a  warning if not all of the tasks could be placed due to task overlap
        failedPlacements = tasks[tasks['core_id']==-1]
        #if len(failedPlacements)>0:
        #    print('Warning, the following tasks could not be placed due to core overlap:')
        #    display(failedPlacements)

        # Loop over all tasks and plot them as rectangles
        for tid, task in tasks.iterrows():
            xroot = task['start_time'] - refTime
            yroot = task['core_id'] 
            dt    = task['duration']
            color = palette[colormap[int(task['stage_id'])]]
            ax1.add_patch(rect((xroot, yroot), dt, 1, facecolor=color, edgecolor='black',zorder=-1))
            
        # Add executor data if the executorData dataframe exists
        if self.existsExecutors:
            cores = 1
            for xid, executor in self.executorData.iterrows():
                ax1.plot(xlims, [cores,cores],color='black')
                ax1.annotate(' E' + str(xid), (xlims[1], cores + float(executor['cores'])/2))
                cores += executor['cores']
            ax1.plot(xlims, [cores,cores],color='black')
            
    
            
            yticks = np.arange(1, cores+1, self.executorData.loc[1].cores)
            ax1.set_yticks(yticks+0.5)
            ax1.set_yticklabels(yticks.astype(int))
            
            
        # Set axes and legend
        ax1.set_xlim(xlims)
        ax1.set_ylim([0,tasks['core_id'].max()+2])
        ax1.set_xlabel('Time [s]', fontsize=16)
        ax1.set_ylabel('Core-ID', fontsize=16)
        if legend == 'on':
            ax1.legend(handles=legend_elements, loc="lower center", ncol=10, bbox_to_anchor=(0.5, -0.12))
        
        
        # Plot the number of active cores
        t = np.linspace(xlims[0],xlims[1], 2000)
        coresInUse = 0*t
        for idx, task in tasks.iterrows():
            idxs = np.logical_and(t>task['start_time']-refTime, t<task['end_time']-refTime)
            coresInUse[idxs] += 1
        ax2.plot(t, coresInUse, linewidth=2)
        ax2.set_xlim(xlims)
        ax2.set_ylabel('Active Cores',fontsize=14)
        
        
        
        if savepath != None:
            #plt.tight_layout()
            plt.savefig(savepath, bbox_inches='tight')
        plt.show()
        
    def generateColors(self, ncolors=None, colorsPerCycle=6, rfrac=0.2):
        colors = []
        hue  = 0
        dhue = 1.0/(colorsPerCycle - rfrac)
        for _ in range(ncolors):
            colors.append(colorsys.hsv_to_rgb(hue, 0.5, 0.9))
            hue += dhue
        return colors

    def plotStages(self, stage_ids=None, sql_id=None, xlim=None, savepath=None):

        if sql_id != None:
            sqlData = self.sqlData.loc[sql_id]
            stage_ids  = sqlData.stage_ids
            refTime = sqlData.start_time
            xlims   = [sqlData.start_time-refTime, sqlData.end_time-refTime]
            stageData = self.stageData.loc[stage_ids]
        elif stage_ids != None:
            stageData = self.stageData.loc[stage_ids]
            xlims = [stageData.start_time.min(), stageData.end_time.max()]
            refTime = 0
        else:
            stage_ids = self.stageData.index.values
            stageData = self.stageData.loc[stage_ids]
            xlims = [stageData.start_time.min(), stageData.end_time.max()]
            refTime = 0

        if xlim != None:
            xlims = xlim


        fig = plt.figure(figsize=(12,8))
        ax = fig.add_axes([0.1, 0.5, 1.0, 0.6]) # Axis for time bar plot
        totalTime = xlims[1]-xlims[0]

        idx=0
        for sid, stage in stageData.iterrows():
            xroot   = stage['start_time'] - refTime
            dt      = stage['duration']
            ax.add_patch(rect((xroot, idx+1), dt, 1, facecolor='teal', edgecolor='black'))

            curStr = 's'   + str(sid) + \
                     ', j' + str(stage['job_id']) + \
                     ', '  + 't=' + "{:0.2f}".format(dt)

            if stage['end_time']-refTime-xlims[0] < totalTime*2/3:
                ax.annotate('---- ' + curStr,(xroot+dt, idx+1.2), color='black', fontsize=10,ha='left')
            else:
                ax.annotate(curStr + ' ----',(xroot   , idx+1.2), color='black', fontsize=10,ha='right')
            idx+=1


        ax.set_xlabel('Time [s]')
        ax.set_xlim(xlims)
        ax.set_ylim([0,idx+2])
        ax.set_title('stage-level breakdown')

        if sql_id != None:
            dt = sqlData.duration
            ax.add_patch(rect((0,  0),    dt   , 1, edgecolor='black'))
            ax.annotate('sql: ' + sqlData.description \
                         + ' t0=' + "{:0.2f}".format(sqlData.start_time),
                         (dt*0.4, 0.1), color='white', weight='bold', fontsize=10)

        if savepath != None:
            #plt.tight_layout()
            plt.savefig(savepath, bbox_inches='tight')
        plt.show()
        
    def printQuery(self, sql_id):
        
        stageData = self.stageData.loc[self.sqlData.loc[sql_id].stage_ids]
        taskData = pd.DataFrame([])
        for sid, stage in stageData.iterrows():
            taskData = taskData.append(self.taskData[self.taskData['stage_id']==sid])

        print(self.sqlData.loc[sql_id])
        display(stageData)
        display(taskData)
        
    def getQueryData(self, sql_id=None):
        
        sqlData   = self.sqlData.loc[sql_id]
        stageData = self.stageData.loc[self.sqlData.loc[sql_id].stage_ids]
        taskData = pd.DataFrame([])
        for sid, stage in stageData.iterrows():
            taskData = taskData.append(self.taskData[self.taskData['stage_id']==sid])

        return sqlData, taskData, stageData
    
    
    def save(self, savedir=None, compress=False):
        t1 = time.time()
        # Convert all dataframes into json and aggregate
        # into a single dict
        saveDat = {}
        if hasattr(self, 'jobData')  : saveDat['jobData']   = self.jobData.reset_index().to_dict() 
        if hasattr(self, 'stageData'): saveDat['stageData'] = self.stageData.reset_index().to_dict()
        if hasattr(self, 'taskData') : saveDat['taskData']  = self.taskData.reset_index().to_dict()
        if self.existsSQL:
            saveDat['sqlData'] = self.sqlData.reset_index().to_dict()
        if self.existsExecutors:
            saveDat['executors'] = self.executorData.reset_index().to_dict()

        saveDat['metadata'] = {
            'existsSQL'      : self.existsSQL,
            'existsExecutors': self.existsExecutors}

        savepath  = f'{savedir}.json'
        if compress == False:
            with open(savepath,'w') as fout:
                fout.write(json.dumps(saveDat))
        elif compress == True:
            with gzip.open(savepath + '.gz','w') as fout:
                fout.write(json.dumps(saveDat).encode('ascii'))

        logging.info('Saved object to: %s [%.2f]' % (savepath, (time.time()-t1)))

    def load(self, filepath=None):
        t1 = time.time()
        if '.gz' in filepath:
            with gzip.open(filepath, 'r') as fin:
                saveDat = json.loads(fin.read().decode('ascii'))
        else:
            with open(filepath, 'r') as fin:
                saveDat = json.loads(fin.read())



        self.existsSQL       = saveDat['metadata']['existsSQL']
        self.existsExecutors = saveDat['metadata']['existsExecutors']

        if 'jobData' in saveDat:   self.jobData   = pd.DataFrame.from_dict(saveDat['jobData']  ).set_index('job_id')
        if 'stageData' in saveDat: self.stageData = pd.DataFrame.from_dict(saveDat['stageData']).set_index('stage_id')
        if 'taskData' in saveDat:  self.taskData  = pd.DataFrame.from_dict(saveDat['taskData'] ).set_index('task_id')

        if self.existsSQL:
            self.sqlData      = pd.DataFrame.from_dict(saveDat['sqlData']).set_index('sql_id')
        if self.existsExecutors:
            self.executorData = pd.DataFrame.from_dict(saveDat['executors']).set_index('executor_id')  

        logging.info('Loaded object from: %s [%.2f]' % (filepath, (time.time()-t1)))

        return self