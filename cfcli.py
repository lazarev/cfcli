#! /usr/bin/python
'''
Created on 05.12.2011

@author: Lazarev
'''
import os, sys
import cloudfiles
import threading
import Queue
from datetime import datetime
import logging
import argparse

logger          = logging.getLogger('cfcli')
console         = logging.StreamHandler()
logger.addHandler(console)
finishFlag      = False
workQueue       = None
connectionPool  = None
containerName   = None
dropped         = 0
bogus           = False

class UploadThread (threading.Thread):
    def run(self):        
        logger.debug(self.name + ' online')
        connection = connectionPool.get()
        container = connection.get_container(containerName)
        while not finishFlag:
            try:
                task = workQueue.get(block=True, timeout=1)
                logger.info(self.name + ' execute: ' + unicode(task))
                
                #overcome problems in lower levels code
                tries = 10
                while tries > 0 and not bogus:
                    try:
                        obj = container.create_object(task['dst'])
                        obj.load_from_filename(task['src'])
                        logger.debug(self.name + ' task is done')
                        break
                    except :
                        logger.error(self.name + '('+ tries +' tries available)' + +' : ' + sys.exc_info()[0])
                        tries = tries - 1                                               
                        if (tries==0):
                            logger.error(self.name + ' task execution tries exceeded. Dropping task.')
                            dropped = dropped + 1                   
                
                workQueue.task_done()
            except Queue.Empty:
                pass
        connectionPool.put(connection)            
        logger.debug(self.name + ' offline')

if __name__ == '__main__':    
    try:
        parser = argparse.ArgumentParser(description='Upload directory tree into Rackspace Cloud Files store.')
        parser.add_argument('-u', metavar = 'username',  help='account name', required=True)
        parser.add_argument('-k', metavar = 'apiKey',    help='rack space API access key', required=True)
        parser.add_argument('-c', metavar = 'container', help='target container')
        parser.add_argument('-s', metavar = 'source',    help='source path to upload (current by default)', default='.')
        parser.add_argument('-p', metavar = 'prefix',    help='path prefix for objects to create', default='')
        parser.add_argument('-t', metavar = 'number',    help='number of parallel upload processes (10 by default)', default=10, type=int)
        parser.add_argument('-d', metavar = 'level',     help='debug level', type=int, default=logging.INFO)
        parser.add_argument('-f', metavar = 'filename',  help='write logs to file')
        parser.add_argument('-n',                        help='use service net (False by default)', default=False, type=bool)
        parser.add_argument('-b',                        help='don\'t upload anything actually. (For test purposes)', default=False, type=bool)
        
        args = parser.parse_args()
        
        logger.setLevel(args.d)
        
        if args.f: logger.addHandler(logging.FileHandler(args.f))
        
        connectionPool  = cloudfiles.ConnectionPool(args.u, args.k, servicenet=args.n)
        workQueue       = Queue.Queue(args.t*3)
        containerName   = args.c
        path            = args.s
        prefix          = args.p
        bogus           = args.b
        totalFiles      = 0

        threads = []
        beginTime = datetime.now() 
       
        # Init threads
        for i in range(0,args.t):
            thread = UploadThread()
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        for filePath, dirs, files in os.walk(path, followlinks=False):
            for curFile in files:
                totalFiles = totalFiles + 1
                dir = os.path.relpath(filePath, path)
                task = {'src' : os.path.join(filePath, curFile),
                        'dst' : os.path.join(prefix, dir, curFile)}                
                logger.debug('Main thread: Put task for workers: ' + unicode(task))
                workQueue.put(task)
        
        logger.info('Main thread: There is no files more. Wait for worker threads.')
        workQueue.join()
        finishFlag = True
        logger.info('Work is done at: ' + unicode(datetime.now() - beginTime))
        logger.info('        threads: ' + args.t)
        logger.info('  dropped tasks: ' + dropped)
        logger.info(' files uploaded: ' + totalFiles)        
        
#        for thread in threads:
#            if (thread.isAlive()): thread.join() 
        
    except Exception as error:
        logger.error(unicode(error))
