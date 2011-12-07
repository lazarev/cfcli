#! /usr/bin/python
'''
Created on 05.12.2011

@author: Lazarev
'''
import os
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

class UploadThread (threading.Thread):
    def run(self):        
        logger.debug(self.name + ' online')
        connection = connectionPool.get()
        container = connection.get_container(containerName)
        while not finishFlag:
            try:
                task = workQueue.get(block=True, timeout=1)
                object = container.create_object(task['dst'])
                logger.info(self.name + ' execute: ' + unicode(task))
                object.load_from_filename(task['src'])                
                logger.debug(self.name + ' task is done')
                workQueue.task_done()
            except Queue.Empty:
                pass
        connectionPool.put(connection)            
        logger.debug(self.name + ' offline')

if __name__ == '__main__':    
    try:
        parser = argparse.ArgumentParser(description='Upload directory tree into Rackspace Cloud Files store.')
        parser.add_argument('username',                 help='account name')
        parser.add_argument('apiKey',                   help='rack space API access key')
        parser.add_argument('container',                help='target container')
        parser.add_argument('-s', metavar = 'source',   help='source path to upload (current by default)', default='.')
        parser.add_argument('-p', metavar = 'prefix',   help='path prefix for objects to create', default='')
        parser.add_argument('-t', metavar = 'number',   help='number of parallel upload processes (10 by default)', default=10, type=int)
        parser.add_argument('-d', metavar = 'level',    help='debug level', type=int, default=logging.INFO)
        parser.add_argument('-n', metavar = 'source',   help='use service net (False by default)', default=False, type=bool)
        
        args = parser.parse_args()
        
        print(args)

        logger.setLevel(args.d)
        
        connectionPool  = cloudfiles.ConnectionPool(args.username, args.apiKey, servicenet=args.n)
        workQueue       = Queue.Queue(args.t*3)
        containerName   = args.container
        path            = args.s
        prefix          = args.p

        threads = []
        beginTime = datetime.now() 
       
        # Init threads
        for i in range(0,3):
            thread = UploadThread()
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        for root, dirs, files in os.walk(path, followlinks=False):
            for curFile in files:
                if not path == root:
                    dir = root[len(path)+1:len(root)]+'/'
                else:
                    dir = ''
                task = {'src' : os.path.join(root, curFile),
                        'dst' : os.path.join(prefix, dir, curFile)
                }
                logger.debug('Put task for workers: ' + unicode(task))
                workQueue.put(task)
        
        workQueue.join()
        finishFlag = True
        logger.info('Work is done at ' + unicode(datetime.now() - beginTime))
        
        for thread in threads:
            if (thread.isAlive()): thread.join() 
        
    except Exception as error:
        logger.error(unicode(error))