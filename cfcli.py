#! /usr/bin/python
'''
Created on 05.12.2011

@author: Lazarev
'''
import os
import sys
import cloudfiles
import threading
import Queue
from datetime import datetime

class UploadThread (threading.Thread):
    def run(self):        
        print(self.name + ' online')
        connection = connectionPool.get()
        container = connection.get_container(containerName)
        while not finishFlag:
            try:
                task = workQueue.get(block=True, timeout=1)
                object = container.create_object(task['upload_path'])
                object.load_from_filename(task['file_path'])                
                print(task)
                workQueue.task_done()
            except Queue.Empty:
                pass
        connectionPool.put(connection)            
        print(self.name + ' offline')

finishFlag = False

workQueue = Queue.Queue(10)
connectionPool  = cloudfiles.ConnectionPool(username, apiKey)

if __name__ == '__main__':    
    try:
        #username          = sys.argv[1]
        #apiKey            = sys.argv[2]
        #container         = sys.argv[3]
        #command           = sys.argv[4]
        #threadsNumber     = sys.argv[5]
        #threadsNumber     = sys.argv[6]
        
        username        = 'lazarev'
        apiKey          = '7793a117936fd93313f30dedf1281550'
        containerName   = 'media'

        
        path            = len(sys.argv) > 1 and sys.argv[1] or '.'
        threads = []
        beginTime = datetime.now() 
        
        # Init threads
        for i in range(0,3):
            thread = UploadThread()
            thread.start()
            threads.append(thread)
        
        for root, dirs, files in os.walk(path, followlinks=False):
            for curFile in files:
                if not path == root:
                    dir = root[len(path)+1:len(root)]+'/'
                else:
                    dir = ''
                task = {'upload_path':   root + '/' + curFile,
                        'file_path'  :   dir+curFile,
                }
                print('Run: ' + str(task))
                workQueue.put(task)
        
        workQueue.join()
        finishFlag = True
        print('Work is done at ' + str(datetime.now() - beginTime))
        
    except Exception as error:
        print(unicode(error))
        
