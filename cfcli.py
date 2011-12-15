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
import traceback

logger          = logging.getLogger('cfcli')
logger.addHandler(logging.StreamHandler())

class WorkerThread (threading.Thread):
    def __init__(self, taskQueue, pool):
        self.taskQueue = taskQueue
        self.pool = pool
        threading.Thread.__init__(self)

    def run(self):        
        logger.debug(self.name + ' online')
        while not self.taskQueue.finishFlag:
            try:                 
                task = self.taskQueue.get(block=True, timeout=1)                
                logger.debug(self.name + ' execute: ' + unicode(task))
                if not self.taskQueue.bogus:
                    connection = self.pool.get()
                    try:
                        callback = task['callback']
                        del(task['callback'])
                        callback(connection, **task)
                        logger.info('%s task finished successfully: %s' % (self.name, unicode(task)))
                    except Exception:
                        logger.error('%s task finished with error: %s' % (self.name, traceback.format_exc()))
                    self.pool.put(connection)                
                self.taskQueue.task_done()
            except Queue.Empty:
                logger.debug('%s waiting' % self.name)
        logger.debug(self.name + ' offline')
        
class TaskQueue(Queue.Queue):
    '''Abstraction for multithreaded commands'''
    finishFlag      = False

    def __init__(self, pool, number=1, bogus=False):
        '''
        @param number: number of worker threads to run
        '''
        self.bogus=bogus
        Queue.Queue.__init__(self, number*3)
        
        self.threads = [] 
        for i in range(0, number):
            thread = WorkerThread(self, pool)
            thread.daemon = True
            thread.start()
            self.threads.append(thread)
            
    def Finish(self):
        logger.debug('Waiting for queue is empty')
        self.join()
        self.finishFlag = True
        logger.debug('Waiting for worker threads to stop')
        for thread in self.threads:
            if (thread.isAlive()): 
                logger.debug('Waiting for: %s' % thread.name)
                thread.join()
    
def uploadFile(connection, source, destination, container):
    container_instance = connection.get_container(container)
    for tries in range(0, 10):
        try:
            obj = container_instance.create_object(destination)
            obj.load_from_filename(source)
            logger.debug('%s uploaded' % destination)
            return
        except Exception:
            logger.error('Upload file crashed on try %d with: %s' % (tries, traceback.format_exc()))    

def deleteFile(connection, container_name, file_name):
    logger.debug('Try to remove from %s : %s' % (container_name, file_name))
    container_instance = connection.get_container(container_name)
    container_instance.delete_object(file_name)
    logger.info('%s removed' % file_name)
    
def command_upload(args):
    # Parameters
    container       = args.container
    path            = args.source
    prefix          = args.prefix
   
    connectionPool  = cloudfiles.ConnectionPool(args.username, args.key, servicenet=args.n)
    queue = TaskQueue(connectionPool, args.threads, args.b)
    
    # Post tasks for workers
    for filePath, dirs, files in os.walk(path, followlinks=False):
        for curFile in files:
#            totalFiles = totalFiles + 1
            relDir = os.path.relpath(filePath, path)
            task = {'callback'    : uploadFile, 
                    'source'      : os.path.join(filePath, curFile),
                    'destination' : os.path.join(prefix, relDir, curFile),
                    'container'   : container }                
            logger.debug('Main thread: Put task for workers: ' + unicode(task))
            queue.put(task)
    
    queue.Finish()

def command_list(args):
    connection  = cloudfiles.Connection(args.username, args.key, servicenet=args.n)
    if args.detailed:
        result = connection.list_containers_info(args.max, args.marker)
        result = ["%s\t%s files\t%s bytes" % (unicode(item['name']), unicode(item['count']), unicode(item['bytes'])) for item in result] 
    else:
        result = connection.list_containers(args.max, args.marker)
    print '\n'.join(result)

def command_listobjects(args):
    connection  = cloudfiles.Connection(args.username, args.key, servicenet=args.n)
    container = connection.get_container(args.container)
    if args.detailed:
        result = container.list_objects_info(limit=args.max, marker=args.marker, path=args.path)
        result = ["%s\t%s\t%s\t%s\t%s" % 
                  (unicode(item['name']), 
                   unicode(item['content_type']), 
                   unicode(item['bytes']), 
                   unicode(item['last_modified']), 
                   unicode(item['hash'])) for item in result] 
    else:
        result = container.list_objects(limit=args.max, marker=args.marker, path=args.path)
    #TODO If i pass this output through grep code will be crashed with "UnicodeEncodeError: 'ascii' codec can't encode characters in position 18014-18017: ordinal not in range(128)"
    print u'\n'.join(result)

def command_create(args):
    connection  = cloudfiles.Connection(args.username, args.key, servicenet=args.n)
    connection.create_container(args.name, error_on_existing=True)
    print "Container: %s created successfully" % args.name 

def command_delete(args):
    container_name = args.container
    object_names = args.object
    connection  = cloudfiles.Connection(args.username, args.key, servicenet=args.n)
    
    if object_names:
        for name in object_names:
            deleteFile(connection, container_name, name)
    else:
        connectionPool  = cloudfiles.ConnectionPool(args.username, args.key, servicenet=args.n)
        queue = TaskQueue(connectionPool, args.threads, args.b)
        
        container = connection.get_container(container_name)
        for name in container.get_objects():
            queue.put({'callback': deleteFile, 'file_name' : unicode(name), 'container_name': container_name })
        
        queue.Finish()
        
        if len(container.get_objects()):
            print(container.get_objects())
        else:
            connection.delete_container(args.container)
            print "Container: %s deleted successfully" % args.container

if __name__ == '__main__':    
    
    options = argparse.ArgumentParser(description='Upload directory tree into Rackspace Cloud Files store.', add_help=False)
    options.add_argument('-u', '--username',
                        metavar = 'username',  
                        help='account name', 
                        required=True)
    options.add_argument('-k', '--key',          
                        metavar = 'apiKey',    
                        help='rack space API access key', 
                        required=True)
    options.add_argument('-t', '--threads',      
                        metavar = 'number',    
                        help='number of parallel upload processes (10 by default)', 
                        default=10, type=int)
    options.add_argument('-l', '--loglevel',     
                        metavar = 'level',     
                        help='Log level (default %d)' % logging.INFO, 
                        type=int, default=logging.INFO)
    options.add_argument('-f', '--logfilename',  
                        metavar = 'filename',  
                        help='write logs to file')
    options.add_argument('-n', 
                        help='use service net (False by default)', 
                        action='store_const', const=True)
    options.add_argument('-b',
                        help='don\'t do anything. (For debug purposes)', 
                        action='store_const', const=True, default=False)
    
    parser = argparse.ArgumentParser(description='Rackspace Cloud Files manipulation toolkit')
    subparsers = parser.add_subparsers( title='commands',
                                        description='valid commands',
                                        help='command --help')
    
    upload_parser = subparsers.add_parser('upload', 
        help='Upload files or directory tree into container', 
        parents=[options])
    upload_parser.add_argument('-p', '--prefix',       
                        metavar = 'prefix',    
                        help='path prefix for objects to create', 
                        default='')
    upload_parser.add_argument('container', 
        help='Destination container name')
    upload_parser.add_argument('source', help='''File or directory to 
        upload. File names in container would not get their path. 
        For ex. ./test/test.txt will produce text.txt in container.
        Use prefix if other behavior required.''')
    upload_parser.set_defaults(func=command_upload)
    
    list_parser = subparsers.add_parser('list',   
        help='List available containers', 
        parents=[options])
    list_parser.add_argument('-m', '--max',       
                        metavar = 'limit',    
                        help='number of results to return (up to 10000)', 
                        default=None,
                        type=int)
    list_parser.add_argument('-d', '--detailed',
                        help='return a list of Containers, including object count and size', 
                        action='store_const', const=True)
    list_parser.add_argument('marker', 
        help='return only results whose name is greater than "marker"', default=None, nargs='?')
    list_parser.set_defaults(func=command_list)
    
    listobjects_parser = subparsers.add_parser('listobjects',   
        help='List objects of specified container', 
        parents=[options])
    listobjects_parser.add_argument('-m', '--max',       
                        metavar = 'limit',    
                        help='number of results to return (up to 10000)', 
                        default=None,
                        type=int)
    listobjects_parser.add_argument('-d', '--detailed',
                        help='return a list of Containers, including object count and size', 
                        action='store_const', const=True)
    listobjects_parser.add_argument('-p', '--path',
                        help='return all objects in "path"', 
                        default=None)
    listobjects_parser.add_argument('container', 
        help='container name')
    listobjects_parser.add_argument('marker', 
        help='return only results whose name is greater than "marker"', default=None, nargs='?')
    listobjects_parser.set_defaults(func=command_listobjects)    
    
    create_parser = subparsers.add_parser('create', 
        help='Create container', parents=[options])
    create_parser.add_argument('name', 
        help='Container name')
    create_parser.set_defaults(func=command_create)
    
    delete_parser = subparsers.add_parser('delete', 
        help='Delete container', parents=[options])
    delete_parser.add_argument('container', 
        help='Container name to delete/delete from')
    delete_parser.add_argument('object', 
        help='One or more objects to delete form container', nargs='*')
    delete_parser.set_defaults(func=command_delete)
            
    args = parser.parse_args()

    if args.logfilename: logger.addHandler(logging.FileHandler(args.logfilename))
    if args.loglevel:    logger.setLevel(args.loglevel)
    
    beginTime = datetime.now()
    args.func(args)
    logger.info('Work done in: ' + unicode(datetime.now() - beginTime))
