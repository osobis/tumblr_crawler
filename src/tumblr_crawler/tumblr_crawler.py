from __future__ import with_statement 

import tumblr
import threading
import time
import sys
import ConfigParser
import logging
import urllib2
import os
import Queue
from cutils import get_from_config
from urlparse import urlparse
    
CONTEXT_LOCK = threading.Lock()
QUEUE_MAX_SIZE = 50


class PhotoObject(object):
    
    def __init__(self, owner, caption, **kwargs):
        
        self.owner = owner
        self.caption = caption
        self.url = None
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.__prev_key = -1
            
    def set_url(self, key, url):
        if key > self.__prev_key:
            self.url = url
            self.__prev_key = key
            
    def __repr__(self):
        return '%r' % self.__dict__
            
    
class TumblrImageCrawler(threading.Thread):
    """Crawls for images under given name account"""
    
    def __init__(self, name, task_queue, context,  email = None, password = None, tags = None, max=50, limit=0):
        
        self.__log = logging.getLogger(self.__class__.__name__)
        self.__task_queue = task_queue
        self.__context = context
        self.__name = name
        self.__email = email
        self.__password = password
        self.__tags = tags
        self.__max = max
        self.__limit = limit
        self.__api = tumblr.Api(name, email, password, None, None, tags, None)
        threading.Thread.__init__(self)
        
    def __process_search_results(self, results):
        """
        @params: results interator from tumblr.api
        @returns: photo_data as dict, count as total number of results
        """
        photo_data = list()
        count = 0
        for result in results:
            if isinstance(result, dict):
                photo_caption = result.get('photo-caption')
                if photo_caption:
                    photo_obj = PhotoObject(owner = self.__name, caption = photo_caption)
                    for key in result.keys():
                        if key.startswith('photo-url-'):
                            try:
                                new_key = int(key.split('photo-url-')[-1])
                                photo_obj.set_url(new_key, result[key])
                            except ValueError:
                                self.__log.warning('Ooops! Strange photo-url key: %r', key)
                    photo_data.append(photo_obj)
            else:
                self.__log.warning('Result: %r not a dict', result)
            count += 1
        return photo_data, count                
        
    def run(self):
        """Run method"""
        start = 0
        while True:
            try:
                self.__log.info('Starting with: %d; max: %d for name: %r with limit: %d', start, self.__max, self.__name, self.__limit)
                results = self.__api.read(start = start, max = self.__max)
                photo_data, count = self.__process_search_results(results)
                
                if count == 0:
                    self.__log.info('No more results for name: %r', self.__name)
                    break
                       
                if photo_data:      
                    self.__log.debug('Putting photo_data on the queue for name: %r', self.__name) 
                    self.__task_queue.put(photo_data, block=True)
                                
                start += self.__max
                
                if self.__limit > 0:
                    if start >= self.__limit*self.__max:
                        self.__log.info('Reached the limit of %d batches' % self.__limit)
                        break
                
            except StandardError, error:
                self.__log.error('Error: %r', error)
                
            self.__log.info('Sleeping for name: %r', self.__name)
            time.sleep(0.2)
            
        with CONTEXT_LOCK:
            self.__context['count_done_crawler'] += 1
     
            
class PhotoSaveThread(threading.Thread):
    """Thread to save photos to the specified folder"""
    
    def __init__(self, config, task_queue, context):
        
        self.__config = config
        self.__log = logging.getLogger(self.__class__.__name__)
        self.__task_queue = task_queue
        self.__context = context
        self.__save_to_dir = get_from_config(self.__config, 'TUMBLR', 'save_to_dir')
        threading.Thread.__init__(self)
        
    def __get_or_create_dir(self, owner):
        path = os.path.join(self.__save_to_dir, owner)
        if os.path.isdir(path):
            return path
        else:
            os.makedirs(path)
            return path
    
    def __save_photo(self, photo_obj):
        
        if not photo_obj.url:
            self.__log.warning('Missing URL for photo: %r, owner: %r', photo_obj.caption, photo_obj.owner)
            return False
        
        file_name = urlparse(photo_obj.url).path.strip('/')
        if file_name.endswith('.jpg') or file_name.endswith('.gif') or file_name.endswith('.png'):
            try:
                url_pointer = urllib2.urlopen(photo_obj.url)
                photo_path = os.path.join(self.__get_or_create_dir(photo_obj.owner), file_name)
                file(photo_path, 'wb').write(url_pointer.read())
                self.__log.info('URL: %r saved to %r', photo_obj.url, photo_path)
                return True
            except StandardError, error:
                self.__log.error('Error saving photo: %r. Error: %r', photo_obj.url, error)
        return False  
        
    def run(self):
        count_saved = 0
        while True:
            try:
                if not self.__context.get('crawlers_stopped', False):
                    photo_data = self.__task_queue.get(block=True)
                    self.__log.info('Received %d photo_data from the queue', len(photo_data))
                else:
                    photo_data = self.__task_queue.get(block=True, timeout=5)
                    self.__log.info('Received %d photo_data from the timeout queue', len(photo_data))
                    
                for photo_obj in photo_data:
                    if self.__save_photo(photo_obj):
                        count_saved += 1
                self.__task_queue.task_done()
            except Queue.Empty:
                with CONTEXT_LOCK:
                    self.__context['saving_done'] = True
                    break
            self.__log.info('Saved %d photos so far', count_saved)
            
    
class TumblrService(object):
    
    def __init__(self, config_file):
        
        self.__config_file = config_file
        self.__config = ConfigParser.SafeConfigParser()
        self.__config.read(config_file)
        self.__get_logging()
        self.__me_name = get_from_config(self.__config, 'TUMBLR', 'me_name')
        self.__me__pasword = get_from_config(self.__config, 'TUMBLR', 'me_password')
        self.__me_mail = get_from_config(self.__config, 'TUMBLR', 'me_email')
        self.__me_max = get_from_config(self.__config, 'TUMBLR', 'me_max')
        self.__acounts = [item.strip() for item in get_from_config(self.__config, 'TUMBLR', 'accounts').split(',')]
        self.__limit = get_from_config(self.__config, 'TUMBLR', 'limit', 0)
        self.__context = {'count_done_crawler': 0}
        self.__task_queue = Queue.Queue(QUEUE_MAX_SIZE)
        self.__image_crawlers = list()
        for account in self.__acounts:
            self.__image_crawlers.append(TumblrImageCrawler(task_queue = self.__task_queue, 
                                                            context = self.__context,
                                                            name = account, 
                                                            max = self.__me_max, 
                                                            limit = self.__limit))
        self.__save_thread = PhotoSaveThread(self.__config, self.__task_queue, self.__context)
        self.__log.info('I am: %r', self.__class__.__name__)
        
    def __get_logging(self):
        
        logging.basicConfig(level = getattr(logging, self.__config.get('TUMBLR', 'logging_level')),
                            format = '%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s')
        self.__log = logging.getLogger(self.__class__.__name__)
        
        
    def __repr__(self):
        return "%r" % self.__dict__
        
        
    def run(self): 
        self.__log.info('Start running crawlers..')
        for thread in self.__image_crawlers:
            thread.start()
            
        self.__log.info('Start saving thread..')
        self.__save_thread.start()
        
        while True:
            self.__log.info('Context: %r', self.__context)
            with CONTEXT_LOCK:
                if  self.__context.get('count_done_crawler') >= len(self.__acounts):
                    self.__context['crawlers_stopped'] = True
                    queue_length = self.__task_queue.qsize()
                    self.__log.info('Queue size: %d', queue_length)
                    if queue_length == 0:
                        if self.__context.get('saving_done', False):
                            self.__log.info('Saving done.')
                            break
            time.sleep(1)
                  
        self.__log.info('All Done.')  
        sys.exit(0)
           
            
def main(config_file_path):
    
    tumblr_service = TumblrService(config_file_path)
    tumblr_service.run()
    
            
if __name__ == '__main__':
    
    if len(sys.argv) < 2:
        print ("usage: python tumblr_crawler.py <config>")
    else:
        main(sys.argv[1])
            