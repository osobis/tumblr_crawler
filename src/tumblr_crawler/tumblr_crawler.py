from __future__ import with_statement 

import tumblr
import threading
import time
import sys
import ConfigParser
import logging
import urllib2
import os
from cutils import get_from_config
from urlparse import urlparse
    
PHOTO_DATA_LOCK = threading.Lock()
    
class TumblrImageCrawler(threading.Thread):
    
    def __init__(self, name, photo_data,  email = None, password = None, tags = None, max=50):
        
        self.__log = logging.getLogger(self.__class__.__name__)
        self.__photo_data = photo_data
        self.__name = name
        self.__email = email
        self.__password = password
        self.__tags = tags
        self.__max = max
        self.__api = tumblr.Api(name, email, password, None, None, tags, None)
        threading.Thread.__init__(self)
        
    def run(self):
        start = 0
        while True:
            try:
                self.__log.info('Starting with: %d; max: %d for name: %r', start, self.__max, self.__name)
                results = self.__api.read(start = start, max = self.__max)
                count = 0
                photo_data = dict()
                for result in results:
                    count += 1
                    if isinstance(result, dict):
                        photo_caption = result.get('photo-caption')
                        if photo_caption:
                            photo_data[photo_caption] = dict()
                            for key in result.keys():
                                if key.startswith('photo-url-'):
                                    try:
                                        new_key = int(key.split('photo-url-')[-1])
                                        photo_data[photo_caption][new_key] = result[key]
                                    except ValueError:
                                        self.__log.warning('Ooops! Strange photo-url key: %r', key)
                    else:
                        self.__log.warning('Result: %r not a dict', result)
                        
                if count == 0:
                    self.__log.info('No more results for name: %r', self.__name)
                    break
                
                with PHOTO_DATA_LOCK:       
                    self.__log.debug('Updating photo_data for name: %r', self.__name)       
                    self.__photo_data.update(photo_data)
                start += self.__max
            except StandardError, error:
                self.__log.error('Error: %r', error)
                
            self.__log.info('Sleeping for name: %r', self.__name)
            time.sleep(0.5)
            

    
    
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
        self.__save_to_dir = get_from_config(self.__config, 'TUMBLR', 'save_to_dir')
        self.__photo_data = dict()
        self.__image_crawlers = list()
        for account in self.__acounts:
            self.__image_crawlers.append(TumblrImageCrawler(photo_data=self.__photo_data, name = account, max = self.__me_max))
        self.__log.info('I am: %r', self.__class__.__name__)
        
    def __get_logging(self):
        
        logging.basicConfig(level = getattr(logging, self.__config.get('TUMBLR', 'logging_level')),
                            format = '%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s')
        self.__log = logging.getLogger(self.__class__.__name__)
        
    def __process_photo_data(self):
        self.__log.info('Start processing and downloading photos')
        result_dict = dict()
        for caption, photo_dict in self.__photo_data.items():
            sizes = photo_dict.keys()
            sizes.sort(reverse=True)
            if sizes:
                photo_dict[sizes[0]]
                result_dict[caption] = {sizes[0]: photo_dict[sizes[0]]}
            else:
                self.__log.warning('Strange! No photos found for caption: %r', caption)
        return result_dict
    
    def __save_photo(self, caption, url):
        file_name = urlparse(url).path.strip('/')
        if file_name.endswith('.jpg') or file_name.endswith('.gif') or file_name.endswith('.png'):
            try:
                url_pointer = urllib2.urlopen(url)
                photo_path = os.path.join(self.__save_to_dir, file_name)
                file(photo_path, 'wb').write(url_pointer.read())
                self.__log.info('URL: %r saved to %r', url, photo_path)
                return True
            except StandardError, error:
                self.__log.error('Error saving photo: %r. Error: %r', url, error)
        return False  
        
    def __repr__(self):
        return "%r" % self.__dict__
        
        
    def run(self): 
        self.__log.info('Start running crawlers..')
        for thread in self.__image_crawlers:
            thread.start()
        for thread in self.__image_crawlers:
            thread.join()
        self.__log.info('Finished collecting data')
        self.__log.info('Collected %d items to photo process', len(self.__photo_data))
        count_saved = 0
        for caption, url_dict in self.__process_photo_data().items():
            urls = url_dict.values()
            if urls:
                if self.__save_photo(caption, urls[0]):
                    count_saved += 1
        self.__log.info('Saved %d photos', count_saved)
          
            
            
def main(config_file_path):
    
    tumblr_service = TumblrService(config_file_path)
    tumblr_service.run()
    
            
if __name__ == '__main__':
    
    if len(sys.argv) < 2:
        print ("usage: python tumblr_crawler.py <config>")
    else:
        main(sys.argv[1])
            