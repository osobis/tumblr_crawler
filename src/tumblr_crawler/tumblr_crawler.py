from __future__ import with_statement 

import tumblr
import threading
import time
import os
import sys
import ConfigParser
import logging
import urllib2
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
            self.__log.info('Starting with: %d; max: %d', start, self.__max)
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
                self.__log.info('No more results')
                break
            with PHOTO_DATA_LOCK:              
                self.__photo_data.update(photo_data)
            start += self.__max
            self.__log.info('Sleeping a bit..')
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
        self.__photo_data = dict()
        self.__image_crawler = TumblrImageCrawler(photo_data=self.__photo_data, name = 'maximusboi')
        self.__log.info('I am: %r', self.__class__.__name__)
        
    def __get_logging(self):
        
        logging.basicConfig(level = getattr(logging, self.__config.get('TUMBLR', 'logging_level')),
                            format = '%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s')
        self.__log = logging.getLogger(self.__class__.__name__)
        
    def __process_photo_data(self):
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
    
    def __save_photo(self, url):
        file_name = urlparse(url).path.strip('/')
        url_pointer = urllib2.urlopen(url)
        file('/Users/skocle/Documents/photos/%s' % file_name, 'wb').write(url_pointer.read())
        self.__log.info('URL: %r saved', url)
            
        
    def __repr__(self):
        return "%r" % self.__dict__
        
        
    def run(self): 
        self.__image_crawler.start()
        self.__image_crawler.join()
        
        for key, value in self.__process_photo_data().items():
            print ("Caption: %s" % key)
            print ("URLs: %r" % value)
            print ('='*40)
            urls = value.values()
            if urls:
                self.__save_photo(urls[0])
          
            
            
def main(config_file_path):
    
    tumblr_service = TumblrService(config_file_path)
    tumblr_service.run()
    
    
            
if __name__ == '__main__':
    
    if len(sys.argv) < 2:
        print ("usage: python tumblr_crawler.py <config>")
    else:
        main(sys.argv[1])
            