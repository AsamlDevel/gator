#!/usr/bin/python
# -*- coding: cp1251 -*-
'''
Created on Nov 6, 2016

@author: SK
'''

import os
import sys
import hashlib
import urllib2
import pprint
from copy import copy
from robotparser import RobotFileParser
from urlparse import urlunsplit, urlsplit

import urlparse
from bs4 import SoupStrainer, BeautifulSoup
#from BeautifulSoup import SoupStrainer, BeautifulSoup
import logging


# Default config
# TODO: move to config file
TIMEOUT = 5  # Max wait time

# HTTP-default headers will be redefined in UserAgent
DEFAULT_HEADERS = {
'Accept'           : 'text/html, text/plain',
'Accept-Charset'   : 'windows-1251, koi8-r, UTF-8, iso-8859-1, US-ASCII',
'Content-Language' : 'ru,en',
}

DEFAULT_AGENTNAME = 'SKCrawler'
DEFAULT_EMAIL = 'skubjob@gmail.com'

class RobotsHTTPHandler(urllib2.HTTPHandler):
    """
    inputs for UserAgent.
    check if request allowed by robots.txt.
    
    Arguments:
    agentname -- name of crawler
    """
    # TODO: release cache   
    def __init__(self, agentname, *args, **kwargs):
        urllib2.HTTPHandler.__init__(self, *args, **kwargs)
        self.agentname = agentname
    
    def http_open(self, request):
        #request -- urllib2.Request
        url = request.get_full_url()
        host = urlsplit(url)[1]
        robots_url = urlunsplit(('http', host, '/robots.txt', '', ''))
        rp = RobotFileParser(robots_url)
        rp.read()
        if not rp.can_fetch(self.agentname, url):
            raise RuntimeError('Forbidden by robots.txt')
        return urllib2.HTTPHandler.http_open(self, request)
           

class ResponseWrapper(object):
    def __init__(self, response, storage_root_dir = "/tmp"):
        self.root_dir = storage_root_dir
        self.__response = response
        self.__path = None
        self.__md5key = None
        
    def read(self):
        r = None
        with open(self.lpath(), "r") as f:
            r = f.read()
        return r

    def lpath(self):
        if self.__path is None:
            self.__path = os.path.join(self.root_dir, self.md5key())
            with open(self.__path, "w+") as f:
                f.write(self.__response.read())
            
        return self.__path
    
    def md5key(self):
        if self.__md5key is None:
            m = hashlib.md5()
            m.update(self.geturl())
            self.__md5key = str(m.hexdigest())
        return self.__md5key
    
    def geturl(self):
        return self.__response.geturl()
    
    def info(self):
        return self.__response.info()

    
class UserAgent(object):
    """
    UserAgent.
    
    Named args and def value:
    name -- ('Test/1.0')
    email -- ('')
    headers -- dict HTTP-headers (DEFAULT_HEADERS)
    """
    def __init__(self,
            agentname=DEFAULT_AGENTNAME,
            email=DEFAULT_EMAIL,
            new_headers={}):
    
        self.agentname = agentname
        self.email = email 

        self.opener = urllib2.build_opener(RobotsHTTPHandler(self.agentname),)
        headers = copy(DEFAULT_HEADERS)
        headers.update(new_headers) 
        opener_headers = [ (k, v) for k, v in headers.iteritems() ]
        opener_headers.append(('User-Agent', self.agentname))
        if self.email:
            opener_headers.append(('From', self.email))
        self.opener.addheaders = opener_headers

    def open(self, url):
        #Returns file-like object
        return ResponseWrapper(self.opener.open(url, None, TIMEOUT))
    

# Rabbit mq url consumer

import pika
class MQConsumer(object):
    def __init__(self, rabbit_host = 'localhost', rabbit_qname = "url_queue"):
        self.host = rabbit_host
        self.qname = rabbit_qname

    def qdel(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        channel = connection.channel()
        channel.queue_delete( queue = self.qname)
        connection.close()

    def processing(self,callback):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host = self.host))
        channel = connection.channel()
        channel.queue_declare(queue = self.qname)
        
        #def callback(ch, method, properties, body):
        #    print(" [x] Received %r" % body)
        
        channel.basic_consume(callback,
                              queue = self.qname,
                              no_ack = False)
        while True:
            channel.start_consuming()
            
####################
# response consumers
####################

response_info_to_get = ( 
        'Server'
        ,'Content-Type'
        ,'Content-Length'
        ,'Expires'
        ,'Date')

# Simple Printer response consumer

class ResultConsumerSimplePrinter(object):
    def consume(self, response):
        print response.geturl()
        print response.read()
        print "________________________________________________"
        for i in response_info_to_get:
            print "%s: %s"%(i,response.info().get(i))
        print "________________________________________________"

# MySQL response consumer

import MySQLdb as mdb
# Common SQL queries 
SQ_CREATE_TABLE = """create table IF NOT EXISTS url_table (
            schm varchar(32), url varchar(2048), info varchar(16384), lpath varchar(16384),
            md5key varchar(32) UNIQUE, PRIMARY KEY (md5key) );"""

SQ_INSERT_DATA = "INSERT INTO url_table(schm, url, info, lpath, md5key) VALUES(%s, %s, %s, %s, %s)"
SQ_UPDATE_DATA = "UPDATE url_table SET schm=%s, url=%s, info=%s, lpath=%s WHERE md5key=%s"

class ResultConsumerMySQLStorage(object):
    
    def __init__(self,host='127.0.0.1', user='root', passwd='password', dbname="test_crawler_db"):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.cnx = mdb.connect(host, user=self.user, passwd=self.passwd)#, buffered=True)
        #if dbname not in self.execQueryAndFetchAll("show databases"):
        self.cnx.cursor().execute('CREATE DATABASE IF NOT EXISTS '+ dbname+";")
        self.cnx.cursor().execute("USE "+dbname+";")              
        self.cnx.cursor().execute(SQ_CREATE_TABLE)
        self.cnx.commit()
        pass

    def __del__(self):
        if self.cnx is not  None:
            self.cnx.close()
        pass
    '''
    def execQueryAndFetchAll(self, query):
        cursor = self.cnx.cursor()
        cursor.execute(query)
        
        return cursor.fetchall()
    '''
    def consume(self, response):
        info = ""
        for i in response_info_to_get:
            v = response.info().get(i)
            if v is not None:
                info += i + ": "+response.info().get(i)+"\n"
        o = response.geturl().split(":")
        row = (o[0], o[1][2:], info, response.lpath(), response.md5key())
        print row
        try:
            self.cnx.cursor().execute(SQ_INSERT_DATA, row)
            self.cnx.commit()
        except mdb.IntegrityError as e:
            if not e[0] == 1062:
                raise
            else:
                self.cnx.cursor().execute(SQ_UPDATE_DATA, row)
                self.cnx.commit()
        pass
     
#####################    
# The Crawler it self
#####################

urls_provider = MQConsumer()
user_agent =  UserAgent()

response_consumers = set()

class Crawler(object):
    
    def __init__(self):
        pass
    
    @staticmethod
    def callback(ch, method, properties, body):
        response = user_agent.open(body)   
        for consumer in response_consumers:
            consumer.consume(response)
        pass
    
    def start(self):
        global urls_provider
        if urls_provider is not None:
            urls_provider.processing(Crawler.callback)
        pass

if __name__ == '__main__':
    
    response_consumers.add(ResultConsumerSimplePrinter())
    response_consumers.add(ResultConsumerMySQLStorage())
    crawler = Crawler()
    crawler.start()

    print "the end";
    
    
    
    
    
