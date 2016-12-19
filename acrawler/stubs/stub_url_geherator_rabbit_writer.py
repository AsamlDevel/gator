#!/usr/bin/python

'''
Created on Dec 12, 2016

@author: someone
'''
import pika
import time
import thread

RABBIT_HOST = 'localhost'
QNAME = "url_queue"


def qdel(qname = QNAME):
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
    channel = connection.channel()
    channel.queue_delete(queue=qname)
    connection.close()
    
def qinit(qname = QNAME):
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QNAME)
    connection.close()

def qpush(msgs):
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
    channel = connection.channel()
    #channel.queue_declare(queue=QNAME)
    for msg in msgs:
        channel.basic_publish(exchange='',
                      routing_key=QNAME,
                      body=msg)
    connection.close()

def qpop():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QNAME)
    
    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
    

    channel.basic_consume(callback,
                          queue=QNAME,
                          no_ack=False)
    while True:
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
   

if __name__ == '__main__':
    #thread.start_new_thread(qpop)
    qdel()
    qinit()
    i = 0
    msgs = ["http://www.ctvnews.ca", "http://www.cnn.com","http://www.cbc.ca"]
    while True:
        msg = msgs[i%3]
        print msg
        qpush((msg,))
        time.sleep(5)
        i+=1
    
    print "the end"
    pass