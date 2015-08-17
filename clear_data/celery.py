# coding=utf-8
from __future__ import absolute_import
__author__ = 'Administrator'

from celery import Celery

app = Celery('clear_data',
             borker='amqp://guest@localhost',
             include=['clear_data.tasks'])

if __name__ == '__main__':
    app.start()
