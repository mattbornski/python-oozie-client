#!/usr/bin/env python

import logging
import lxml.etree
import os
import os.path
import random
import requests
import socket
import time

# Generate useful errors for our callers.
from . import errors
# Export the "workflow" element to our importers.
from .elements import workflow
# We use the HDFS client to place workflows in the required location.
from . import hdfs



WORKFLOW_SCRATCH_DIR = '/tmp/oozieworkflows/'



class client(object):
    def __init__(self, url=None):
        if url is None:
            url = os.environ.get('OOZIE_URL')
        if url is None:
            raise errors.ClientError('No Oozie URL provided and none set in environment OOZIE_URL')
        self._url = url.rstrip('/')
        self._version = 'v1'
    def healthcheck(self):
        try:
            systemMode = requests.get('/'.join([self._url, self._version, 'admin/status'])).json['systemMode']
            assert(systemMode == 'NORMAL')
            logging.info('Oozie installation at ' + self._url + ' appears operational')
            return True
        except AssertionError:
            raise errors.ServerError(systemMode)
        except ValueError as e:
            raise errors.ClientError(e.message)
        except urllib2.HTTPError as e:
            raise errors.ClientError('HTTP Error ' + str(e.getcode()) + ': ' + e.msg + ' ' + e.geturl())
    def run(self, wf):
        # Validate workflow
        # Requires at least one start node
        #  - which points to an existing action node
        # Requires at least one action node
        #  - which should point to existing kill node on failure
        #  - which should point to existing action node or existing end node on success
        wf.validate()
        # Submit job
        # Workflow job submission requires that the workflow XML file be
        # visible in HDFS, so first things first, we'll upload the XML file.
        hdfsClient = hdfs.client()
        hdfsClient.mkdir(WORKFLOW_SCRATCH_DIR)
        # Hostname + timestamp + random number will be my attempt at conflict-free writes.
        workflowDirectory = os.path.join(WORKFLOW_SCRATCH_DIR, '-'.join([socket.gethostname(), str(int(time.time() * 1000)), str(random.random())]))
        hdfsClient.mkdir(workflowDirectory)
        workflowPath = os.path.join(workflowDirectory, 'workflow.xml')
        hdfsClient.write(workflowPath, lxml.etree.tostring(wf, pretty_print=True))
        # Secondly, we'll compose the proper Oozie request which will run
        # the workflow on the cluster.
        conf = elements.configuration({
            'user.name': 'hdfs',
            'oozie.wf.application.path': workflowDirectory,
        })
        print lxml.etree.tostring(conf, pretty_print=True)
        import sys
        print requests.post('/'.join([self._url, self._version, 'jobs']), data=lxml.etree.tostring(conf), headers={'content-type':'application/xml'}, config={'verbose': sys.stderr}).text