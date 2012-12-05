#!/usr/bin/env python

import logging
import lxml.etree
import os
import os.path
import random
import socket
import time

# Generate useful errors for our callers.
from . import errors
# The elements represent XML nodes used to configure Oozie.
from . import elements
# We use the Oozie client to communicate with the Oozie cluster.
from . import oozie
# We use the HDFS client to place workflows in the required location.
from . import hdfs



OUTPUT_SCRATCH_DIR = '/tmp/oozieoutput/'
WORKFLOW_SCRATCH_DIR = '/tmp/oozieworkflows/'



class jobConfiguration(object):
    # A job is a particular configuration of work.
    # Jobs must be assigned to a cluster before they can be run.
    # A job will have 0 or more runs in it's history.
    # Most functions of the job will implicitly return information from the
    # most recent job run.
    
    #def __new__(cls, *args, **kwargs):
    #    print 'instantiate a jobConfiguration object please'
    #    print args
    #    print kwargs
    #    # Jobs can be initialized from:
    #    #  - a job id which already exists
    #    #  - an HDFS directory which contains a workflow.xml or a coordinator.xml
    #    #  - a local directory which contains a workflow.xml or a coordinator.xml (requires HDFS access)
    #    #  - a Python workflow object or coordinator object (requires HDFS access)
    #    
    #    # We could be initialized from a workflow object (XML)
    #    wf = kwargs.get('workflow', (list(args) + [None])[0])
    #    if isinstance(wf, elements.workflow):
    #        # Init from a workflow object
    #        pass
    #    # We could also be initialized from
    #    
    #    return
    
    # Jobs need to interact with HDFS via WebHDFS and Oozie via the web
    # service APIs.
    @property
    def _hdfsClient(self):
        try:
            assert self.__hdfsClient is not None
        except AttributeError:
            # If no WEBHDFS_URL is configured, let's guess based on the name node configured for Oozie
            webHdfsUrl = os.environ.get('WEBHDFS_URL') or ','.join([h for (h, p) in [(nodename.split(':') + [''])[:2] for nodename in self._oozieClient.config().get('oozie.service.HadoopAccessorService.nameNode.whitelist').split(',')]])
            self.__hdfsClient = hdfs.client(webHdfsUrl)
        return self.__hdfsClient
    
    @property
    def _oozieClient(self):
        try:
            assert self.__oozieClient is not None
        except AttributeError:
            self.__oozieClient = oozie.client()
        return self.__oozieClient
    
    
    # These properties generally need to be discovered or assigned once, at
    # which point they become static.
    @property
    def uniquifier(self):
        try:
            assert self._uniquifier is not None
        except AttributeError:
            self._uniquifier = '-'.join([self.get('name'), socket.gethostname(), str(int(time.time() * 1000)), str(''.join([random.choice('0123456789abcdef') for i in xrange(0, 4)]))])
        return self._uniquifier
    
    @property
    def id(self):
        # Determine the id of this job, submitting it to Oozie if necessary to generate one.
        try:
            assert self._id is not None
        except AttributeError:
            self.submit()
        return self._id
    
    @property
    def status(self):
        # Determine the status of this job, querying Oozie to find it.
        return self._oozieClient.status(self.id)
    
    def upload(self, localFilename, remoteFilename=None):
        try:
            assert os.path.exists(localFilename)
        except AssertionError:
            raise errors.ClientError('File to upload does not exist: "' + localFilename + '"')
        # Absolute paths will be respected.
        # Relative paths will be interpreted as relative to the job's scratch directory.
        # No remote filename provided will result in the same basename in the workflow source
        # directory.
        if remoteFilename is None:
            remoteFilename = os.path.basename(localFilename)
        remoteFilename = ([''] + remoteFilename.split('hdfs://', 1))[-1]
        if not remoteFilename.startswith('/'):
            remoteFilename = os.path.join(self.sourcePath, remoteFilename)
        self._hdfsClient.mkdir(os.path.dirname(remoteFilename))
        status = self._hdfsClient.copyFromLocal(localFilename, remoteFilename)
        remoteFilename = 'hdfs://' + remoteFilename
        try:
            assert status == 201
            with open(localFilename, 'r') as f:
                assert self._hdfsClient.read(remoteFilename) == f.read()
        except AssertionError:
            raise errors.ServerError('Uploaded file not created: "' + remoteFilename + '"')
        return remoteFilename
    
    @property
    def sourcePath(self):
        # Determine or create the HDFS source path for this job.
        try:
            assert self._sourcePath is not None
        except AttributeError:
            # Workflow job submission requires that the workflow XML file be
            # visible in HDFS, so first things first, we'll upload the XML file.
            self._hdfsClient.mkdir(WORKFLOW_SCRATCH_DIR)
            # Hostname + timestamp + random number will be my attempt at conflict-free writes.
            workflowDirectory = os.path.join(WORKFLOW_SCRATCH_DIR, self.uniquifier)
            self._hdfsClient.mkdir(workflowDirectory)
            workflowPath = os.path.join(workflowDirectory, 'workflow.xml')
            print lxml.etree.tostring(self, pretty_print=True)
            self._hdfsClient.write(workflowPath, lxml.etree.tostring(self, pretty_print=True))
            # Let's also upload an XML file which contains configuration defaults.
            #defaultConfigPath = os.path.join(workflowDirectory, 'config-default.xml')
            #defaultConfig = elements.configuration({
            #    'jobTracker': '',
            #    'nameNode': '',
            #    'input': 'hdfs://',
            #    'output': 'hdfs://',
            #})
            #self._hdfsClient.write(defaultConfigPath, lxml.etree.tostring(defaultConfig, pretty_print=True))
            # Set the containing directory as our source path
            self._sourcePath = workflowDirectory
        return self._sourcePath
    @property
    def outputPath(self):
        # Determine or assign the HDFS output path for this invocation of this job.
        try:
            assert self._outputPath is not None
        except AttributeError:
            # Assign directory to handle output
            # DO NOT create the directory; Hadoop likes to do that itself.
            # TODO most people will want to configure this to something else.
            outputDirectory = os.path.join(OUTPUT_SCRATCH_DIR, self.uniquifier)
            #self._hdfsClient.mkdir(outputDirectory)
            self._outputPath = outputDirectory
        return self._outputPath
    
    # Actions which we can take for this job against the Oozie service.
    def submit(self, parameters=None):
        # Compose the proper Oozie request which will submit the workflow to
        # the cluster.
        parameters = parameters or {}
        if 'user.name' not in parameters:
            parameters['user.name'] = 'hdfs'
        if 'oozie.wf.application.path' not in parameters:
            parameters['oozie.wf.application.path'] = 'hdfs://' + ([''] + self.sourcePath.split('hdfs://', 1))[-1]
        # The Hadoop client jars must be uploaded to HDFS beforehand.
        # TODO is this a sane default for most people?
        if 'oozie.libpath' not in parameters:
            parameters['oozie.libpath'] = 'hdfs:///user/' + parameters['user.name'] + '/lib'
            
        # Required parameters which you might not have set.
        # We'll try to do it for you if we can.
        if 'jobTracker' not in parameters:
            parameters['jobTracker'] = self._oozieClient.config().get('oozie.service.HadoopAccessorService.jobTracker.whitelist')
        if 'nameNode' not in parameters:
            parameters['nameNode'] = self._oozieClient.config().get('oozie.service.HadoopAccessorService.nameNode.whitelist')
        
        # Default substitutions I think you might use and I am using to test this
        if 'output' not in parameters:
            parameters['output'] = 'hdfs://' + ([''] + self.outputPath.split('hdfs://', 1))[-1]
        conf = elements.configuration(parameters)
        print lxml.etree.tostring(conf, pretty_print=True)
        self._id = self._oozieClient.submit(lxml.etree.tostring(conf))
        return True
    def run(self):
        return self._oozieClient.run(self.id)
    def suspend(self):
        pass
    def resume(self):
        pass
    def kill(self):
        pass
    def rerun(self, skip, parameters):
        pass
    def schedule(self, action, ts, parameters):
        # Create a coordinator job which will run the job at the specified time.
        pass
    
    def iterOutputFilenames(self):
        try:
            assert self.status == 'SUCCEEDED'
        except AssertionError:
            self.run()
        # Inventory output directory
        def inventoryFilesRecursively(path):
            subpaths = self._hdfsClient.listdir(path)
            if len(subpaths) == 1 and subpaths[0] == '':
                yield path
            else:
                for subpath in subpaths:
                    if not subpath.startswith('_'):
                        for filename in inventoryFilesRecursively(os.path.join(path, subpath)):
                            yield filename
        for filename in inventoryFilesRecursively(self.outputPath):
            # Retrieve file
            yield filename
    def iterOutputLines(self):
        for filename in self.iterOutputFilenames():
            for line in self._hdfsClient.read(filename).splitlines():
                yield line.rstrip('\n')

class workflowJob(elements.workflow, jobConfiguration):
    pass