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



# Helper function to convert a configuration parameter which probably contains
# a namenode URI, but might contain several, to a guaranteed single (and
# hopefully active) namenode.
def _extractSingleNamenodeUri(namenodeConfiguration):
    for nodename in namenodeConfiguration.split(','):
        try:
            nodename = ([None] + nodename.split('hdfs://', 1))[-1]
            # Internally, the WebHDFS client will test whether this is a valid name node.
            c = hdfs.client(nodename)
            return 'hdfs://' + nodename
        except KeyboardInterrupt:
            raise
        except errors.ClientError:
            pass



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
            webHdfsUrl = os.environ.get('WEBHDFS_URL') or _extractSingleNamenodeUri(self._oozieClient.config().get('oozie.service.HadoopAccessorService.nameNode.whitelist'))
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
            self._uniquifier = '-'.join([self.get('name', 'unknown'), socket.gethostname(), str(int(time.time() * 1000)), str(''.join([random.choice('0123456789abcdef') for i in xrange(0, 4)]))])
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
    
    def upload(self, localPath, remotePath=None):
        try:
            assert os.path.exists(localPath)
        except AssertionError:
            raise errors.ClientError('File to upload does not exist: "' + localFilename + '"')
        
        def walk(path):
            for (dirpath, dirnames, filenames) in os.walk(path):
                for filename in filenames:
                    yield os.path.join(dirpath, filename)
                for dirname in dirnames:
                    for filename in walk(os.path.join(dirpath, dirname)):
                        yield filename
        
        remotePath = ([''] + (remotePath or self.sourcePath).split('hdfs://', 1))[-1]
        if not remotePath.startswith('/'):
            # Not an absolute path.
            remotePath = os.path.join(self.sourcePath, remotePath)
        
        for localFilename in (walk(localPath) if os.path.isdir(localPath) else [localPath]):
            print 'please upload '
            print localFilename
            deltaPath = localPath.split(os.path.commonprefix([localFilename, localPath]), 1)[-1]
            if deltaPath == '':
                remoteFilename = os.path.join(remotePath, os.path.basename(localFilename))
            else:
                remoteFilename = os.path.join(remotePath, deltaPath)
            print 'to'
            print remoteFilename
            self._hdfsClient.mkdir(os.path.dirname(remoteFilename))
            
            status = self._hdfsClient.copyFromLocal(localFilename, remoteFilename)
            try:
                assert status == 201
            except AssertionError:
                raise errors.ServerError('Uploaded file not created: "' + remoteFilename + '"')
        
        remotePath = 'hdfs://' + remotePath
        return remotePath
    
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
        try:
            assert self._id is None
        except AssertionError:
            raise errors.ClientError('Workflow job instance already submitted')
        except AttributeError:
            pass
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
        # Ensure that the libpath exists and has some JARs.  If not, warn loudly.
        # TODO
        #if len(self._hdfsClient.listdir(parameters['oozie.libpath'])) == 0:
        #    raise errors.ClientError('Libpath "' + parameters['oozie.libpath'] + '" does not exist or is empty.')
            
        # Required parameters which you might not have set.
        # We'll try to do it for you if we can.
        if 'jobTracker' not in parameters:
            parameters['jobTracker'] = self._oozieClient.config().get('oozie.service.HadoopAccessorService.jobTracker.whitelist')
        if 'nameNode' not in parameters:
            parameters['nameNode'] = _extractSingleNamenodeUri(self._oozieClient.config().get('oozie.service.HadoopAccessorService.nameNode.whitelist'))
        
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
        return self._oozieClient.suspend(self.id)
    def resume(self):
        try:
            assert self.status == 'SUSPENDED'
        except AssertionError:
            raise errors.ClientError('Cannot resume a workflow job that is not suspended')
        return self._oozieClient.resume(self.id)
    def kill(self):
        return self._oozieClient.kill(self.id)
    def rerun(self, skip=None, parameters=None):
        if skip is None:
            # Inventory steps.  Skip all the ones that have a status of "OK"
            skip = 1
        return self._oozieClient.rerun()
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
    def __init__(self, *args, **kwargs):
        initstring = None
        if isinstance(args[0], basestring):
            initstring = args[0]
            args = list(args[1:])
        super(workflowJob, self).__init__(*args, **kwargs)
        if initstring is not None:
            if initstring in self._oozieClient.list():
                # It's a running workflow id to be re-connected to.
                self._id = initstring
            elif os.path.exists(initstring):
                # It's a workflow.xml file or a directory containing one on the local filesystem.
                if os.path.isdir(initstring):
                    # Locate XML file
                    for filename in os.listdir(initstring):
                        if filename.endswith('.xml'):
                            # Extract name
                            with open(os.path.join(initstring, filename), 'r') as f:
                                wf = lxml.etree.fromstring(f.read())
                                if wf.tag == 'workflow-app' or wf.tag.endswith('}workflow-app'):
                                    self.set('name', wf.get('name'))
                    # Upload entire directory.
                    self._sourcePath = self.upload(initstring)
            #    else:
            #        # Create directory, upload this file as workflow.xml
            #        
            #elif :
            #    # It's a workflow.xml file or a directory containing one on the remote filesystem.
            #    self._sourcePath = args[0]
            #    args = list(args[1:])
            else:
                raise errors.ClientError('What am I supposed to do with this? "' + args[0] + '"')
