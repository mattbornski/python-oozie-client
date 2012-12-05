import os
import tempfile
import urlparse
import webhdfs.webhdfs

from . import errors



class client(webhdfs.webhdfs.WebHDFS):
    def __init__(self, url=None):
        if url is None:
            url = os.environ.get('WEBHDFS_URL')
        if url is None:
            raise errors.ClientError('No WebHDFS URL provided and none set in environment WEBHDFS_URL')
        parsed = urlparse.urlparse(url)
        # Let's allow for failover configuration URLs like
        # http://namenode1,namenode2:57000/webhdfs/v1/
        # If you prefix your URL properly with http we'll parse the comma separated hosts.
        # If you just pass "namenode1,namenode2" we'll split the whole fake URL.
        for namenode_host in (parsed.hostname or url).split(','):
            namenode_port = parsed.port or 50070
            hdfs_username = parsed.username or 'hdfs'
            test = webhdfs.webhdfs.WebHDFS(namenode_host=namenode_host, namenode_port=namenode_port, hdfs_username=hdfs_username)
            try:
                test.listdir('/')
                super(client, self).__init__(namenode_host=namenode_host, namenode_port=namenode_port, hdfs_username=hdfs_username)
                break
            except KeyError:
                pass
        else:
            raise errors.ClientError('WebHDFS at ' + url + ' appears misconfigured')
    # Override the webhdfs copy[To|From]Local functions, which erroneously
    # append a leading / to the remote address.
    def copyFromLocal(self, *args, **kwargs):
        # Second argument is target_path.
        try:
            kwargs['target_path'] = kwargs['target_path'].lstrip('/')
        except KeyError:
            args = list(args)
            args[1] = args[1].lstrip('/')
        return super(client, self).copyFromLocal(*args, **kwargs)
    def copyToLocal(self, *args, **kwargs):
        # First argument is source_path.
        try:
            kwargs['source_path'] = kwargs['source_path'].lstrip('/')
        except KeyError:
            args = list(args)
            args[0] = args[0].lstrip('/')
        return super(client, self).copyToLocal(*args, **kwargs)
    # Create helper functions which read and write buffers instead of
    # requiring filenames.
    def write(self, path, data):
        filename = tempfile.NamedTemporaryFile(delete=False).name
        try:
            with open(filename, 'wb') as f:
                f.write(data)
            return self.copyFromLocal(filename, path)
        finally:
            try:
                os.remove(filename)
            except OSError:
                pass
    def read(self, path):
        filename = tempfile.NamedTemporaryFile().name
        try:
            self.copyToLocal(path, filename)
            with open(filename, 'r') as f:
                return f.read()
        finally:
            try:
                os.remove(filename)
            except OSError:
                pass