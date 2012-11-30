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
        super(client, self).__init__(namenode_host=parsed.hostname, namenode_port=parsed.port or 50070, hdfs_username=parsed.username or 'hdfs')
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