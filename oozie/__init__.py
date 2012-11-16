#!/usr/bin/env python

import json
import urllib2
import sys

class ClientError(Exception):
    pass

class ServerError(Exception):
    pass

class client(object):
    def __init__(self, url):
        self._url = url
    def healthcheck(self):
        try:
            systemMode = json.loads(urllib2.urlopen(self._url + 'v1/admin/status').read().strip())['systemMode']
            assert(systemMode == 'NORMAL')
            return True
        except AssertionError:
            raise ServerError(systemMode)
        except ValueError as e:
            raise ClientError(e.message)
        except urllib2.HTTPError as e:
            raise ClientError('HTTP Error ' + str(e.getcode()) + ': ' + e.msg + ' ' + e.geturl())

if __name__ == '__main__':
    if len(sys.argv) > 1:
        c = client(sys.argv[1])
        c.healthcheck()
        print 'Oozie installation at ' + sys.argv[1] + ' appears operational'
    else:
        print 'Usage: python ' + __file__ + ' <OOZIE_URL>'