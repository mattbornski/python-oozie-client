import collections
import logging
import lxml.etree

from . import errors



class _parameterizedElement(lxml.etree.ElementBase):
    # setting allowedAttributes to ['foo', 'bar'] will only let attributes
    # 'foo' or 'bar' be set on this object.
    allowedAttributes = None
    # setting deniedAttributes to ['boo', 'far'] will prohibit attributes
    # 'boo' and 'far' from being set on this object.
    deniedAttributes = None
    # setting both deniedAttributes and allowedAttributes will result in the
    # most restrictive interpretation: an attributes must be in the
    # allowedAttributes list and not in the deniedAttributed list to be set.
    def __init__(self, *args, **kwargs):
        super(_parameterizedElement, self).__init__()
        self._parameters = kwargs.get('parameters', (list(args) + [{}])[0])
        self._setAttributes(self._parameters)
    def _setAttributes(self, attributes):
        for (k, v) in attributes.iteritems():
            if self.deniedAttributes is not None and k in self.deniedAttributes:
                continue
            if self.allowedAttributes is None or k in self.allowedAttributes:
                self.set(k, v)

class workflow(_parameterizedElement):
    def __init__(self, *args, **kwargs):
        self.deniedAttributes = (self.deniedAttributes or [])
        self.deniedAttributes.extend(['actions', 'template'])
        super(workflow, self).__init__(*args, **kwargs)
        # Override the tag.  The XML workflow parent element is "workflow-app"
        self.tag = 'workflow-app'
        self.set('xmlns', 'uri:oozie:workflow:0.2')
        for action in self._parameters.get('actions', []):
            self.add_action(action)
    
    def add_action(self, parameters=None):
        parameters = parameters or {}
        if parameters.get('name') is None:
            parameters['name'] = 'action-' + str(len(list(self.iterchildren())) + 1)
        if parameters.get('template') is not None:
            actionElement = {
                'map-reduce': mapreduce({k: v for (k, v) in parameters.iteritems() if k not in self.deniedAttributes}),
            }[parameters.get('template')]
        else:
            actionElement = action(parameters)
        self.append(actionElement)
        return actionElement
    
    def fix(self):
        print lxml.etree.tostring(self, pretty_print=True)
        
        # Must have a start node
        if len(list(self.iterchildren(tag='start'))) < 1 and len(list(self.iterchildren(tag='action'))) > 0:
            start = self.makeelement('start')
            start.set('to', list(self.iterchildren(tag='action'))[0].get('name'))
            self.append(start)
        
        # We'll want to know the name of the existing end node and kill node
        # in case we find some actions that are missing the references.  If
        # none exists, we'll create some with some default names later.
        endNodeName = (list(self.iterchildren(tag='end')) + [{}])[0].get('name') or 'end'
        killNodeName = (list(self.iterchildren(tag='kill')) + [{}])[0].get('name') or 'kill'
        
        for action in self.iterchildren(tag='action'):
            # Every action node needs an "ok" transition and an "error"
            # transition, and they must appear in that order as the last
            # two sub-elements of the action element.
            if len(list(action.iterchildren(tag='ok'))) < 1:
                okElement = action.makeelement('ok')
                okElement.set('to', endNodeName)
                action.append(okElement)
            if len(list(action.iterchildren(tag='error'))) < 1:
                errorElement = action.makeelement('error')
                errorElement.set('to', killNodeName)
                action.append(errorElement)
            if len(list(action.iterchildren(tag='ok'))) == 1 and list(action.iterchildren())[-2].tag != 'ok':
                okElement = list(action.iterchildren(tag='ok'))[0]
                action.remove(okElement)
                action.insert(len(list(action.iterchildren())) - 1, okElement)
            if len(list(action.iterchildren(tag='error'))) == 1 and list(action.iterchildren())[-1].tag != 'error':
                errorElement = list(action.iterchildren(tag='error'))[0]
                action.remove(errorElement)
                action.append(errorElement)
        
        # If a node was ever referenced then it should exist.
        existingNodeNames = set([node.get('name') for node in lxml.etree.XPath('//action | //end | //kill')(self)])
        referencedNodeNames = set([node.get('to') for node in lxml.etree.XPath('//*[@to]')(self)])
        endNodeNames = set([node.get('to') for node in lxml.etree.XPath('//ok')(self)] + [endNodeName])
        killNodeNames = set([node.get('to') for node in lxml.etree.XPath('//error')(self)] + [killNodeName])
        for name in referencedNodeNames - existingNodeNames:
            if name in endNodeNames:
                logging.warning('Implicitly creating end node "' + name + '"')
                endElement = self.makeelement('end')
                endElement.set('name', name)
                self.append(endElement)
            elif name in killNodeNames:
                logging.warning('Implicitly creating kill node "' + name + '"')
                killElement = self.makeelement('kill')
                killElement.set('name', name)
                messageElement = killElement.makeelement('message')
                messageElement.text = 'Map/Reduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]'
                killElement.append(messageElement)
                self.append(killElement)
            
        # Start node must be first in workflow
        if len(list(self.iterchildren(tag='start'))) == 1 and list(self.iterchildren())[0].tag != 'start':
            startNode = list(self.iterchildren(tag='start'))[0]
            self.remove(startNode)
            self.insert(0, startNode)
        # End node must be last in workflow
        if len(list(self.iterchildren(tag='end'))) == 1 and list(self.iterchildren())[-1].tag != 'end':
            endNode = list(self.iterchildren(tag='end'))[0]
            self.remove(endNode)
            self.append(endNode)
    
    def validate(self, fix=True):
        # Fix any trivial problems before strict validation.
        if fix:
            self.fix()
        
        print lxml.etree.tostring(self, pretty_print=True)
        
        try:
            assert len(list(self.iterchildren(tag='start'))) == 1, 'no start node'
            assert len(list(self.iterchildren(tag='action'))) >= 1, 'no action nodes'
            assert len(list(self.iterchildren(tag='end'))) == 1, 'no end node'
            
            assert list(self.iterchildren())[0].tag == 'start', 'start node not first'
            assert list(self.iterchildren())[-1].tag == 'end', 'end node not last'
            
            for action in self.iterchildren(tag='action'):
                assert len(list(action.iterchildren(tag='ok'))) == 1, 'no ok node'
                assert len(list(action.iterchildren(tag='error'))) == 1, 'no error node'
                assert list(action.iterchildren())[-2].tag == 'ok', 'ok node not second to last'
                assert list(action.iterchildren())[-1].tag == 'error', 'error node not last'
            
            # Ensure all referenced nodes exist.
            existingNodeNames = set([node.get('name') for node in lxml.etree.XPath('//action | //end | //kill')(self)])
            referencedNodeNames = set([node.get('to') for node in lxml.etree.XPath('//*[@to]')(self)])
            assert len(referencedNodeNames - existingNodeNames) == 0, 'some referenced nodes do not exist'
            
        except AssertionError as e:
            raise errors.ClientError('Workflow appears malformed: ' + e.message)

class action(_parameterizedElement):
    def __init__(self, *args, **kwargs):
        self.deniedAttributes = (self.deniedAttributes or [])
        self.deniedAttributes.extend(['input', 'output'])
        super(action, self).__init__(*args, **kwargs)
        # Override the tag.  The action element is "action".
        # We normally wouldn't have to do this because the class name is the
        # default tag name; however, we subclass this element, and want the
        # tag name to remain as "action" when we do.
        self.tag = 'action'

class _nestedAction(_parameterizedElement):
    def __init__(self, *args, **kwargs):
        self.deniedAttributes = (self.deniedAttributes or [])
        self.deniedAttributes.extend(['input', 'output'])
        super(_nestedAction, self).__init__(*args, **kwargs)
        # Add the required parameterized tags for the Hadoop cluster's
        # basic config.
        jt = self.makeelement('job-tracker')
        jt.text = '${wf:conf("jobTracker")}'
        self.append(jt)
        nn = self.makeelement('name-node')
        nn.text = '${wf:conf("nameNode")}'
        self.append(nn)

class _nestedMapReduce(_nestedAction):
    def __init__(self, *args, **kwargs):
        self.deniedAttributes = (self.deniedAttributes or [])
        self.deniedAttributes.extend(['mapper', 'reducer', 'name'])
        super(_nestedMapReduce, self).__init__(*args, **kwargs)
        # Override the tag.  The map reduce action element is "map-reduce"
        self.tag = 'map-reduce'
        # Add the Streaming nature
        s = self.makeelement('streaming')
        m = s.makeelement('mapper')
        m.text = self._parameters['mapper']
        s.append(m)
        r = s.makeelement('reducer')
        r.text = self._parameters['reducer']
        s.append(r)
        self.append(s)
        # Add the config for this action.
        parameters = {
            'mapred.input.dir': self._parameters.get('input', '${wf:conf("' + self._parameters['name'] + '-input")}'),
            'mapred.output.dir': self._parameters.get('output', '${wf:conf("output")}/' + self._parameters['name']),
        }
        for (k, v) in self._parameters.iteritems():
            if k in self.deniedAttributes and k not in ['mapper', 'reducer', 'name']:
                parameters[k] = v 
        self.append(configuration(parameters))

class mapreduce(action):
    def __init__(self, *args, **kwargs):
        self.deniedAttributes = (self.deniedAttributes or [])
        self.deniedAttributes.extend(['mapper', 'reducer'])
        super(mapreduce, self).__init__(*args, **kwargs)
        self.append(_nestedMapReduce({k: v for (k, v) in self._parameters.iteritems() if k in (self.deniedAttributes + ['name'])}))

class _nestedHive(_nestedAction):
    def __init__(self, *args, **kwargs):
        self.deniedAttributes = (self.deniedAttributes or [])
        self.deniedAttributes.extend(['mapper', 'reducer', 'name'])
        super(_nestedMapReduce, self).__init__(*args, **kwargs)
        # Override the tag.  The hive action element is "hive"
        self.tag = 'hive'
        self.set('xmlns', 'uri:oozie:hive-action:0.2')

class hive(action):
    def __init__(self, *args, **kwargs):
        self.deniedAttributes = (self.deniedAttributes or [])
        self.deniedAttributes.extend(['mapper', 'reducer'])
        super(hive, self).__init__(*args, **kwargs)
        self.append(_nestedHive({k: v for (k, v) in self._parameters.iteritems() if k in (self.deniedAttributes + ['name'])}))

class ok(_parameterizedElement):
    pass

class error(_parameterizedElement):
    pass



def _flattenForConfigFile(value):
    if value is None:
        return ''
    elif value is False:
        return 'false'
    elif value is True:
        return 'true'
    elif isinstance(value, basestring):
        return value
    elif isinstance(value, collections.Sequence):
        return ' '.join([_flattenForConfigFile(v) for v in value])
    else:
        return str(value)

class configuration(_parameterizedElement):
    # Override this function.  We do not have attributes;
    # rather, we have sub-elements.
    def _setAttributes(self, attributes):
        for (k, v) in attributes.iteritems():
            prop = self.makeelement('property')
            name = prop.makeelement('name')
            name.text = k
            prop.append(name)
            value = prop.makeelement('value')
            value.text = _flattenForConfigFile(v)
            prop.append(value)
            self.append(prop)

class property(_parameterizedElement):
    pass