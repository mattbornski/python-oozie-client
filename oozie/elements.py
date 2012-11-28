import logging
import lxml.etree

from . import errors



class _parameterizedElement(lxml.etree.ElementBase):
    # setting allow to ['foo', 'bar'] will only let properties of 'foo' or
    # 'bar' be set on this object
    allow = None
    # setting deny to ['boo', 'far'] will prohibit properties 'boo' and 'far'
    # from being set on this object
    deny = None
    # setting both deny and allow will result in the most restrictive
    # interpretation: a key must be allowed and not denied to be set.
    def __init__(self, *args, **kwargs):
        super(_parameterizedElement, self).__init__()
        self._parameters = kwargs.get('parameters', (list(args) + [{}])[0])
        self._setAttributes(self._parameters)
    def _setAttributes(self, attributes):
        for (k, v) in attributes.iteritems():
            if self.deny is not None and k in self.deny:
                continue
            if self.allow is None or k in self.allow:
                self.set(k, v)

class workflow(_parameterizedElement):
    def __init__(self, *args, **kwargs):
        super(workflow, self).__init__(*args, **kwargs)
        # Override the tag.  The XML workflow parent element is "workflow-app"
        self.tag = 'workflow-app'
        self.set('xmlns', 'uri:oozie:workflow:0.1')
        
    def add_action(self, parameters=None):
        parameters = parameters or {}
        if parameters.get('template') is not None:
            actionElement = {
                'map-reduce': mapreduce({k: v for (k, v) in parameters.iteritems() if k not in ['template']}),
            }[parameters.get('template')]
        else:
            actionElement = action(parameters)
        self.append(actionElement)
        return actionElement
    def fix(self):
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
                messageElement.text = 'Map/Reduce failed, error message[${wf:errorMessage()}]'
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
            #assert len(list(self.iterchildren(tag='kill'))) >= 1, 'no kill node'
            assert len(list(self.iterchildren(tag='end'))) == 1, 'no end node'
            
            assert list(self.iterchildren())[0].tag == 'start', 'start node not first'
            assert list(self.iterchildren())[-1].tag == 'end', 'end node not last'
            
            for action in self.iterchildren(tag='action'):
                assert len(list(action.iterchildren(tag='ok'))) == 1, 'no ok node'
                assert len(list(action.iterchildren(tag='error'))) == 1, 'no error node'
                assert list(action.iterchildren())[-2].tag == 'ok', 'ok node not second to last'
                assert list(action.iterchildren())[-1].tag == 'error', 'error node not last'
            
        except AssertionError as e:
            raise errors.ClientError('Workflow appears malformed: ' + e.message)

class action(_parameterizedElement):
    def __init__(self, *args, **kwargs):
        super(action, self).__init__(*args, **kwargs)
        # Override the tag.  The action element is "action".
        # We normally wouldn't have to do this because the class name is the
        # default tag name; however, we subclass this element, and want the
        # tag name to remain as "action" when we do.
        self.tag = 'action'

class _mapreduce(_parameterizedElement):
    def __init__(self, *args, **kwargs):
        super(_mapreduce, self).__init__(*args, **kwargs)
        # Override the tag.  The map reduce action element is "map-reduce"
        self.tag = 'map-reduce'
        # Add the required parameterized tags for the Hadoop cluster's
        # basic config.
        jt = self.makeelement('job-tracker')
        jt.text = '${jobTracker}'
        self.append(jt)
        nn = self.makeelement('name-node')
        nn.text = '${nameNode}'
        self.append(nn)
        # Add the Streaming nature
        s = self.makeelement('streaming')
        m = s.makeelement('mapper')
        m.text = '/bin/cat'
        s.append(m)
        r = s.makeelement('reducer')
        r.text = '/bin/cat'
        s.append(r)
        self.append(s)
        # Add the non-basic Hadoop config.
        #self.append(configuration())

class mapreduce(action):
    def __init__(self, *args, **kwargs):
        super(mapreduce, self).__init__(*args, **kwargs)
        self.append(_mapreduce())

class ok(_parameterizedElement):
    pass

class error(_parameterizedElement):
    pass



class configuration(_parameterizedElement):
    allow = []
    def __init__(self, *args, **kwargs):
        super(configuration, self).__init__(*args, **kwargs)
        for (k, v) in self._parameters.iteritems():
            prop = self.makeelement('property')
            name = prop.makeelement('name')
            name.text = k
            prop.append(name)
            value = prop.makeelement('value')
            value.text = v
            prop.append(value)
            self.append(prop)

class property(_parameterizedElement):
    pass