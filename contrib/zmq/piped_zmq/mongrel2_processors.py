# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# Parts Copyright (c) 2010, Zed A. Shaw and Mongrel2 Project Contributors.
# See LICENSE for details.

""" Utility processors that are especially useful in ZMQ contexts. """
import json
import urlparse

import zmq
from zope import interface

from piped import util, processing
from piped.processors import base


json_encoder = util.PipedJSONEncoder()
HTTP_FORMAT = "HTTP/1.1 %(code)s %(status)s\r\n%(headers)s\r\n\r\n%(body)s"


def http_response(body, code=200, status='OK', headers=None):
    payload = {'code': code, 'status': status, 'body': body}
    headers = headers or dict()
    headers['Content-Length'] = len(body)
    payload['headers'] = "\r\n".join('%s: %s' % (k, v) for k, v in headers.items())

    return HTTP_FORMAT % payload


def parse_netstring(ns):
    len, rest = ns.split(':', 1)
    len = int(len)
    assert rest[len] == ',', "Netstring did not end in ','"
    return rest[:len], rest[len + 1:]


def parse_mongrel_http_request(msg):
    sender, conn_id, path, rest = msg.split(' ', 3)
    headers, rest = parse_netstring(rest)
    body = parse_netstring(rest)[0]
    headers = json.loads(headers)

    data = dict()
    if headers['METHOD'] == 'JSON':
        data = json.loads(body)

    # The query-string is URL-encoded, so it's all ASCII at this
    # point. But json.loads have made all strings into unicode, though
    # it's unaware of the URL-encoding. Make sure the input to
    # parse_qs is a bytestring, otherwise it gets confused.
    raw_query_string = headers.get('QUERY', u'').encode('utf8')

    # Now turn the raw query-(byte)-string into a dictionary,
    # converting the utf8-strings into unicode-objects post-parse_qs.
    query_string = dict((key.decode('utf8'), [v.decode('utf8') for v in list_of_values])
                        for (key, list_of_values) in urlparse.parse_qs(raw_query_string).items())

    # parse_qs returns a list of values for every parameter.  We
    # expect most parameters to take a single value, and want those to
    # be scalars.
    for key, list_of_values in query_string.items():
        if len(list_of_values) == 1:
            query_string[key] = list_of_values[0]

    return dict(
        sender=sender,
        conn_id=conn_id,
        path=path,
        headers=headers,
        body=body,
        data=data,
        query_string=query_string
    )


class MongrelRequestToBatonParser(base.Processor):
    name = 'parse-msg-as-mongrel-request'
    interface.classProvides(processing.IProcessor)

    def get_consumers(self, baton):
        if util.dict_get_path(baton, 'http_request.data.type') == 'disconnect':
            return []
        return super(MongrelRequestToBatonParser, self).get_consumers(baton)

    def process(self, msg):
        baton = dict()
        baton['http_request'] = request = parse_mongrel_http_request(msg)
        if request['data'].get('type') == 'disconnect':
            return

        baton['http_response'] = dict(uuid=request['sender'], idents=[request['conn_id']], headers=dict(), body='')
        return baton


class MongrelReplySender(base.Processor):
    name = 'send-mongrel-reply'
    interface.classProvides(processing.IProcessor)

    def __init__(self, queue_name, response_path='http_response', close=True, *a, **kw):
        super(MongrelReplySender, self).__init__(*a, **kw)
        self.queue_name = queue_name
        self.response_path = response_path
        self.close = close

    def configure(self, runtime_environment):
        self.dependencies = runtime_environment.create_dependency_map(self,
            socket=dict(provider='zmq.socket.%s' % self.queue_name)
        )

    def process(self, baton):
        response = util.dict_get_path(baton, self.response_path)
        assert response is not None, "provide a response if you expect something sensible from this processor"
        message = self._make_http_response(response)
        self.dependencies.socket.send(message, flags=zmq.NOBLOCK)

        if self.close:
            response = dict(response) # Don't empty the body of the original response dict.
            response['body'] = ''
            close_message = self._make_close_response(response)
            self.dependencies.socket.send(close_message)
        return baton

    @classmethod
    def _make_http_response(cls, response):
        response = dict(response)
        uuid = response.pop('uuid')
        idents = ' '.join(response.pop('idents'))
        msg = http_response(**response)
        payload = dict(uuid=uuid, ident_length=len(idents), idents=idents, msg=msg)
        return "%(uuid)s %(ident_length)i:%(idents)s, %(msg)s" % payload

    @classmethod
    def _make_close_response(cls, response):
        response = dict(response)
        uuid = response.pop('uuid')
        idents = ' '.join(response.pop('idents'))
        payload = dict(uuid=uuid, ident_length=len(idents), idents=idents, msg='')
        return "%(uuid)s %(ident_length)i:%(idents)s, " % payload
