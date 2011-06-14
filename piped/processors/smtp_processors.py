# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from email import message

from zope import interface
from twisted.internet import defer

from piped import util, processing, exceptions
from piped.processors import base
from piped.providers import smtp_provider


class CreateEmailMessage(base.Processor):
    """ Creates an instance of :class:`email.message.Message`.
    """
    interface.classProvides(processing.IProcessor)
    name = 'create-email-message'

    def __init__(self, output_path='message', payload_path=None, headers=None, **kw):
        """
        :param output_path: The path in the baton where the :class:`email.message.Message`
            should be stored.

        :param payload_path: The path to the payload in the baton.

        :param headers: A dict of initial headers to set in the email message.

            Example:

            .. code-block:: yaml

                - create-email-message:
                    ...
                    headers:
                        Subject: my-subject
                        From: Sender Name <sender@example.com>
                        To: Recipient Name <recipient@example.com>
        """
        super(CreateEmailMessage, self).__init__(**kw)
        
        self.output_path = output_path
        self.payload_path = payload_path
        self.headers = headers or dict()

    def process(self, baton):
        msg = message.Message()

        for key, value in self.headers.items():
            msg.add_header(key, value)

        if self.payload_path:
            payload = util.dict_get_path(baton, self.payload_path)
            
            if hasattr(payload, 'read'):
                payload = payload.read()

            msg.set_payload(payload)

        if self.output_path == '':
            return msg

        util.dict_set_path(baton, self.output_path, msg)

        return baton


class SetMessageHeaders(base.Processor):
    """ Set or replace message headers of an :class:`email.message.Message`.

    If the header already exists in the message, it will be replaced, otherwise
    it will be added.
    """
    interface.classProvides(processing.IProcessor)
    name = 'replace-email-headers'

    def __init__(self, headers, message_path='message', **kw):
        """
        :param message_path: The path to the :class:`email.message.Message` in
            the baton.

        :param headers: A :class:`dict` of headers and their values which should be set
        """
        super(SetMessageHeaders, self).__init__(**kw)
        self.message_path = message_path

        self.headers = headers

    def process(self, baton):
        message = util.dict_get_path(baton, self.message_path)

        for key, value in self.headers.items():
            if key in message:
                message.replace_header(key, value)
            else:
                message.add_header(key, value)

        return baton


class SendEmail(base.Processor):
    """ Send an email. """
    interface.classProvides(processing.IProcessor)
    name = 'send-email'

    def __init__(self, message_path='message', configuration=None, **kw):
        """
        :param message_path: The path to the message in the baton.

        :param from: The email address of the sender.
        :param from_path: The path to the senders email address in the baton.
            Only one of ``from`` and ``from_path`` may be used.

        :param to: The email address of the recipient.
        :param to_path: The path to the recipients email address in the baton.
            Only one of ``to`` and ``to_path`` may be used.

        :param configuration:
            The configuration argument may contain arguments accepted by
            :meth:`~piped.providers.smtp_provider.send_mail`, except
            ``from_addr``, ``to_addr`` and ``file`` which is provided by
            this processor:

            .. automethod:: piped.providers.smtp_provider.send_mail
                :noindex:
        """
        self.from_addr = kw.pop('from', None)
        self.from_path = kw.pop('from_path', None)

        self.to_addr = kw.pop('to', None)
        self.to_path = kw.pop('to_path', None)

        self._fail_if_from_to_not_properly_configured()
        
        super(SendEmail, self).__init__(**kw)

        self.message_path = message_path

        self.configuration = configuration or dict()

    def _fail_if_from_to_not_properly_configured(self):
        if (self.from_addr and self.from_path) or not (self.from_addr or self.from_path):
            e_msg = 'Invalid "from"-configuration.'
            detail = 'Either "from" or "from_path" must be used.'
            raise exceptions.ConfigurationError(e_msg, detail)

        if (self.to_addr and self.to_path) or not (self.to_addr or self.to_path):
            e_msg = 'Invalid "to"-configuration.'
            detail = 'Either "to" or "to_path" must be used.'
            raise exceptions.ConfigurationError(e_msg, detail)

    @defer.inlineCallbacks
    def process(self, baton):
        message = util.dict_get_path(baton, self.message_path)

        from_addr = self.from_addr
        if not from_addr:
            from_addr = util.dict_get_path(baton, self.from_path)

        to_addr = self.to_addr
        if not to_addr:
            to_addr = util.dict_get_path(baton, self.to_path)

        yield smtp_provider.send_mail(from_addr, to_addr, message, **self.configuration)

        defer.returnValue(baton)
