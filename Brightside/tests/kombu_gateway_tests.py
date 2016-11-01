#!/usr/bin/env python
""""
File         : kombu_gateway_tests.py
Author           : ian
Created          : 09-01-2016

Last Modified By : ian
Last Modified On : 09-01-2016
***********************************************************************
The MIT License (MIT)
Copyright © 2015 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
**********************************************************************i*s
"""

import unittest
from kombu_brighter.kombu_gateway import KombuConsumer, KombuConnection, KombuProducer
from core.messaging import Message, MessageBody, MessageHeader, MessageType
from uuid import uuid4


class KombuGatewayTests(unittest.TestCase):

    test_topic = "kombu_gateway_tests"

    def setUp(self):
        self._connection = KombuConnection("amqp://guest:guest@localhost:5672//", "paramore.brighter.exchange")
        self._producer = KombuProducer(self._connection)
        self._consumer = KombuConsumer(self._connection, "brightside_tests", self.test_topic)

    def test_posting_a_message(self):
        """Given that I have an RMQ message producer
            when I send that message via the produecer
            then I should be able to read that message via the consumer
        """
        header = MessageHeader(uuid4(), self.test_topic, MessageType.command, uuid4())
        body = MessageBody("test content")
        message = Message(header, body)

        # self._consumer.purge()

        self._producer.send(message)

        read_message = self._consumer.receive(500)

        self.assertEqual(message.id, read_message.id)
        self.assertEqual(message.body.value, read_message.body.value)

