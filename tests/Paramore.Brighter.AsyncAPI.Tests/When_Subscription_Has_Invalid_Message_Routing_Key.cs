#region Licence
/* The MIT License (MIT)
Copyright © 2026 Jonny Olliff-Lee <jonny.ollifflee@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FakeItEasy;
using Microsoft.Extensions.Logging.Abstractions;
using Neuroglia.AsyncApi.v3;
using Xunit;

namespace Paramore.Brighter.AsyncAPI.Tests
{
    public class When_Subscription_Has_Invalid_Message_Routing_Key
    {
        private readonly IAmASchemaGenerator _schemaGenerator;
        private readonly AsyncApiOptions _options;

        public When_Subscription_Has_Invalid_Message_Routing_Key()
        {
            _schemaGenerator = A.Fake<IAmASchemaGenerator>();
            using var doc = JsonDocument.Parse("{\"type\":\"object\"}");
            var schema = new V3SchemaDefinition
            {
                SchemaFormat = "application/schema+json;version=draft-07",
                Schema = doc.RootElement.Clone()
            };
            A.CallTo(() => _schemaGenerator.GenerateAsync(A<Type?>.Ignored, A<CancellationToken>.Ignored))
                .Returns(Task.FromResult<V3SchemaDefinition?>(schema));

            _options = new AsyncApiOptions
            {
                Title = "Test API",
                Version = "1.0.0",
                DisableAssemblyScanning = true
            };
        }

        [Fact]
        public async Task It_Should_Emit_Invalid_Message_Channel_And_Receive_Operation_With_Extensions()
        {
            var subscriptions = new[]
            {
                new InMemorySubscription(
                    new SubscriptionName("orders-consumer"),
                    new ChannelName("orders"),
                    new RoutingKey("order.created"),
                    requestType: typeof(TestEvent),
                    messagePumpType: MessagePumpType.Reactor)
                {
                    InvalidMessageRoutingKey = new RoutingKey("order.created.invalid")
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, null, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            Assert.True(result.Channels.ContainsKey("order_created_invalid"));
            Assert.Equal("order.created.invalid", result.Channels["order_created_invalid"].Address);
            Assert.Equal("Invalid-message channel for order.created", result.Channels["order_created_invalid"].Description);

            Assert.True(result.Operations.ContainsKey("receive_order_created_invalid"));
            Assert.Equal(V3OperationAction.Receive, result.Operations["receive_order_created_invalid"].Action);

            Assert.True(generator.ChannelExtensions.ContainsKey("order_created_invalid"));
            var invalidExtensions = generator.ChannelExtensions["order_created_invalid"];
            Assert.Equal("invalid-message", invalidExtensions["x-brighter-channel-role"]);
            Assert.Equal("order_created", invalidExtensions["x-brighter-source-channel"]);
        }

        [Fact]
        public async Task It_Should_Not_Emit_Invalid_Message_Channel_When_Routing_Key_Is_Null()
        {
            var subscriptions = new[]
            {
                new InMemorySubscription(
                    new SubscriptionName("orders-consumer"),
                    new ChannelName("orders"),
                    new RoutingKey("order.created"),
                    requestType: typeof(TestEvent),
                    messagePumpType: MessagePumpType.Reactor)
                {
                    InvalidMessageRoutingKey = null,
                    DeadLetterRoutingKey = null
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, null, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            Assert.Single(result.Channels);
            Assert.True(result.Channels.ContainsKey("order_created"));
        }

        [Fact]
        public async Task It_Should_Not_Emit_Invalid_Message_Channel_When_Routing_Key_Is_Empty()
        {
            var subscriptions = new[]
            {
                new InMemorySubscription(
                    new SubscriptionName("orders-consumer"),
                    new ChannelName("orders"),
                    new RoutingKey("order.created"),
                    requestType: typeof(TestEvent),
                    messagePumpType: MessagePumpType.Reactor)
                {
                    InvalidMessageRoutingKey = new RoutingKey(""),
                    DeadLetterRoutingKey = null
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, null, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            Assert.Single(result.Channels);
        }

        [Fact]
        public async Task It_Should_Deduplicate_Shared_Invalid_Message_Channel_Across_Subscriptions()
        {
            var subscriptions = new[]
            {
                new InMemorySubscription(
                    new SubscriptionName("orders-consumer"),
                    new ChannelName("orders"),
                    new RoutingKey("order.created"),
                    requestType: typeof(TestEvent),
                    messagePumpType: MessagePumpType.Reactor)
                {
                    InvalidMessageRoutingKey = new RoutingKey("shared.invalid"),
                    DeadLetterRoutingKey = null
                },
                new InMemorySubscription(
                    new SubscriptionName("shipments-consumer"),
                    new ChannelName("shipments"),
                    new RoutingKey("shipment.created"),
                    requestType: typeof(OtherEvent),
                    messagePumpType: MessagePumpType.Reactor)
                {
                    InvalidMessageRoutingKey = new RoutingKey("shared.invalid"),
                    DeadLetterRoutingKey = null
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, null, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            var invalidChannelCount = 0;
            foreach (var kvp in result.Channels)
            {
                if (kvp.Value.Address == "shared.invalid") invalidChannelCount++;
            }
            Assert.Equal(1, invalidChannelCount);
        }

        [Fact]
        public async Task It_Should_Skip_Invalid_Message_Emission_For_Subscription_Without_Invalid_Message_Support()
        {
            var subscriptions = new[]
            {
                new Subscription(
                    new SubscriptionName("orders-consumer"),
                    new ChannelName("orders"),
                    new RoutingKey("order.created"),
                    requestType: typeof(TestEvent),
                    messagePumpType: MessagePumpType.Reactor)
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, null, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            Assert.Single(result.Channels);
            Assert.True(result.Channels.ContainsKey("order_created"));
        }

        [Fact]
        public async Task It_Should_Emit_Both_Dead_Letter_And_Invalid_Message_Channels_When_Both_Routing_Keys_Are_Set()
        {
            var subscriptions = new[]
            {
                new InMemorySubscription(
                    new SubscriptionName("orders-consumer"),
                    new ChannelName("orders"),
                    new RoutingKey("order.created"),
                    requestType: typeof(TestEvent),
                    messagePumpType: MessagePumpType.Reactor)
                {
                    DeadLetterRoutingKey = new RoutingKey("order.created.dlq"),
                    InvalidMessageRoutingKey = new RoutingKey("order.created.invalid")
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, null, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            Assert.True(result.Channels.ContainsKey("order_created_dlq"));
            Assert.True(result.Channels.ContainsKey("order_created_invalid"));

            Assert.Equal("dead-letter", generator.ChannelExtensions["order_created_dlq"]["x-brighter-channel-role"]);
            Assert.Equal("invalid-message", generator.ChannelExtensions["order_created_invalid"]["x-brighter-channel-role"]);
        }

        public class TestEvent : Event
        {
            public TestEvent() : base(Guid.NewGuid()) { }
        }

        public class OtherEvent : Event
        {
            public OtherEvent() : base(Guid.NewGuid()) { }
        }
    }
}
