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
    public class When_Subscription_Has_Dead_Letter_Routing_Key
    {
        private readonly IAmASchemaGenerator _schemaGenerator;
        private readonly AsyncApiOptions _options;

        public When_Subscription_Has_Dead_Letter_Routing_Key()
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
        public async Task It_Should_Emit_Dead_Letter_Channel_And_Receive_Operation_With_Extensions()
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
                    DeadLetterRoutingKey = new RoutingKey("order.created.dlq")
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, null, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            Assert.True(result.Channels.ContainsKey("order_created_dlq"));
            Assert.Equal("order.created.dlq", result.Channels["order_created_dlq"].Address);
            Assert.Equal("Dead-letter channel for order.created", result.Channels["order_created_dlq"].Description);

            Assert.True(result.Operations.ContainsKey("receive_order_created_dlq"));
            Assert.Equal(V3OperationAction.Receive, result.Operations["receive_order_created_dlq"].Action);

            Assert.True(generator.ChannelExtensions.ContainsKey("order_created_dlq"));
            var dlqExtensions = generator.ChannelExtensions["order_created_dlq"];
            Assert.Equal("dead-letter", dlqExtensions["x-brighter-channel-role"]);
            Assert.Equal("order_created", dlqExtensions["x-brighter-source-channel"]);
        }

        [Fact]
        public async Task It_Should_Not_Emit_Dead_Letter_Channel_When_Routing_Key_Is_Null()
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
        public async Task It_Should_Not_Emit_Dead_Letter_Channel_When_Routing_Key_Is_Empty()
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
                    DeadLetterRoutingKey = new RoutingKey("")
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, null, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            Assert.Single(result.Channels);
        }

        [Fact]
        public async Task It_Should_Deduplicate_Shared_Dead_Letter_Channel_Across_Subscriptions()
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
                    DeadLetterRoutingKey = new RoutingKey("shared.dlq")
                },
                new InMemorySubscription(
                    new SubscriptionName("shipments-consumer"),
                    new ChannelName("shipments"),
                    new RoutingKey("shipment.created"),
                    requestType: typeof(OtherEvent),
                    messagePumpType: MessagePumpType.Reactor)
                {
                    DeadLetterRoutingKey = new RoutingKey("shared.dlq")
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, null, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            var dlqChannelCount = 0;
            foreach (var kvp in result.Channels)
            {
                if (kvp.Value.Address == "shared.dlq") dlqChannelCount++;
            }
            Assert.Equal(1, dlqChannelCount);
        }

        [Fact]
        public async Task It_Should_Skip_Dlq_Emission_For_Subscription_Without_Dead_Letter_Support()
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
