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
    public class When_Channel_Is_Shared_Between_Subscription_And_Publication
    {
        private readonly IAmASchemaGenerator _schemaGenerator;
        private readonly AsyncApiOptions _options;

        public When_Channel_Is_Shared_Between_Subscription_And_Publication()
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
        public async Task It_Should_Compose_Description_And_Merge_Extensions_When_Subscription_First_Then_Publication()
        {
            var subscriptions = new[]
            {
                new Subscription(
                    new SubscriptionName("orders-consumer"),
                    new ChannelName("orders"),
                    new RoutingKey("order.created"),
                    requestType: typeof(SharedEvent),
                    messagePumpType: MessagePumpType.Reactor)
            };

            var publications = new[]
            {
                new Publication
                {
                    Topic = new RoutingKey("order.created"),
                    RequestType = typeof(SharedEvent),
                    Type = new CloudEventsType("com.example.order.created")
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, publications, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            Assert.Single(result.Channels);
            Assert.True(result.Channels.ContainsKey("order_created"));

            var channel = result.Channels["order_created"];
            Assert.Equal("Published by com.example.order.created; consumed by orders-consumer", channel.Description);

            Assert.True(generator.ChannelExtensions.ContainsKey("order_created"));
            var extensions = generator.ChannelExtensions["order_created"];
            Assert.Equal("com.example.order.created", extensions["x-brighter-producer-cloudevents-type"]);
            Assert.Equal("orders-consumer", extensions["x-brighter-consumer-subscription-name"]);

            Assert.Equal(2, result.Operations.Count);
            Assert.True(result.Operations.ContainsKey("receive_order_created"));
            Assert.True(result.Operations.ContainsKey("send_order_created"));
        }

        [Fact]
        public async Task It_Should_Use_RequestType_Name_When_CloudEvents_Type_Is_Empty()
        {
            var subscriptions = new[]
            {
                new Subscription(
                    new SubscriptionName("orders-consumer"),
                    new ChannelName("orders"),
                    new RoutingKey("order.created"),
                    requestType: typeof(SharedEvent),
                    messagePumpType: MessagePumpType.Reactor)
            };

            var publications = new[]
            {
                new Publication
                {
                    Topic = new RoutingKey("order.created"),
                    RequestType = typeof(SharedEvent)
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, publications, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            var channel = result.Channels["order_created"];
            Assert.Equal("Published by SharedEvent; consumed by orders-consumer", channel.Description);

            var extensions = generator.ChannelExtensions["order_created"];
            Assert.Equal("SharedEvent", extensions["x-brighter-producer-cloudevents-type"]);
            Assert.Equal("orders-consumer", extensions["x-brighter-consumer-subscription-name"]);
        }

        [Fact]
        public async Task It_Should_Emit_Single_Sided_Description_When_Only_Subscription()
        {
            var subscriptions = new[]
            {
                new Subscription(
                    new SubscriptionName("orders-consumer"),
                    new ChannelName("orders"),
                    new RoutingKey("order.created"),
                    requestType: typeof(SharedEvent),
                    messagePumpType: MessagePumpType.Reactor)
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, null, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            var channel = result.Channels["order_created"];
            Assert.Equal("Consumed by orders-consumer", channel.Description);

            var extensions = generator.ChannelExtensions["order_created"];
            Assert.Equal("orders-consumer", extensions["x-brighter-consumer-subscription-name"]);
            Assert.False(extensions.ContainsKey("x-brighter-producer-cloudevents-type"));
        }

        [Fact]
        public async Task It_Should_Emit_Single_Sided_Description_When_Only_Publication()
        {
            var publications = new[]
            {
                new Publication
                {
                    Topic = new RoutingKey("order.created"),
                    RequestType = typeof(SharedEvent),
                    Type = new CloudEventsType("com.example.order.created")
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, null, publications, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            var channel = result.Channels["order_created"];
            Assert.Equal("Published by com.example.order.created", channel.Description);

            var extensions = generator.ChannelExtensions["order_created"];
            Assert.Equal("com.example.order.created", extensions["x-brighter-producer-cloudevents-type"]);
            Assert.False(extensions.ContainsKey("x-brighter-consumer-subscription-name"));
        }

        [Fact]
        public async Task It_Should_Compose_When_Publication_First_Then_Subscription()
        {
            var subscriptions = new[]
            {
                new Subscription(
                    new SubscriptionName("orders-consumer"),
                    new ChannelName("orders"),
                    new RoutingKey("order.created"),
                    requestType: typeof(SharedEvent),
                    messagePumpType: MessagePumpType.Reactor)
            };

            var publications = new[]
            {
                new Publication
                {
                    Topic = new RoutingKey("order.created"),
                    RequestType = typeof(SharedEvent),
                    Type = new CloudEventsType("com.example.order.created")
                }
            };

            // AddSubscriptionsAsync always runs before AddPublicationsAsync, so this test
            // exists to document the behaviour: the final composed description holds both
            // sides regardless of source order within each collection.
            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, publications, NullLogger.Instance);

            var result = await generator.GenerateAsync();

            var channel = result.Channels["order_created"];
            Assert.Equal("Published by com.example.order.created; consumed by orders-consumer", channel.Description);
        }

        public class SharedEvent : Event
        {
            public SharedEvent() : base(Guid.NewGuid()) { }
        }
    }
}
