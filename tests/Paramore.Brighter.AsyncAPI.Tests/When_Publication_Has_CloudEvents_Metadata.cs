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
    public class When_Publication_Has_CloudEvents_Metadata
    {
        private readonly IAmASchemaGenerator _schemaGenerator;
        private readonly AsyncApiOptions _options;

        public When_Publication_Has_CloudEvents_Metadata()
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
        public async Task It_Should_Emit_All_Four_CloudEvents_Extensions_When_Publication_Sets_All_Fields()
        {
            var publications = new[]
            {
                new Publication
                {
                    Topic = new RoutingKey("order.created"),
                    RequestType = typeof(OrderCreated),
                    Type = new CloudEventsType("com.example.order.created"),
                    Source = new Uri("https://orders.example.com/service"),
                    Subject = "order-1234",
                    DataSchema = new Uri("https://schemas.example.com/order/v1.json")
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, null, publications, NullLogger.Instance);

            await generator.GenerateAsync();

            Assert.True(generator.MessageExtensions.ContainsKey(nameof(OrderCreated)));
            var extensions = generator.MessageExtensions[nameof(OrderCreated)];
            Assert.Equal("com.example.order.created", extensions["x-cloudevents-type"]);
            Assert.Equal("https://orders.example.com/service", extensions["x-cloudevents-source"]);
            Assert.Equal("order-1234", extensions["x-cloudevents-subject"]);
            Assert.Equal("https://schemas.example.com/order/v1.json", extensions["x-cloudevents-dataschema"]);
        }

        [Fact]
        public async Task It_Should_Omit_Extensions_For_Default_Or_Null_Values()
        {
            var publications = new[]
            {
                new Publication
                {
                    Topic = new RoutingKey("order.created"),
                    RequestType = typeof(OrderCreated)
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, null, publications, NullLogger.Instance);

            await generator.GenerateAsync();

            var hasExtensions = generator.MessageExtensions.TryGetValue(nameof(OrderCreated), out var extensions);
            if (hasExtensions)
            {
                Assert.False(extensions!.ContainsKey("x-cloudevents-type"));
                Assert.False(extensions.ContainsKey("x-cloudevents-source"));
                Assert.False(extensions.ContainsKey("x-cloudevents-subject"));
                Assert.False(extensions.ContainsKey("x-cloudevents-dataschema"));
            }
        }

        [Fact]
        public async Task It_Should_Not_Emit_Message_Extensions_For_Subscription_Only_Paths()
        {
            var subscriptions = new[]
            {
                new Subscription(
                    new SubscriptionName("orders-consumer"),
                    new ChannelName("orders"),
                    new RoutingKey("order.created"),
                    requestType: typeof(OrderCreated),
                    messagePumpType: MessagePumpType.Reactor)
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, null, NullLogger.Instance);

            await generator.GenerateAsync();

            Assert.False(generator.MessageExtensions.ContainsKey(nameof(OrderCreated)));
        }

        [Fact]
        public async Task It_Should_Set_Default_ContentType_Of_Application_Json()
        {
            var publications = new[]
            {
                new Publication
                {
                    Topic = new RoutingKey("order.created"),
                    RequestType = typeof(OrderCreated),
                    Type = new CloudEventsType("com.example.order.created")
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, null, publications, NullLogger.Instance);

            var document = await generator.GenerateAsync();

            Assert.NotNull(document.Components);
            Assert.NotNull(document.Components!.Messages);
            var message = document.Components.Messages![nameof(OrderCreated)];
            Assert.Equal("application/json", message.ContentType);
        }

        [Fact]
        public async Task It_Should_Emit_CloudEvents_Extensions_On_Shared_Message_When_Both_Subscription_And_Publication_Present()
        {
            var subscriptions = new[]
            {
                new Subscription(
                    new SubscriptionName("orders-consumer"),
                    new ChannelName("orders"),
                    new RoutingKey("order.created"),
                    requestType: typeof(OrderCreated),
                    messagePumpType: MessagePumpType.Reactor)
            };

            var publications = new[]
            {
                new Publication
                {
                    Topic = new RoutingKey("order.created"),
                    RequestType = typeof(OrderCreated),
                    Type = new CloudEventsType("com.example.order.created"),
                    Subject = "order-shared"
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, subscriptions, publications, NullLogger.Instance);

            await generator.GenerateAsync();

            Assert.True(generator.MessageExtensions.ContainsKey(nameof(OrderCreated)));
            var extensions = generator.MessageExtensions[nameof(OrderCreated)];
            Assert.Equal("com.example.order.created", extensions["x-cloudevents-type"]);
            Assert.Equal("order-shared", extensions["x-cloudevents-subject"]);
        }

        public class OrderCreated : Event
        {
            public OrderCreated() : base(Guid.NewGuid()) { }
        }
    }
}
