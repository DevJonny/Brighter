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
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FakeItEasy;
using Microsoft.Extensions.Logging.Abstractions;
using Neuroglia.AsyncApi.v3;
using Xunit;

namespace Paramore.Brighter.AsyncAPI.Tests
{
    public class When_Publication_Has_Default_Headers
    {
        private readonly IAmASchemaGenerator _schemaGenerator;
        private readonly AsyncApiOptions _options;

        public When_Publication_Has_Default_Headers()
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
        public async Task It_Should_Emit_CloudEvents_Required_Keys_And_Custom_Headers_With_Inferred_Types()
        {
            var publications = new[]
            {
                new Publication
                {
                    Topic = new RoutingKey("order.created"),
                    RequestType = typeof(OrderCreated),
                    DefaultHeaders = new Dictionary<string, object>
                    {
                        ["x-correlation-id"] = "abc-123",
                        ["x-retry-count"] = 3,
                        ["x-flagged"] = true,
                        ["x-score"] = 4.5
                    }
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, null, publications, NullLogger.Instance);

            var document = await generator.GenerateAsync();

            Assert.NotNull(document.Components);
            Assert.NotNull(document.Components!.Messages);
            var message = document.Components.Messages![nameof(OrderCreated)];
            Assert.NotNull(message.Headers);

            var headersSchema = (JsonElement)message.Headers!.Schema!;
            var properties = headersSchema.GetProperty("properties");

            Assert.Equal("string", properties.GetProperty("id").GetProperty("type").GetString());
            Assert.Equal("string", properties.GetProperty("source").GetProperty("type").GetString());
            Assert.Equal("uri", properties.GetProperty("source").GetProperty("format").GetString());
            Assert.Equal("string", properties.GetProperty("type").GetProperty("type").GetString());
            Assert.Equal("string", properties.GetProperty("specversion").GetProperty("type").GetString());
            Assert.Equal("string", properties.GetProperty("time").GetProperty("type").GetString());
            Assert.Equal("date-time", properties.GetProperty("time").GetProperty("format").GetString());

            Assert.Equal("string", properties.GetProperty("x-correlation-id").GetProperty("type").GetString());
            Assert.Equal("integer", properties.GetProperty("x-retry-count").GetProperty("type").GetString());
            Assert.Equal("boolean", properties.GetProperty("x-flagged").GetProperty("type").GetString());
            Assert.Equal("number", properties.GetProperty("x-score").GetProperty("type").GetString());

            var required = headersSchema.GetProperty("required")
                .EnumerateArray()
                .Select(e => e.GetString())
                .ToArray();
            Assert.Contains("id", required);
            Assert.Contains("source", required);
            Assert.Contains("type", required);
            Assert.Contains("specversion", required);
        }

        [Fact]
        public async Task It_Should_Set_Headers_To_CloudEvents_Only_When_DefaultHeaders_Is_Null()
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

            var document = await generator.GenerateAsync();

            var message = document.Components!.Messages![nameof(OrderCreated)];
            Assert.NotNull(message.Headers);

            var headersSchema = (JsonElement)message.Headers!.Schema!;
            var properties = headersSchema.GetProperty("properties");
            Assert.True(properties.TryGetProperty("id", out _));
            Assert.True(properties.TryGetProperty("source", out _));
            Assert.True(properties.TryGetProperty("type", out _));
            Assert.True(properties.TryGetProperty("specversion", out _));
            Assert.True(properties.TryGetProperty("time", out _));
        }

        [Fact]
        public async Task It_Should_Leave_Headers_Null_For_Subscription_Only_Messages()
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

            var document = await generator.GenerateAsync();

            var message = document.Components!.Messages![nameof(OrderCreated)];
            Assert.Null(message.Headers);
        }

        [Fact]
        public async Task It_Should_Not_Overwrite_CloudEvents_Required_Properties_If_DefaultHeaders_Uses_Same_Key()
        {
            var publications = new[]
            {
                new Publication
                {
                    Topic = new RoutingKey("order.created"),
                    RequestType = typeof(OrderCreated),
                    DefaultHeaders = new Dictionary<string, object>
                    {
                        ["id"] = 42,
                        ["source"] = 99
                    }
                }
            };

            var generator = new AsyncApiDocumentGenerator(
                _options, _schemaGenerator, null, publications, NullLogger.Instance);

            var document = await generator.GenerateAsync();

            var message = document.Components!.Messages![nameof(OrderCreated)];
            var headersSchema = (JsonElement)message.Headers!.Schema!;
            var properties = headersSchema.GetProperty("properties");

            Assert.Equal("string", properties.GetProperty("id").GetProperty("type").GetString());
            Assert.Equal("string", properties.GetProperty("source").GetProperty("type").GetString());
        }

        public class OrderCreated : Event
        {
            public OrderCreated() : base(Guid.NewGuid()) { }
        }
    }
}
