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
    public class When_Assembly_Scanning_Finds_IEvent_Publications
    {
        private readonly IAmASchemaGenerator _schemaGenerator;

        public When_Assembly_Scanning_Finds_IEvent_Publications()
        {
            _schemaGenerator = A.Fake<IAmASchemaGenerator>();
            using var doc = JsonDocument.Parse("{\"type\":\"object\"}");
            A.CallTo(() => _schemaGenerator.GenerateAsync(A<Type?>.Ignored, A<CancellationToken>.Ignored))
                .Returns(Task.FromResult<V3SchemaDefinition?>(new V3SchemaDefinition
                {
                    SchemaFormat = "application/schema+json;version=draft-07",
                    Schema = doc.RootElement.Clone()
                }));
        }

        [Fact]
        public async Task It_Should_Discover_IEvent_Types_With_PublicationTopic_Attribute()
        {
            var options = new AsyncApiOptions
            {
                Title = "Test API",
                Version = "1.0.0",
                AssembliesToScan = new[] { typeof(ScannableIEventOnly).Assembly }
            };

            var generator = new AsyncApiDocumentGenerator(options, _schemaGenerator, null, null, NullLogger.Instance);
            var result = await generator.GenerateAsync();

            Assert.True(result.Channels.ContainsKey("ievent_only_topic"));
            Assert.Equal("ievent.only.topic", result.Channels["ievent_only_topic"].Address);

            Assert.True(result.Operations.ContainsKey("send_ievent_only_topic"));
            Assert.Equal(V3OperationAction.Send, result.Operations["send_ievent_only_topic"].Action);

            Assert.NotNull(result.Components?.Messages);
            Assert.True(result.Components.Messages.ContainsKey("ScannableIEventOnly"));
        }

        [PublicationTopic("ievent.only.topic")]
        public class ScannableIEventOnly : IEvent
        {
            public Id Id { get; set; } = Id.Random();
            public Id? CorrelationId { get; set; }
            public string Data { get; set; } = string.Empty;
        }
    }
}
