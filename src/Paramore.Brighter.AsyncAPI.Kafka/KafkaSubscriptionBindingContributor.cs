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

using Json.Schema;
using Neuroglia.AsyncApi.Bindings;
using Neuroglia.AsyncApi.Bindings.Kafka;
using Paramore.Brighter.MessagingGateway.Kafka;

namespace Paramore.Brighter.AsyncAPI.Kafka
{
    /// <summary>
    /// An <see cref="IAmASubscriptionBindingContributor"/> that populates Kafka channel and
    /// operation bindings on the generated AsyncAPI document for <see cref="KafkaSubscription"/>
    /// subscriptions.
    /// </summary>
    public sealed class KafkaSubscriptionBindingContributor : IAmASubscriptionBindingContributor
    {
        private const string KafkaBindingVersion = "0.5.0";

        /// <inheritdoc />
        public bool CanContribute(Subscription subscription) => subscription is KafkaSubscription;

        /// <inheritdoc />
        public void Contribute(Subscription subscription, SubscriptionBindingContext context)
        {
            if (subscription is not KafkaSubscription kafkaSubscription)
            {
                return;
            }

            context.Channel.Bindings ??= new ChannelBindingDefinitionCollection();
            context.Channel.Bindings.Kafka = new KafkaChannelBindingDefinition
            {
                Partitions = kafkaSubscription.NumPartitions >= 0 ? (uint)kafkaSubscription.NumPartitions : null,
                Replicas = kafkaSubscription.ReplicationFactor >= 0 ? (uint)kafkaSubscription.ReplicationFactor : null,
                BindingVersion = KafkaBindingVersion
            };

            context.Operation.Bindings ??= new OperationBindingDefinitionCollection();
            context.Operation.Bindings.Kafka = new KafkaOperationBindingDefinition
            {
                GroupId = BuildConstSchema(kafkaSubscription.GroupId),
                ClientId = BuildConstSchema(kafkaSubscription.Name?.Value),
                BindingVersion = KafkaBindingVersion
            };
        }

        private static JsonSchema? BuildConstSchema(string? value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return null;
            }

            return new JsonSchemaBuilder()
                .Type(SchemaValueType.String)
                .Const(value)
                .Build();
        }
    }
}
