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
using System.Text.Json.Nodes;
using FakeItEasy;
using Json.Schema;
using Microsoft.Extensions.DependencyInjection;
using Neuroglia.AsyncApi.v3;
using Paramore.Brighter.AsyncAPI;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.Kafka;
using Xunit;

namespace Paramore.Brighter.AsyncAPI.Kafka.Tests
{
    public class When_Kafka_Subscription_Binding_Contributor_Runs
    {
        private static SubscriptionBindingContext NewContext() =>
            new(
                new V3ChannelDefinition { Address = "test-topic" },
                new V3OperationDefinition { Action = V3OperationAction.Receive },
                new Dictionary<string, object>(),
                new Dictionary<string, object>());

        [Fact]
        public void It_Should_CanContribute_For_KafkaSubscription()
        {
            var contributor = new KafkaSubscriptionBindingContributor();
            var kafkaSub = new KafkaSubscription<TestEvent>(
                subscriptionName: new SubscriptionName("kafka-consumer"),
                channelName: new ChannelName("test-topic"),
                routingKey: new RoutingKey("test-topic"),
                groupId: "test-group");

            Assert.True(contributor.CanContribute(kafkaSub));
        }

        [Fact]
        public void It_Should_Not_Contribute_For_Non_Kafka_Subscription()
        {
            var contributor = new KafkaSubscriptionBindingContributor();
            var subscription = new Subscription(
                new SubscriptionName("other"),
                new ChannelName("other"),
                new RoutingKey("other"),
                requestType: typeof(TestEvent),
                messagePumpType: MessagePumpType.Reactor);

            Assert.False(contributor.CanContribute(subscription));
        }

        [Fact]
        public void It_Should_Write_Kafka_Channel_Binding_With_Partitions_And_Replicas()
        {
            var contributor = new KafkaSubscriptionBindingContributor();
            var subscription = new KafkaSubscription<TestEvent>(
                subscriptionName: new SubscriptionName("kafka-consumer"),
                channelName: new ChannelName("test-topic"),
                routingKey: new RoutingKey("test-topic"),
                groupId: "test-group",
                numOfPartitions: 3,
                replicationFactor: 2);

            var context = NewContext();

            contributor.Contribute(subscription, context);

            Assert.NotNull(context.Channel.Bindings);
            var kafkaBinding = context.Channel.Bindings!.Kafka;
            Assert.NotNull(kafkaBinding);
            Assert.Equal((uint)3, kafkaBinding!.Partitions);
            Assert.Equal((uint)2, kafkaBinding.Replicas);
            Assert.Equal("0.5.0", kafkaBinding.BindingVersion);
        }

        [Fact]
        public void It_Should_Write_Kafka_Operation_Binding_With_GroupId_And_ClientId()
        {
            var contributor = new KafkaSubscriptionBindingContributor();
            var subscription = new KafkaSubscription<TestEvent>(
                subscriptionName: new SubscriptionName("kafka-consumer"),
                channelName: new ChannelName("test-topic"),
                routingKey: new RoutingKey("test-topic"),
                groupId: "orders-group");

            var context = NewContext();

            contributor.Contribute(subscription, context);

            Assert.NotNull(context.Operation.Bindings);
            var kafkaOp = context.Operation.Bindings!.Kafka;
            Assert.NotNull(kafkaOp);
            Assert.Equal("0.5.0", kafkaOp!.BindingVersion);
            Assert.Equal("orders-group", GetSchemaConstString(kafkaOp.GroupId));
            Assert.Equal("kafka-consumer", GetSchemaConstString(kafkaOp.ClientId));
        }

        [Fact]
        public void It_Should_Omit_GroupId_When_Not_Set()
        {
            var contributor = new KafkaSubscriptionBindingContributor();
            var subscription = new KafkaSubscription<TestEvent>(
                subscriptionName: new SubscriptionName("kafka-consumer"),
                channelName: new ChannelName("test-topic"),
                routingKey: new RoutingKey("test-topic"));

            var context = NewContext();

            contributor.Contribute(subscription, context);

            Assert.Null(context.Operation.Bindings!.Kafka!.GroupId);
            Assert.Equal("kafka-consumer", GetSchemaConstString(context.Operation.Bindings.Kafka.ClientId));
        }

        [Fact]
        public void It_Should_Be_Auto_Discovered_By_UseAsyncApi()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IAmASchemaGenerator>(A.Fake<IAmASchemaGenerator>());
            var builder = A.Fake<IBrighterBuilder>();
            A.CallTo(() => builder.Services).Returns(services);

            builder.UseAsyncApi();

            var provider = services.BuildServiceProvider();
            var contributors = provider.GetServices<IAmASubscriptionBindingContributor>().ToArray();

            Assert.Contains(contributors, c => c is KafkaSubscriptionBindingContributor);
        }

        [Fact]
        public void It_Should_Register_Via_UseAsyncApiKafkaBindings_Without_Duplication()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IAmASchemaGenerator>(A.Fake<IAmASchemaGenerator>());
            var builder = A.Fake<IBrighterBuilder>();
            A.CallTo(() => builder.Services).Returns(services);

            builder.UseAsyncApi();
            builder.UseAsyncApiKafkaBindings();
            builder.UseAsyncApiKafkaBindings();

            var provider = services.BuildServiceProvider();
            var contributors = provider.GetServices<IAmASubscriptionBindingContributor>().ToArray();

            Assert.Single(contributors, c => c is KafkaSubscriptionBindingContributor);
        }

        private static string? GetSchemaConstString(JsonSchema? schema)
        {
            if (schema == null)
            {
                return null;
            }

            var constKeyword = schema.Keywords?.OfType<ConstKeyword>().FirstOrDefault();
            return (constKeyword?.Value as JsonValue)?.GetValue<string>();
        }

        public class TestEvent : Event
        {
            public TestEvent() : base(Guid.NewGuid()) { }
        }
    }
}
