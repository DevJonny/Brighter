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
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using Neuroglia.AsyncApi.Bindings.Sns;
using Neuroglia.AsyncApi.Bindings.Sqs;
using Neuroglia.AsyncApi.v3;
using Paramore.Brighter.AsyncAPI;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.AWSSQS;
using Xunit;

namespace Paramore.Brighter.AsyncAPI.AWSSQS.Tests
{
    public class When_Sqs_Subscription_Binding_Contributor_Runs
    {
        private static SubscriptionBindingContext NewContext() =>
            new(
                new V3ChannelDefinition { Address = "orders.topic" },
                new V3OperationDefinition { Action = V3OperationAction.Receive },
                new Dictionary<string, object>(),
                new Dictionary<string, object>());

        private static SqsSubscription<TestEvent> NewPubSubSubscription(
            SqsAttributes? queueAttributes = null,
            SnsAttributes? topicAttributes = null) =>
            new(
                subscriptionName: new SubscriptionName("sqs-consumer"),
                channelName: new ChannelName("orders-queue"),
                channelType: ChannelType.PubSub,
                routingKey: new RoutingKey("orders.topic"),
                messagePumpType: MessagePumpType.Proactor,
                queueAttributes: queueAttributes,
                topicAttributes: topicAttributes);

        private static SqsSubscription<TestEvent> NewPointToPointSubscription(
            SqsAttributes? queueAttributes = null) =>
            new(
                subscriptionName: new SubscriptionName("sqs-consumer"),
                channelName: new ChannelName("orders-queue"),
                channelType: ChannelType.PointToPoint,
                routingKey: new RoutingKey("orders-queue"),
                messagePumpType: MessagePumpType.Proactor,
                queueAttributes: queueAttributes);

        [Fact]
        public void It_Should_CanContribute_For_SqsSubscription()
        {
            var contributor = new SqsSubscriptionBindingContributor();
            Assert.True(contributor.CanContribute(NewPubSubSubscription()));
        }

        [Fact]
        public void It_Should_Not_Contribute_For_Non_Sqs_Subscription()
        {
            var contributor = new SqsSubscriptionBindingContributor();
            var subscription = new Subscription(
                new SubscriptionName("other"),
                new ChannelName("other"),
                new RoutingKey("other"),
                requestType: typeof(TestEvent),
                messagePumpType: MessagePumpType.Reactor);

            Assert.False(contributor.CanContribute(subscription));
        }

        [Fact]
        public void It_Should_Emit_PubSub_Channel_Type_With_Sns_And_Sqs_Bindings()
        {
            var contributor = new SqsSubscriptionBindingContributor();
            var context = NewContext();

            contributor.Contribute(NewPubSubSubscription(), context);

            Assert.Equal(
                SqsSubscriptionBindingContributor.PubSubChannelType,
                context.ChannelExtensions[SqsSubscriptionBindingContributor.ChannelTypeExtensionKey]);

            Assert.NotNull(context.Channel.Bindings?.Sqs);
            Assert.NotNull(context.Channel.Bindings?.Sns);
            Assert.Equal("orders-queue", context.Channel.Bindings!.Sqs!.Queue!.Name);
            Assert.Equal("orders.topic", context.Channel.Bindings.Sns!.Name);
            Assert.Equal(SnsTopicOrderingType.Standard, context.Channel.Bindings.Sns.Ordering!.Type);
        }

        [Fact]
        public void It_Should_Emit_PointToPoint_Channel_Type_Without_Sns_Binding()
        {
            var contributor = new SqsSubscriptionBindingContributor();
            var context = NewContext();

            contributor.Contribute(NewPointToPointSubscription(), context);

            Assert.Equal(
                SqsSubscriptionBindingContributor.PointToPointChannelType,
                context.ChannelExtensions[SqsSubscriptionBindingContributor.ChannelTypeExtensionKey]);

            Assert.NotNull(context.Channel.Bindings?.Sqs);
            Assert.Null(context.Channel.Bindings!.Sns);
        }

        [Fact]
        public void It_Should_Project_SqsAttributes_Onto_Queue_Binding()
        {
            var contributor = new SqsSubscriptionBindingContributor();
            var context = NewContext();
            var attributes = new SqsAttributes(
                lockTimeout: TimeSpan.FromSeconds(120),
                delaySeconds: TimeSpan.FromSeconds(10),
                timeOut: TimeSpan.FromSeconds(5),
                messageRetentionPeriod: TimeSpan.FromDays(2),
                type: SqsType.Fifo,
                deduplicationScope: DeduplicationScope.MessageGroup,
                fifoThroughputLimit: FifoThroughputLimit.PerMessageGroupId,
                rawMessageDelivery: false);

            contributor.Contribute(
                NewPubSubSubscription(
                    queueAttributes: attributes,
                    topicAttributes: new SnsAttributes(type: SqsType.Fifo)),
                context);

            var queue = context.Channel.Bindings!.Sqs!.Queue!;
            Assert.True(queue.FifoQueue);
            Assert.Equal(120, queue.VisibilityTimeout);
            Assert.Equal(10, queue.DeliveryDelay);
            Assert.Equal(5, queue.ReceiveMessageWaitTime);
            Assert.Equal((int)TimeSpan.FromDays(2).TotalSeconds, queue.MessageRetentionPeriod);
            Assert.Equal(SqsDeduplicationScope.MessageGroup, queue.DeduplicationScope);
            Assert.Equal(SqsFifoThroughputLimit.PerMessageGroupId, queue.FifoThroughputLimit);
            Assert.Equal(
                false,
                context.OperationExtensions[SqsSubscriptionBindingContributor.RawMessageDeliveryExtensionKey]);
        }

        [Fact]
        public void It_Should_Project_RedrivePolicy_Onto_Queue_Binding()
        {
            var contributor = new SqsSubscriptionBindingContributor();
            var context = NewContext();
            var attributes = new SqsAttributes(
                redrivePolicy: new RedrivePolicy(new ChannelName("orders-dlq"), 7));

            contributor.Contribute(NewPubSubSubscription(queueAttributes: attributes), context);

            var redrive = context.Channel.Bindings!.Sqs!.Queue!.RedrivePolicy;
            Assert.NotNull(redrive);
            Assert.Equal(7, redrive!.MaxReceiveCount);
            Assert.Equal("orders-dlq", redrive.DeadLetterQueue!.Name);
        }

        [Fact]
        public void It_Should_Project_Fifo_Topic_Ordering()
        {
            var contributor = new SqsSubscriptionBindingContributor();
            var context = NewContext();
            var queueAttributes = new SqsAttributes(type: SqsType.Fifo);
            var topicAttributes = new SnsAttributes(type: SqsType.Fifo, contentBasedDeduplication: false);

            contributor.Contribute(NewPubSubSubscription(queueAttributes, topicAttributes), context);

            var ordering = context.Channel.Bindings!.Sns!.Ordering!;
            Assert.Equal(SnsTopicOrderingType.Fifo, ordering.Type);
            Assert.False(ordering.ContentBasedDeduplication);
        }

        [Fact]
        public void It_Should_Surface_Find_Topic_By_Only_On_PubSub_Subscriptions()
        {
            var contributor = new SqsSubscriptionBindingContributor();
            var pubSubContext = NewContext();
            contributor.Contribute(NewPubSubSubscription(), pubSubContext);
            Assert.True(pubSubContext.OperationExtensions.ContainsKey(
                SqsSubscriptionBindingContributor.FindTopicByExtensionKey));

            var pointToPointContext = NewContext();
            contributor.Contribute(NewPointToPointSubscription(), pointToPointContext);
            Assert.False(pointToPointContext.OperationExtensions.ContainsKey(
                SqsSubscriptionBindingContributor.FindTopicByExtensionKey));
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

            Assert.Contains(contributors, c => c is SqsSubscriptionBindingContributor);
        }

        [Fact]
        public void It_Should_Register_Via_UseAsyncApiAWSSQSBindings_Without_Duplication()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IAmASchemaGenerator>(A.Fake<IAmASchemaGenerator>());
            var builder = A.Fake<IBrighterBuilder>();
            A.CallTo(() => builder.Services).Returns(services);

            builder.UseAsyncApi();
            builder.UseAsyncApiAWSSQSBindings();
            builder.UseAsyncApiAWSSQSBindings();

            var provider = services.BuildServiceProvider();
            var contributors = provider.GetServices<IAmASubscriptionBindingContributor>().ToArray();

            Assert.Single(contributors, c => c is SqsSubscriptionBindingContributor);
        }

        public class TestEvent : Event
        {
            public TestEvent() : base(Guid.NewGuid()) { }
        }
    }
}
