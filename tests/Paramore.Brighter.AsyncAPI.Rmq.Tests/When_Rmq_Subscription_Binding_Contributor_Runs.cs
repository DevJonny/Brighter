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
using Neuroglia.AsyncApi.Bindings.Amqp;
using Neuroglia.AsyncApi.v3;
using Paramore.Brighter.AsyncAPI;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using Xunit;

namespace Paramore.Brighter.AsyncAPI.Rmq.Tests
{
    public class When_Rmq_Subscription_Binding_Contributor_Runs
    {
        private static SubscriptionBindingContext NewContext() =>
            new(
                new V3ChannelDefinition { Address = "test-queue" },
                new V3OperationDefinition { Action = V3OperationAction.Receive },
                new Dictionary<string, object>(),
                new Dictionary<string, object>());

        [Fact]
        public void It_Should_CanContribute_For_RmqSubscription()
        {
            var contributor = new RmqSubscriptionBindingContributor();
            var rmqSub = new RmqSubscription<TestEvent>(
                subscriptionName: new SubscriptionName("rmq-consumer"),
                channelName: new ChannelName("test-queue"),
                routingKey: new RoutingKey("test-queue"));

            Assert.True(contributor.CanContribute(rmqSub));
        }

        [Fact]
        public void It_Should_Not_Contribute_For_Non_Rmq_Subscription()
        {
            var contributor = new RmqSubscriptionBindingContributor();
            var subscription = new Subscription(
                new SubscriptionName("other"),
                new ChannelName("other"),
                new RoutingKey("other"),
                requestType: typeof(TestEvent),
                messagePumpType: MessagePumpType.Reactor);

            Assert.False(contributor.CanContribute(subscription));
        }

        [Fact]
        public void It_Should_Write_Amqp_Channel_Binding_With_Queue_Attributes()
        {
            var contributor = new RmqSubscriptionBindingContributor();
            var subscription = new RmqSubscription<TestEvent>(
                subscriptionName: new SubscriptionName("rmq-consumer"),
                channelName: new ChannelName("orders-queue"),
                routingKey: new RoutingKey("orders"),
                isDurable: true);

            var context = NewContext();

            contributor.Contribute(subscription, context);

            Assert.NotNull(context.Channel.Bindings);
            var amqpBinding = context.Channel.Bindings!.Amqp;
            Assert.NotNull(amqpBinding);
            Assert.Equal(AmqpChannelType.Queue, amqpBinding!.Type);
            Assert.Equal("0.3.0", amqpBinding.BindingVersion);
            Assert.NotNull(amqpBinding.Queue);
            Assert.Equal("orders-queue", amqpBinding.Queue!.Name);
            Assert.True(amqpBinding.Queue.Durable);
            Assert.False(amqpBinding.Queue.Exclusive);
            Assert.False(amqpBinding.Queue.AutoDelete);
            Assert.Equal("/", amqpBinding.Queue.VirtualHost);
        }

        [Fact]
        public void It_Should_Write_Non_Durable_Queue_When_Subscription_Is_Transient()
        {
            var contributor = new RmqSubscriptionBindingContributor();
            var subscription = new RmqSubscription<TestEvent>(
                subscriptionName: new SubscriptionName("rmq-consumer"),
                channelName: new ChannelName("ephemeral-queue"),
                routingKey: new RoutingKey("ephemeral"),
                isDurable: false);

            var context = NewContext();

            contributor.Contribute(subscription, context);

            Assert.False(context.Channel.Bindings!.Amqp!.Queue!.Durable);
        }

        [Fact]
        public void It_Should_Write_Amqp_Operation_Binding_With_Ack_True()
        {
            var contributor = new RmqSubscriptionBindingContributor();
            var subscription = new RmqSubscription<TestEvent>(
                subscriptionName: new SubscriptionName("rmq-consumer"),
                channelName: new ChannelName("test-queue"),
                routingKey: new RoutingKey("test-queue"));

            var context = NewContext();

            contributor.Contribute(subscription, context);

            Assert.NotNull(context.Operation.Bindings);
            var amqpOp = context.Operation.Bindings!.Amqp;
            Assert.NotNull(amqpOp);
            Assert.Equal("0.3.0", amqpOp!.BindingVersion);
            Assert.True(amqpOp.Ack);
        }

        [Fact]
        public void It_Should_Add_Dlq_Channel_Name_Extension_When_DeadLetterChannelName_Set()
        {
            var contributor = new RmqSubscriptionBindingContributor();
            var subscription = new RmqSubscription<TestEvent>(
                subscriptionName: new SubscriptionName("rmq-consumer"),
                channelName: new ChannelName("test-queue"),
                routingKey: new RoutingKey("test-queue"),
                deadLetterChannelName: new ChannelName("test-queue.dlq"),
                deadLetterRoutingKey: new RoutingKey("test-queue.dlq"));

            var context = NewContext();

            contributor.Contribute(subscription, context);

            Assert.True(context.ChannelExtensions.TryGetValue("x-brighter-dlq-channel-name", out var value));
            Assert.Equal("test-queue.dlq", value);
        }

        [Fact]
        public void It_Should_Omit_Dlq_Channel_Name_Extension_When_Not_Set()
        {
            var contributor = new RmqSubscriptionBindingContributor();
            var subscription = new RmqSubscription<TestEvent>(
                subscriptionName: new SubscriptionName("rmq-consumer"),
                channelName: new ChannelName("test-queue"),
                routingKey: new RoutingKey("test-queue"));

            var context = NewContext();

            contributor.Contribute(subscription, context);

            Assert.False(context.ChannelExtensions.ContainsKey("x-brighter-dlq-channel-name"));
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

            Assert.Contains(contributors, c => c is RmqSubscriptionBindingContributor);
        }

        [Fact]
        public void It_Should_Register_Via_UseAsyncApiRmqBindings_Without_Duplication()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IAmASchemaGenerator>(A.Fake<IAmASchemaGenerator>());
            var builder = A.Fake<IBrighterBuilder>();
            A.CallTo(() => builder.Services).Returns(services);

            builder.UseAsyncApi();
            builder.UseAsyncApiRmqBindings();
            builder.UseAsyncApiRmqBindings();

            var provider = services.BuildServiceProvider();
            var contributors = provider.GetServices<IAmASubscriptionBindingContributor>().ToArray();

            Assert.Single(contributors, c => c is RmqSubscriptionBindingContributor);
        }

        public class TestEvent : Event
        {
            public TestEvent() : base(Guid.NewGuid()) { }
        }
    }
}
