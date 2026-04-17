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
using Neuroglia.AsyncApi.v3;
using Paramore.Brighter.Extensions.DependencyInjection;
using Xunit;

namespace Paramore.Brighter.AsyncAPI.Tests
{
    public class When_Core_Subscription_Contributor_Runs
    {
        private static SubscriptionBindingContext NewContext() =>
            new(
                new V3ChannelDefinition { Address = "order.created" },
                new V3OperationDefinition { Action = V3OperationAction.Receive },
                new Dictionary<string, object>(),
                new Dictionary<string, object>());

        [Fact]
        public void It_Should_Write_Subscription_Name_On_Operation_Extensions()
        {
            var contributor = new CoreSubscriptionBindingContributor();
            var subscription = new Subscription(
                new SubscriptionName("orders-consumer"),
                new ChannelName("orders"),
                new RoutingKey("order.created"),
                requestType: typeof(TestEvent),
                messagePumpType: MessagePumpType.Reactor);

            var context = NewContext();

            contributor.Contribute(subscription, context);

            Assert.Equal("orders-consumer", context.OperationExtensions["x-brighter-subscription-name"]);
        }

        [Fact]
        public void It_Should_Write_Message_Pump_Type_As_Lowercase_String()
        {
            var contributor = new CoreSubscriptionBindingContributor();

            var reactorCtx = NewContext();
            contributor.Contribute(
                new Subscription(
                    new SubscriptionName("s"),
                    new ChannelName("c"),
                    new RoutingKey("r"),
                    requestType: typeof(TestEvent),
                    messagePumpType: MessagePumpType.Reactor),
                reactorCtx);

            var proactorCtx = NewContext();
            contributor.Contribute(
                new Subscription(
                    new SubscriptionName("s"),
                    new ChannelName("c"),
                    new RoutingKey("r"),
                    requestType: typeof(TestEvent),
                    messagePumpType: MessagePumpType.Proactor),
                proactorCtx);

            Assert.Equal("reactor", reactorCtx.OperationExtensions["x-brighter-message-pump-type"]);
            Assert.Equal("proactor", proactorCtx.OperationExtensions["x-brighter-message-pump-type"]);
        }

        [Fact]
        public void It_Should_Write_Performers_Requeue_And_Unacceptable_Limit_Metadata()
        {
            var contributor = new CoreSubscriptionBindingContributor();
            var subscription = new Subscription(
                new SubscriptionName("orders-consumer"),
                new ChannelName("orders"),
                new RoutingKey("order.created"),
                requestType: typeof(TestEvent),
                noOfPerformers: 3,
                requeueCount: 5,
                requeueDelay: TimeSpan.FromSeconds(2),
                unacceptableMessageLimit: 10,
                messagePumpType: MessagePumpType.Proactor);

            var context = NewContext();

            contributor.Contribute(subscription, context);

            Assert.Equal(3, context.OperationExtensions["x-brighter-performers"]);
            Assert.Equal(5, context.OperationExtensions["x-brighter-requeue-count"]);
            Assert.Equal(2000d, context.OperationExtensions["x-brighter-requeue-delay-ms"]);
            Assert.Equal(10, context.OperationExtensions["x-brighter-unacceptable-message-limit"]);
        }

        [Fact]
        public void It_Should_Omit_Subscription_Name_When_Empty()
        {
            var contributor = new CoreSubscriptionBindingContributor();
            var subscription = new Subscription(
                new SubscriptionName(string.Empty),
                new ChannelName("orders"),
                new RoutingKey("order.created"),
                requestType: typeof(TestEvent),
                messagePumpType: MessagePumpType.Reactor);

            var context = NewContext();

            contributor.Contribute(subscription, context);

            Assert.False(context.OperationExtensions.ContainsKey("x-brighter-subscription-name"));
        }

        [Fact]
        public void It_Should_Not_Touch_Channel_Extensions()
        {
            var contributor = new CoreSubscriptionBindingContributor();
            var subscription = new Subscription(
                new SubscriptionName("orders-consumer"),
                new ChannelName("orders"),
                new RoutingKey("order.created"),
                requestType: typeof(TestEvent),
                messagePumpType: MessagePumpType.Reactor);

            var context = NewContext();

            contributor.Contribute(subscription, context);

            Assert.Empty(context.ChannelExtensions);
        }

        [Fact]
        public void It_Should_CanContribute_For_Every_Subscription()
        {
            var contributor = new CoreSubscriptionBindingContributor();
            var subscription = new Subscription(
                new SubscriptionName("s"),
                new ChannelName("c"),
                new RoutingKey("r"),
                requestType: typeof(TestEvent),
                messagePumpType: MessagePumpType.Reactor);

            Assert.True(contributor.CanContribute(subscription));
        }

        [Fact]
        public void It_Should_Be_Registered_Via_UseAsyncApi()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            var builder = A.Fake<IBrighterBuilder>();
            A.CallTo(() => builder.Services).Returns(services);

            builder.UseAsyncApi();

            var provider = services.BuildServiceProvider();
            var contributors = provider.GetServices<IAmASubscriptionBindingContributor>().ToArray();

            Assert.Contains(contributors, c => c is CoreSubscriptionBindingContributor);
        }

        [Fact]
        public void It_Should_Not_Duplicate_Core_Contributor_On_Repeated_UseAsyncApi_Calls()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            var builder = A.Fake<IBrighterBuilder>();
            A.CallTo(() => builder.Services).Returns(services);

            builder.UseAsyncApi();
            builder.UseAsyncApi();

            var provider = services.BuildServiceProvider();
            var contributors = provider.GetServices<IAmASubscriptionBindingContributor>().ToArray();

            Assert.Single(contributors, c => c is CoreSubscriptionBindingContributor);
        }

        public class TestEvent : Event
        {
            public TestEvent() : base(Guid.NewGuid()) { }
        }
    }
}
