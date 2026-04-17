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
using Paramore.Brighter.AsyncAPI;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.AzureServiceBus;
using Xunit;

namespace Paramore.Brighter.AsyncAPI.AzureServiceBus.Tests
{
    public class When_AzureServiceBus_Subscription_Binding_Contributor_Runs
    {
        private static SubscriptionBindingContext NewContext() =>
            new(
                new V3ChannelDefinition { Address = "test-topic" },
                new V3OperationDefinition { Action = V3OperationAction.Receive },
                new Dictionary<string, object>(),
                new Dictionary<string, object>());

        private static AzureServiceBusSubscription<TestEvent> NewSubscription(
            AzureServiceBusSubscriptionConfiguration? configuration = null) =>
            new(
                subscriptionName: new SubscriptionName("asb-consumer"),
                channelName: new ChannelName("test-subscription"),
                routingKey: new RoutingKey("test-topic"),
                messagePumpType: MessagePumpType.Proactor,
                subscriptionConfiguration: configuration);

        [Fact]
        public void It_Should_CanContribute_For_AzureServiceBusSubscription()
        {
            var contributor = new AzureServiceBusSubscriptionBindingContributor();
            Assert.True(contributor.CanContribute(NewSubscription()));
        }

        [Fact]
        public void It_Should_Not_Contribute_For_Non_AzureServiceBus_Subscription()
        {
            var contributor = new AzureServiceBusSubscriptionBindingContributor();
            var subscription = new Subscription(
                new SubscriptionName("other"),
                new ChannelName("other"),
                new RoutingKey("other"),
                requestType: typeof(TestEvent),
                messagePumpType: MessagePumpType.Reactor);

            Assert.False(contributor.CanContribute(subscription));
        }

        [Fact]
        public void It_Should_Write_Channel_Type_As_Topic_Subscription_By_Default()
        {
            var contributor = new AzureServiceBusSubscriptionBindingContributor();
            var context = NewContext();

            contributor.Contribute(NewSubscription(), context);

            Assert.True(context.ChannelExtensions.TryGetValue(
                AzureServiceBusSubscriptionBindingContributor.ChannelTypeExtensionKey, out var channelType));
            Assert.Equal(
                AzureServiceBusSubscriptionBindingContributor.TopicSubscriptionChannelType, channelType);
        }

        [Fact]
        public void It_Should_Write_Channel_Type_As_Queue_When_UseServiceBusQueue_Is_True()
        {
            var contributor = new AzureServiceBusSubscriptionBindingContributor();
            var context = NewContext();
            var subscription = NewSubscription(
                new AzureServiceBusSubscriptionConfiguration { UseServiceBusQueue = true });

            contributor.Contribute(subscription, context);

            Assert.Equal(
                AzureServiceBusSubscriptionBindingContributor.QueueChannelType,
                context.ChannelExtensions[
                    AzureServiceBusSubscriptionBindingContributor.ChannelTypeExtensionKey]);
        }

        [Fact]
        public void It_Should_Write_Core_Operation_Extensions_From_Configuration()
        {
            var contributor = new AzureServiceBusSubscriptionBindingContributor();
            var configuration = new AzureServiceBusSubscriptionConfiguration
            {
                LockDuration = TimeSpan.FromSeconds(45),
                RequireSession = true,
                MaxDeliveryCount = 9,
                DeadLetteringOnMessageExpiration = false,
                DefaultMessageTimeToLive = TimeSpan.FromHours(6)
            };
            var context = NewContext();

            contributor.Contribute(NewSubscription(configuration), context);

            Assert.Equal(
                TimeSpan.FromSeconds(45).ToString("c"),
                context.OperationExtensions[
                    AzureServiceBusSubscriptionBindingContributor.LockDurationExtensionKey]);
            Assert.Equal(
                true,
                context.OperationExtensions[
                    AzureServiceBusSubscriptionBindingContributor.RequireSessionExtensionKey]);
            Assert.Equal(
                9,
                context.OperationExtensions[
                    AzureServiceBusSubscriptionBindingContributor.MaxDeliveryCountExtensionKey]);
            Assert.Equal(
                false,
                context.OperationExtensions[
                    AzureServiceBusSubscriptionBindingContributor.DeadLetteringOnMessageExpirationExtensionKey]);
            Assert.Equal(
                TimeSpan.FromHours(6).ToString("c"),
                context.OperationExtensions[
                    AzureServiceBusSubscriptionBindingContributor.DefaultMessageTimeToLiveExtensionKey]);
        }

        [Fact]
        public void It_Should_Add_SqlFilter_Extension_When_Set()
        {
            var contributor = new AzureServiceBusSubscriptionBindingContributor();
            var configuration = new AzureServiceBusSubscriptionConfiguration
            {
                SqlFilter = "event = 'OrderPlaced'"
            };
            var context = NewContext();

            contributor.Contribute(NewSubscription(configuration), context);

            Assert.Equal(
                "event = 'OrderPlaced'",
                context.OperationExtensions[
                    AzureServiceBusSubscriptionBindingContributor.SqlFilterExtensionKey]);
        }

        [Fact]
        public void It_Should_Omit_SqlFilter_Extension_When_Empty()
        {
            var contributor = new AzureServiceBusSubscriptionBindingContributor();
            var context = NewContext();

            contributor.Contribute(NewSubscription(), context);

            Assert.False(context.OperationExtensions.ContainsKey(
                AzureServiceBusSubscriptionBindingContributor.SqlFilterExtensionKey));
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

            Assert.Contains(contributors, c => c is AzureServiceBusSubscriptionBindingContributor);
        }

        [Fact]
        public void It_Should_Register_Via_UseAsyncApiAzureServiceBusBindings_Without_Duplication()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IAmASchemaGenerator>(A.Fake<IAmASchemaGenerator>());
            var builder = A.Fake<IBrighterBuilder>();
            A.CallTo(() => builder.Services).Returns(services);

            builder.UseAsyncApi();
            builder.UseAsyncApiAzureServiceBusBindings();
            builder.UseAsyncApiAzureServiceBusBindings();

            var provider = services.BuildServiceProvider();
            var contributors = provider.GetServices<IAmASubscriptionBindingContributor>().ToArray();

            Assert.Single(contributors, c => c is AzureServiceBusSubscriptionBindingContributor);
        }

        public class TestEvent : Event
        {
            public TestEvent() : base(Guid.NewGuid()) { }
        }
    }
}
