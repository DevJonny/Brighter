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

using System.Globalization;
using Paramore.Brighter.MessagingGateway.AzureServiceBus;

namespace Paramore.Brighter.AsyncAPI.AzureServiceBus
{
    /// <summary>
    /// An <see cref="IAmASubscriptionBindingContributor"/> that populates Azure Service Bus
    /// channel and operation metadata on the generated AsyncAPI document for
    /// <see cref="AzureServiceBusSubscription"/> subscriptions. ASB has no standard AsyncAPI
    /// binding yet, so everything is surfaced via <c>x-brighter-asb-*</c> channel and operation
    /// extensions.
    /// </summary>
    public sealed class AzureServiceBusSubscriptionBindingContributor : IAmASubscriptionBindingContributor
    {
        /// <summary>The extension key for the x-brighter-asb channel type.</summary>
        public const string ChannelTypeExtensionKey = "x-brighter-asb-channel-type";

        /// <summary>The extension key for the x-brighter-asb lock duration.</summary>
        public const string LockDurationExtensionKey = "x-brighter-asb-lock-duration";

        /// <summary>The extension key for the x-brighter-asb require-session flag.</summary>
        public const string RequireSessionExtensionKey = "x-brighter-asb-require-session";

        /// <summary>The extension key for the x-brighter-asb max-delivery-count value.</summary>
        public const string MaxDeliveryCountExtensionKey = "x-brighter-asb-max-delivery-count";

        /// <summary>The extension key for the x-brighter-asb dead-lettering-on-message-expiration flag.</summary>
        public const string DeadLetteringOnMessageExpirationExtensionKey =
            "x-brighter-asb-dead-lettering-on-message-expiration";

        /// <summary>The extension key for the x-brighter-asb default-message-time-to-live value.</summary>
        public const string DefaultMessageTimeToLiveExtensionKey = "x-brighter-asb-default-message-time-to-live";

        /// <summary>The extension key for the x-brighter-asb sql-filter.</summary>
        public const string SqlFilterExtensionKey = "x-brighter-asb-sql-filter";

        /// <summary>The channel type value indicating a Service Bus queue.</summary>
        public const string QueueChannelType = "queue";

        /// <summary>The channel type value indicating a topic + subscription.</summary>
        public const string TopicSubscriptionChannelType = "topic-subscription";

        /// <inheritdoc />
        public bool CanContribute(Subscription subscription) => subscription is AzureServiceBusSubscription;

        /// <inheritdoc />
        public void Contribute(Subscription subscription, SubscriptionBindingContext context)
        {
            if (subscription is not AzureServiceBusSubscription asbSubscription)
            {
                return;
            }

            var config = asbSubscription.Configuration;

            context.ChannelExtensions[ChannelTypeExtensionKey] =
                config.UseServiceBusQueue ? QueueChannelType : TopicSubscriptionChannelType;

            context.OperationExtensions[LockDurationExtensionKey] =
                config.LockDuration.ToString("c", CultureInfo.InvariantCulture);
            context.OperationExtensions[RequireSessionExtensionKey] = config.RequireSession;
            context.OperationExtensions[MaxDeliveryCountExtensionKey] = config.MaxDeliveryCount;
            context.OperationExtensions[DeadLetteringOnMessageExpirationExtensionKey] =
                config.DeadLetteringOnMessageExpiration;
            context.OperationExtensions[DefaultMessageTimeToLiveExtensionKey] =
                config.DefaultMessageTimeToLive.ToString("c", CultureInfo.InvariantCulture);

            if (!string.IsNullOrEmpty(config.SqlFilter))
            {
                context.OperationExtensions[SqlFilterExtensionKey] = config.SqlFilter;
            }
        }
    }
}
