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

using Neuroglia.AsyncApi.Bindings;
using Neuroglia.AsyncApi.Bindings.Amqp;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;

namespace Paramore.Brighter.AsyncAPI.Rmq
{
    /// <summary>
    /// An <see cref="IAmASubscriptionBindingContributor"/> that populates AMQP channel and
    /// operation bindings on the generated AsyncAPI document for <see cref="RmqSubscription"/>
    /// subscriptions.
    /// </summary>
    public sealed class RmqSubscriptionBindingContributor : IAmASubscriptionBindingContributor
    {
        private const string AmqpBindingVersion = "0.3.0";
        private const string DefaultVirtualHost = "/";
        private const string DlqChannelNameExtensionKey = "x-brighter-dlq-channel-name";

        /// <inheritdoc />
        public bool CanContribute(Subscription subscription) => subscription is RmqSubscription;

        /// <inheritdoc />
        public void Contribute(Subscription subscription, SubscriptionBindingContext context)
        {
            if (subscription is not RmqSubscription rmqSubscription)
            {
                return;
            }

            context.Channel.Bindings ??= new ChannelBindingDefinitionCollection();
            context.Channel.Bindings.Amqp = new AmqpChannelBindingDefinition
            {
                Type = AmqpChannelType.Queue,
                Queue = new AmqpQueueDefinition
                {
                    Name = subscription.ChannelName?.Value,
                    Durable = rmqSubscription.IsDurable,
                    Exclusive = false,
                    AutoDelete = false,
                    VirtualHost = DefaultVirtualHost
                },
                BindingVersion = AmqpBindingVersion
            };

            context.Operation.Bindings ??= new OperationBindingDefinitionCollection();
            context.Operation.Bindings.Amqp = new AmqpOperationBindingDefinition
            {
                Ack = true,
                BindingVersion = AmqpBindingVersion
            };

            var dlqName = rmqSubscription.DeadLetterChannelName?.Value;
            if (!string.IsNullOrEmpty(dlqName))
            {
                context.ChannelExtensions[DlqChannelNameExtensionKey] = dlqName!;
            }
        }
    }
}
