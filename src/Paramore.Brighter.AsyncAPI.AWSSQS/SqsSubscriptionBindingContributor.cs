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
using Neuroglia.AsyncApi.Bindings.Sns;
using Neuroglia.AsyncApi.Bindings.Sqs;
using Paramore.Brighter.MessagingGateway.AWSSQS;

namespace Paramore.Brighter.AsyncAPI.AWSSQS
{
    /// <summary>
    /// An <see cref="IAmASubscriptionBindingContributor"/> that populates SQS (and, when the
    /// subscription uses SNS-fronted pub/sub, SNS) channel and operation bindings on the generated
    /// AsyncAPI document for <see cref="SqsSubscription"/> subscriptions. Fields that have no
    /// standard binding counterpart are surfaced via <c>x-brighter-sqs-*</c> operation extensions.
    /// </summary>
    public sealed class SqsSubscriptionBindingContributor : IAmASubscriptionBindingContributor
    {
        /// <summary>The extension key for the x-brighter-sqs channel type.</summary>
        public const string ChannelTypeExtensionKey = "x-brighter-sqs-channel-type";

        /// <summary>The extension key for the x-brighter-sqs raw-message-delivery flag.</summary>
        public const string RawMessageDeliveryExtensionKey = "x-brighter-sqs-raw-message-delivery";

        /// <summary>The extension key for the x-brighter-sqs FindQueueBy hint.</summary>
        public const string FindQueueByExtensionKey = "x-brighter-sqs-find-queue-by";

        /// <summary>The extension key for the x-brighter-sqs FindTopicBy hint.</summary>
        public const string FindTopicByExtensionKey = "x-brighter-sqs-find-topic-by";

        /// <summary>The channel type value indicating a point-to-point SQS queue.</summary>
        public const string PointToPointChannelType = "point-to-point";

        /// <summary>The channel type value indicating an SNS-fronted pub/sub topic + queue pair.</summary>
        public const string PubSubChannelType = "pub-sub";

        /// <inheritdoc />
        public bool CanContribute(Subscription subscription) => subscription is SqsSubscription;

        /// <inheritdoc />
        public void Contribute(Subscription subscription, SubscriptionBindingContext context)
        {
            if (subscription is not SqsSubscription sqsSubscription)
            {
                return;
            }

            context.ChannelExtensions[ChannelTypeExtensionKey] =
                sqsSubscription.ChannelType == ChannelType.PubSub ? PubSubChannelType : PointToPointChannelType;
            context.OperationExtensions[FindQueueByExtensionKey] = sqsSubscription.FindQueueBy.ToString();

            ApplyQueueBinding(sqsSubscription, context);

            if (sqsSubscription.ChannelType == ChannelType.PubSub)
            {
                ApplyTopicBinding(sqsSubscription, context);
            }
        }

        private static void ApplyQueueBinding(SqsSubscription subscription, SubscriptionBindingContext context)
        {
            var attributes = subscription.QueueAttributes;
            var isFifo = attributes.Type == SqsType.Fifo;

            var queueDefinition = new SqsQueueDefinition
            {
                Name = subscription.ChannelName?.Value ?? string.Empty,
                FifoQueue = isFifo,
                DeliveryDelay = (int)attributes.DelaySeconds.TotalSeconds,
                VisibilityTimeout = (int)attributes.LockTimeout.TotalSeconds,
                ReceiveMessageWaitTime = attributes.TimeOut.HasValue
                    ? (int)attributes.TimeOut.Value.TotalSeconds
                    : null,
                MessageRetentionPeriod = (int)attributes.MessageRetentionPeriod.TotalSeconds,
                DeduplicationScope = MapDeduplicationScope(attributes.DeduplicationScope),
                FifoThroughputLimit = MapFifoThroughputLimit(attributes.FifoThroughputLimit),
                RedrivePolicy = BuildRedrivePolicy(attributes.RedrivePolicy)
            };

            context.Channel.Bindings ??= new ChannelBindingDefinitionCollection();
            context.Channel.Bindings.Sqs = new SqsChannelBindingDefinition
            {
                Queue = queueDefinition
            };

            context.OperationExtensions[RawMessageDeliveryExtensionKey] = attributes.RawMessageDelivery;
        }

        private static void ApplyTopicBinding(SqsSubscription subscription, SubscriptionBindingContext context)
        {
            var topicAttributes = subscription.TopicAttributes ?? SnsAttributes.Empty;
            var isFifo = topicAttributes.Type == SqsType.Fifo;

            context.Channel.Bindings ??= new ChannelBindingDefinitionCollection();
            context.Channel.Bindings.Sns = new SnsChannelBindingDefinition
            {
                Name = subscription.RoutingKey.Value,
                Ordering = new SnsTopicOrderingDefinition
                {
                    Type = isFifo ? SnsTopicOrderingType.Fifo : SnsTopicOrderingType.Standard,
                    ContentBasedDeduplication = topicAttributes.ContentBasedDeduplication
                }
            };

            context.OperationExtensions[FindTopicByExtensionKey] = subscription.FindTopicBy.ToString();
        }

        private static SqsDeduplicationScope? MapDeduplicationScope(DeduplicationScope? scope) => scope switch
        {
            Paramore.Brighter.MessagingGateway.AWSSQS.DeduplicationScope.MessageGroup => SqsDeduplicationScope.MessageGroup,
            Paramore.Brighter.MessagingGateway.AWSSQS.DeduplicationScope.Queue => SqsDeduplicationScope.Queue,
            _ => null
        };

        private static SqsFifoThroughputLimit? MapFifoThroughputLimit(FifoThroughputLimit? limit) => limit switch
        {
            Paramore.Brighter.MessagingGateway.AWSSQS.FifoThroughputLimit.PerQueue => SqsFifoThroughputLimit.PerQueue,
            Paramore.Brighter.MessagingGateway.AWSSQS.FifoThroughputLimit.PerMessageGroupId => SqsFifoThroughputLimit.PerMessageGroupId,
            _ => null
        };

        private static SqsQueueRedrivePolicyDefinition? BuildRedrivePolicy(RedrivePolicy? redrive)
        {
            if (redrive is null)
            {
                return null;
            }

            return new SqsQueueRedrivePolicyDefinition
            {
                MaxReceiveCount = redrive.MaxReceiveCount,
                DeadLetterQueue = new SnsIdentifier
                {
                    Name = redrive.DeadlLetterQueueName.Value
                }
            };
        }
    }
}
