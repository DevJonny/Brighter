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

namespace Paramore.Brighter.AsyncAPI
{
    /// <summary>
    /// Default <see cref="IAmASubscriptionBindingContributor"/> that surfaces Brighter-level
    /// subscription metadata (name, message pump type, performers, requeue policy) as
    /// <c>x-brighter-*</c> operation extensions on every generated receive operation,
    /// regardless of the underlying transport.
    /// <para>
    /// Registered unconditionally by <see cref="AsyncApiBrighterBuilderExtensions.UseAsyncApi"/>.
    /// Transport-specific contributors complement this one by adding their own bindings and
    /// <c>x-*</c> keys.
    /// </para>
    /// </summary>
    public sealed class CoreSubscriptionBindingContributor : IAmASubscriptionBindingContributor
    {
        /// <inheritdoc />
        public bool CanContribute(Subscription subscription) => true;

        /// <inheritdoc />
        public void Contribute(Subscription subscription, SubscriptionBindingContext context)
        {
            var extensions = context.OperationExtensions;

            var subscriptionName = subscription.Name?.Value;
            if (!string.IsNullOrEmpty(subscriptionName))
            {
                extensions["x-brighter-subscription-name"] = subscriptionName!;
            }

            extensions["x-brighter-message-pump-type"] = FormatMessagePumpType(subscription.MessagePumpType);
            extensions["x-brighter-performers"] = subscription.NoOfPerformers;
            extensions["x-brighter-requeue-count"] = subscription.RequeueCount;
            extensions["x-brighter-requeue-delay-ms"] = subscription.RequeueDelay.TotalMilliseconds;
            extensions["x-brighter-unacceptable-message-limit"] = subscription.UnacceptableMessageLimit;
        }

        private static string FormatMessagePumpType(MessagePumpType pumpType) => pumpType switch
        {
            MessagePumpType.Reactor => "reactor",
            MessagePumpType.Proactor => "proactor",
            _ => pumpType.ToString().ToLowerInvariant()
        };
    }
}
