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
    /// Contributes transport- or cross-cutting metadata to the AsyncAPI channel and operation
    /// generated for a <see cref="Subscription"/>.
    /// <para>
    /// Implementations are invoked by <see cref="AsyncApiDocumentGenerator"/> once per subscription
    /// after its channel and receive operation have been materialized. A contributor may enrich the
    /// channel or operation objects directly (e.g. by setting Bindings, Description, Tags) and/or
    /// populate the <see cref="SubscriptionBindingContext.OperationExtensions"/> and
    /// <see cref="SubscriptionBindingContext.ChannelExtensions"/> slots with AsyncAPI
    /// specification-compliant <c>x-*</c> extension values.
    /// </para>
    /// <para>
    /// The interface is deliberately non-generic: a single generator dispatches across every
    /// <see cref="Subscription"/> subclass, and each contributor decides via
    /// <see cref="CanContribute(Subscription)"/> whether the subscription is relevant by
    /// performing a runtime type check.
    /// </para>
    /// </summary>
    public interface IAmASubscriptionBindingContributor
    {
        /// <summary>
        /// Determines whether this contributor applies to the given <paramref name="subscription"/>.
        /// Typical implementations return <c>subscription is MyTransportSubscription</c>.
        /// </summary>
        /// <param name="subscription">The subscription being processed by the generator.</param>
        /// <returns><c>true</c> when <see cref="Contribute"/> should be invoked; otherwise <c>false</c>.</returns>
        bool CanContribute(Subscription subscription);

        /// <summary>
        /// Enriches the channel and operation associated with <paramref name="subscription"/>.
        /// Called only when <see cref="CanContribute"/> returned <c>true</c> for the same subscription.
        /// </summary>
        /// <param name="subscription">The subscription being processed.</param>
        /// <param name="context">The channel, operation, and extension slots the contributor may mutate.</param>
        void Contribute(Subscription subscription, SubscriptionBindingContext context);
    }
}
