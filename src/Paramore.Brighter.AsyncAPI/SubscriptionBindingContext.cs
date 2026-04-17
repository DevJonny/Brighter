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

using System.Collections.Generic;
using Neuroglia.AsyncApi.v3;

namespace Paramore.Brighter.AsyncAPI
{
    /// <summary>
    /// The channel, operation, and extension slots passed to an
    /// <see cref="IAmASubscriptionBindingContributor"/> when it is invoked for a subscription.
    /// <para>
    /// Contributors may mutate <see cref="Channel"/> and <see cref="Operation"/> directly (e.g. set
    /// transport-specific <c>Bindings</c>), and/or populate <see cref="ChannelExtensions"/> and
    /// <see cref="OperationExtensions"/> with AsyncAPI-compliant <c>x-*</c> values. Extension values
    /// are intended to be primitives or serializable object graphs; they are surfaced on the final
    /// document by the generator.
    /// </para>
    /// </summary>
    /// <param name="Channel">The channel materialized for the subscription's routing key.</param>
    /// <param name="Operation">The receive operation materialized for the subscription.</param>
    /// <param name="ChannelExtensions">A mutable bag of channel-level <c>x-*</c> extensions.</param>
    /// <param name="OperationExtensions">A mutable bag of operation-level <c>x-*</c> extensions.</param>
    public sealed record SubscriptionBindingContext(
        V3ChannelDefinition Channel,
        V3OperationDefinition Operation,
        IDictionary<string, object> ChannelExtensions,
        IDictionary<string, object> OperationExtensions);
}
