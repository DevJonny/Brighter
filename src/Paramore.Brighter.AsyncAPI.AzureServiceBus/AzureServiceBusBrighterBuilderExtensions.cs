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

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Paramore.Brighter.Extensions.DependencyInjection;

namespace Paramore.Brighter.AsyncAPI.AzureServiceBus
{
    /// <summary>
    /// Extension methods for explicitly registering <see cref="AzureServiceBusSubscriptionBindingContributor"/>.
    /// The auto-discovery probe in <c>UseAsyncApi()</c> resolves this contributor automatically when
    /// the assembly is present; call this extension explicitly in trimmed or AOT-published scenarios
    /// where reflection-based discovery is disabled.
    /// </summary>
    public static class AzureServiceBusBrighterBuilderExtensions
    {
        /// <summary>
        /// Registers <see cref="AzureServiceBusSubscriptionBindingContributor"/> as an
        /// <see cref="IAmASubscriptionBindingContributor"/>. Idempotent across repeated calls.
        /// </summary>
        /// <param name="builder">The Brighter builder to extend.</param>
        /// <returns>The builder for chaining.</returns>
        public static IBrighterBuilder UseAsyncApiAzureServiceBusBindings(this IBrighterBuilder builder)
        {
            builder.Services.TryAddEnumerable(
                ServiceDescriptor.Singleton<IAmASubscriptionBindingContributor, AzureServiceBusSubscriptionBindingContributor>());
            return builder;
        }
    }
}
