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
using Paramore.Brighter.AsyncAPI;

namespace Paramore.Brighter.AsyncAPI.NJsonSchema
{
    /// <summary>
    /// Extension methods for explicitly registering <see cref="NJsonSchemaGenerator"/> as the
    /// <see cref="IAmASchemaGenerator"/> implementation. This avoids the reflection-based auto-discovery
    /// in <c>UseAsyncApi()</c> and is the recommended approach for trimmed or AOT-published applications.
    /// </summary>
    public static class NJsonSchemaServiceCollectionExtensions
    {
        /// <summary>
        /// Registers <see cref="NJsonSchemaGenerator"/> as the <see cref="IAmASchemaGenerator"/> implementation.
        /// Call this before <c>UseAsyncApi()</c> to bypass reflection-based discovery.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection UseNJsonSchemaGenerator(this IServiceCollection services)
        {
            services.TryAddSingleton<IAmASchemaGenerator, NJsonSchemaGenerator>();
            return services;
        }
    }
}
