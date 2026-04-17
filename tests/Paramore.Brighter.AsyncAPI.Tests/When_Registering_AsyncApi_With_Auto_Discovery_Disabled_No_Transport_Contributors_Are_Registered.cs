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

using System.Linq;
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using Paramore.Brighter.Extensions.DependencyInjection;
using Xunit;

namespace Paramore.Brighter.AsyncAPI.Tests
{
    public class When_Registering_AsyncApi_With_Auto_Discovery_Disabled_No_Transport_Contributors_Are_Registered
    {
        [Fact]
        public void It_Should_Only_Register_Core_Contributor_When_Discovery_Disabled()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            var builder = A.Fake<IBrighterBuilder>();
            A.CallTo(() => builder.Services).Returns(services);

            builder.UseAsyncApi(options => options.DisableAutoBindingContributorDiscovery = true);

            var provider = services.BuildServiceProvider();
            var contributors = provider.GetServices<IAmASubscriptionBindingContributor>().ToArray();

            Assert.Single(contributors);
            Assert.IsType<CoreSubscriptionBindingContributor>(contributors[0]);
        }

        [Fact]
        public void It_Should_Probe_For_Transport_Contributors_By_Default()
        {
            // None of the probe targets exist in the solution yet, so the default (probe enabled)
            // behaves identically to the disabled case. This test pins the expected baseline so that
            // when transport packages ship (US-023+), a failure here signals the probe ran and found
            // new contributors without an explicit UseAsyncApi*Bindings() call — the desired ergonomics.
            var services = new ServiceCollection();
            services.AddLogging();
            var builder = A.Fake<IBrighterBuilder>();
            A.CallTo(() => builder.Services).Returns(services);

            builder.UseAsyncApi();

            var provider = services.BuildServiceProvider();
            var contributors = provider.GetServices<IAmASubscriptionBindingContributor>().ToArray();

            Assert.Single(contributors);
            Assert.IsType<CoreSubscriptionBindingContributor>(contributors[0]);
        }
    }
}
