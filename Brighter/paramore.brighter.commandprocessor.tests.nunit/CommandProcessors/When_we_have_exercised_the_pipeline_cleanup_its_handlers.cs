using System;
using System.Linq;
using nUnitShouldAdapter;
using NUnit.Specifications;
using paramore.brighter.commandprocessor.tests.nunit.CommandProcessors.TestDoubles;

namespace paramore.brighter.commandprocessor.tests.nunit.CommandProcessors
{
    [Subject(typeof(PipelineBuilder<>))]
    public class When_We_Have_Exercised_The_Pipeline_Cleanup_Its_Handlers : ContextSpecification
    {
        private static PipelineBuilder<MyCommand> s_pipeline_Builder;
        private static string s_released;

        private Establish _context = () =>
        {
            s_released = string.Empty;

            var registry = new SubscriberRegistry();
            registry.Register<MyCommand, MyPreAndPostDecoratedHandler>();
            registry.Register<MyCommand, MyLoggingHandler<MyCommand>>();

            var handlerFactory = new CheapHandlerFactory();

            s_pipeline_Builder = new PipelineBuilder<MyCommand>(registry, handlerFactory);
            s_pipeline_Builder.Build(new RequestContext()).Any();
        };

        internal class CheapHandlerFactory : IAmAHandlerFactory
        {
            public IHandleRequests Create(Type handlerType)
            {
                if (handlerType == typeof(MyPreAndPostDecoratedHandler))
                {
                    return new MyPreAndPostDecoratedHandler();
                }
                if (handlerType == typeof(MyLoggingHandler<MyCommand>))
                {
                    return new MyLoggingHandler<MyCommand>();
                }
                if (handlerType == typeof(MyValidationHandler<MyCommand>))
                {
                    return new MyValidationHandler<MyCommand>();
                }
                return null;
            }

            public void Release(IHandleRequests handler)
            {
                var disposable = handler as IDisposable;
                disposable?.Dispose();

                s_released += "|" + handler.Name;
            }
        }


        private Because _of = () => s_pipeline_Builder.Dispose();

        private It _should_have_called_dispose_on_instances_from_ioc = () => MyPreAndPostDecoratedHandler.DisposeWasCalled.ShouldBeTrue();
        private It _should_have_called_dispose_on_instances_from_pipeline_builder = () => MyLoggingHandler<MyCommand>.DisposeWasCalled.ShouldBeTrue();
        private It _should_have_called_release_on_all_handlers = () => s_released.ShouldEqual("|MyValidationHandler`1|MyPreAndPostDecoratedHandler|MyLoggingHandler`1|MyLoggingHandler`1");
    }
}