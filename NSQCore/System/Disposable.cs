using System;

namespace NSQCore.System
{
    internal class Disposable
    {
        public static readonly IDisposable Empty = new EmptyDisposable();

        private class EmptyDisposable : IDisposable
        {
            public void Dispose()
            {
            }
        }
    }
}
