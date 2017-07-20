using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NSQCore;

namespace NSQCoreClient
{
    class Program
    {
        private static void Main(string[] args)
        {
            
            //Task.Run( PublishMessage).GetAwaiter().GetResult();

            Task.Run(LookupConsumeMessages).GetAwaiter().GetResult();

            Console.ReadKey();
        }

        private static async Task PublishMessage()
        {

            Console.WriteLine("Publishing Message");

            var prod = new NsqProducer("localhost", 4151);
            await prod.PublishAsync("topic1", "hello world" );
            Console.WriteLine("Message Published");
        }


        private static async Task LookupConsumeMessages()
        {

            var cons = NsqConsumer.Create("lookupd=localhost:4161; topic=topic1; channel=abc");
            await cons.ConnectAndWaitAsync(Handler);
            await cons.SetMaxInFlightAsync(5);
        }

       
        static async Task Handler(Message message)
        {
            Console.WriteLine("Received: Message={0}", message.Body);
            await message.FinishAsync();
        }
    }
}
