using System;

namespace AzureServiceBus_Ordering
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("Publishing messages to Azure Service Bus");

                var app1 = new Publisher();
                app1.Run().GetAwaiter().GetResult();
                
                Console.ReadKey();


                Console.WriteLine("Reading messages from Azure Service Bus");

                var app2 = new Subscriber();
                app2.Run().GetAwaiter().GetResult();


                Console.ReadKey();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
    }
}
