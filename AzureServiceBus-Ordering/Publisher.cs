using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureServiceBus_Ordering
{
    public class Publisher
    {
        string c = "<< ADD YOUR AZURE SERVICE BUS CONNECTION STRING HERE >>";

        public Task Run()
        {
            List<dynamic> temp = new List<dynamic>();
            var session_id = Guid.NewGuid();

            //var max_count = 10;
            //for (var i = 1; i <= max_count; i++)
            //{
            //    temp.Add(new { order = i, is_last = (i == max_count), session = session_id });
            //}

            temp.Add(new { order = 6, is_last = false, session = session_id });
            temp.Add(new { order = 5, is_last = false, session = session_id });
            temp.Add(new { order = 9, is_last = false, session = session_id });
            temp.Add(new { order = 4, is_last = false, session = session_id });
            temp.Add(new { order = 10, is_last = true, session = session_id });
            temp.Add(new { order = 7, is_last = false, session = session_id });
            temp.Add(new { order = 2, is_last = false, session = session_id });
            temp.Add(new { order = 1, is_last = false, session = session_id });
            temp.Add(new { order = 3, is_last = false, session = session_id });
            temp.Add(new { order = 8, is_last = false, session = session_id });

            return PublishToOrderedSession(temp, c, "ordered");
        }


        private Task PublishToOrderedSession(List<dynamic> temp, string ConnectionString, string TopicName) {


            //creating Messages out of test data
            List<Message> all_messages = temp.Select(m =>
            {
                var message = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(m)))
                {
                    ContentType = "application/json",
                    MessageId = $"{"OrderedObject"}-{m.order}-{DateTime.Now.ToString("yyyy-M-ddThh:mm:ss.ff")}-{Guid.NewGuid()}",
                    Label = "Ordered Session",
                    TimeToLive = TimeSpan.FromMinutes(1),
                    SessionId = $"{m.session}"
                };

                //TODO: Message properties
                message.UserProperties.Add("Order", m.order);
                message.UserProperties.Add("IsLast", m.is_last);

                return message;
            }).ToList();

            //send async
            var all_tasks = new List<Task>();
            var sender = new MessageSender(ConnectionString, TopicName);
            all_messages.ForEach(m => all_tasks.Add(sender.SendAsync(m)));

            return Task.WhenAll(all_tasks.ToArray());
        }
    }
}
