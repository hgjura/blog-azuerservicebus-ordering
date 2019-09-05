using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AzureServiceBus_Ordering
{
    public class Subscriber
    {
        string c = "<< ADD YOUR AZURE SERVICE BUS CONNECTION STRING HERE >>";

        public Task Run()
        {
            return ListenToTopicSessionStateSubscriptionsAsync(c, "ordered", "ordered_session");
        }



        public async Task ListenToTopicSessionStateSubscriptionsAsync(string ConnectionString, string TopicName, string SubscriptionName)
        {
            var cts = new CancellationTokenSource();

            var allListeneres = Task.WhenAll(
                ReceiveMessages_FromTopicSessionStateSubscriptionsAsync(new SubscriptionClient(ConnectionString, TopicName, SubscriptionName), cts.Token, ConsoleColor.Blue)
            );

            await Task.WhenAll(
                Task.WhenAny(
                    Task.Run(() => Console.ReadKey()),
                    Task.Delay(TimeSpan.FromMinutes(5))
                ).ContinueWith((t) => cts.Cancel()),
                allListeneres
                );
        }


        private async Task ReceiveMessages_FromTopicSessionStateSubscriptionsAsync(SubscriptionClient client, CancellationToken token, ConsoleColor color)
        {
            var doneReceiving = new TaskCompletionSource<bool>();

            token.Register(
                async () =>
                {
                    await client.CloseAsync();
                    doneReceiving.SetResult(true);
                });

            client.RegisterSessionHandler(
                async (session, message, token1) =>
                {
                    try
                    {
                        var stateData = await session.GetStateAsync();

                        var session_state = stateData != null ? Deserialize<SessionStateManager>(stateData) : new SessionStateManager();

                        if ((int)message.UserProperties["Order"] == session_state.LastProcessedCount + 1)  //check if message is next in the sequence
                        {
                            if (ProcessMessages(message, color))
                            {
                                await session.CompleteAsync(message.SystemProperties.LockToken);

                                session_state.LastProcessedCount = ((int)message.UserProperties["Order"]);
                                await session.SetStateAsync(Serialize<SessionStateManager>(session_state));
                                if (message.UserProperties["IsLast"].ToString().ToLower() == "true")
                                {
                                    //end of the session
                                    await session.SetStateAsync(null);
                                    await session.CloseAsync();
                                }
                            }
                            else
                            {
                                await client.DeadLetterAsync(message.SystemProperties.LockToken, "Message is of the wrong type or could not be processed", "Cannot deserialize this message as the type is unknown.");
                            }
                        }
                        else
                        {
                            session_state.DeferredList.Add((int)message.UserProperties["Order"], message.SystemProperties.SequenceNumber);
                            await session.DeferAsync(message.SystemProperties.LockToken);
                            await session.SetStateAsync(Serialize(session_state));
                        }

                        long last_processed = await ProcessNextMessagesWithSessionStateAsync(client, session, session_state, color);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("-->> ERROR : Unable to receive {0} from subscription: Exception {1}", message.MessageId, ex);
                    }
                },
                new SessionHandlerOptions(e => LogMessageHandlerException(e))
                {
                    MessageWaitTimeout = TimeSpan.FromSeconds(5),
                    MaxConcurrentSessions = 1,
                    AutoComplete = false
                });

            await doneReceiving.Task;
        }

        private static async Task<long> ProcessNextMessagesWithSessionStateAsync(SubscriptionClient client, IMessageSession session, SessionStateManager session_state, ConsoleColor color)
        {
            int x = session_state.LastProcessedCount + 1;

            long seq2 = 0;

            while (true)
            {

                if (!session_state.DeferredList.TryGetValue(x, out seq2)) break;

                //-------------------------------
                var deferredMessage = await session.ReceiveDeferredMessageAsync(seq2);

                if (ProcessMessages(deferredMessage, color))
                {
                    await session.CompleteAsync(deferredMessage.SystemProperties.LockToken);

                    if (deferredMessage.UserProperties["IsLast"].ToString().ToLower() == "true")
                    {
                        //end of the session
                        await session.SetStateAsync(null);
                        await session.CloseAsync();
                    }
                    else
                    {
                        session_state.LastProcessedCount = ((int)deferredMessage.UserProperties["Order"]);
                        session_state.DeferredList.Remove(x);
                        await session.SetStateAsync(Serialize<SessionStateManager>(session_state));
                    }

                }
                else
                {
                    await client.DeadLetterAsync(deferredMessage.SystemProperties.LockToken, "Message is of the wrong type or could not be processed", "Cannot deserialize this message as the type is unknown.");
                    session_state.DeferredList.Remove(x);
                    await session.SetStateAsync(Serialize<SessionStateManager>(session_state));
                }

                //------------------------------

                x++;

            }
            
            return seq2;
        }


        static object lockObj = new object();
        private static bool ProcessMessages(Message message, ConsoleColor color)
        {

            var msg = Deserialize<dynamic>(message.Body);

            lock (lockObj)
            {
                var s = $"\nMessage received: \nMessageId = {message.MessageId}, \nSequenceNumber = {message.SystemProperties.SequenceNumber}, \nEnqueuedTimeUtc = {message.SystemProperties.EnqueuedTimeUtc},\nSession: {message.SessionId},\nOrder: {((int)message.UserProperties["Order"])}";

                ConsoleWrite(s, color);
            }








            return true;            
        }


        
        public static byte[] Serialize<T>(T Item)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(Item));
        }

        public static T Deserialize<T>(byte[] Item)
        {
            return JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(Item));
        }

  
        private Task LogMessageHandlerException(ExceptionReceivedEventArgs e)
        {
            ConsoleWrite($"Exception: \"{e.Exception.Message}\" {e.ExceptionReceivedContext.EntityPath}", ConsoleColor.Red);
            return Task.CompletedTask;
        }

        public static void ConsoleWrite(string Text, ConsoleColor color)
        {
            Console.ForegroundColor = color;
            Console.WriteLine(Text, color);
            Console.ResetColor();
        }
    }
}
