using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Newtonsoft.Json;

namespace ServiceBusDemo
{
    public class Program
    {
        const string TopicName = "helpdesk";
        const string SubscriptionAllMessages = "AllMessages";
        const string SubscriptionIT = "IT";
        const string SubscriptionHR = "HR";


        // connection string to your Service Bus namespace
        static string connectionString = "Endpoint=sb://<service bus name>>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<access key>>";

        // the client that owns the connection and can be used to create senders and receivers
        static ServiceBusClient client;

        // the sender used to publish messages to the topic
        static ServiceBusSender sender;

        // the receiver used to receive messages from the subscription 
        static ServiceBusReceiver receiver;

        public async Task SendAndReceiveTestsAsync(string connectionString)
        {
            // This sample demonstrates how to use advanced filters with ServiceBus topics and subscriptions.
            // The sample creates a topic and 3 subscriptions with different filter definitions.
            // Each receiver will receive matching messages depending on the filter associated with a subscription.

            // Send sample messages.
            await this.SendMessagesToTopicAsync(connectionString);

            // Receive messages from subscriptions.
            await this.ReceiveAllMessageFromSubscription(connectionString, SubscriptionHR);
            await this.ReceiveAllMessageFromSubscription(connectionString, SubscriptionIT);
        }


        async Task SendMessagesToTopicAsync(string connectionString)
        {
            // Create the clients that we'll use for sending and processing messages.
            client = new ServiceBusClient(connectionString);
            sender = client.CreateSender(TopicName);

            Console.WriteLine("\nSending orders to topic.");

            // Now we can start sending orders.
            await Task.WhenAll(
                SendOrder(sender, new Message()),
                SendOrder(sender, new Message { Id=1, MessageText="Help with my computer", Helpdesk="IT" }),
                SendOrder(sender, new Message { Id=1, MessageText="Help with my payslip", Helpdesk="HR"  })
                );

            Console.WriteLine("All messages sent.");
        }

        async Task SendOrder(ServiceBusSender sender, Message message)
        {
            var sbMessage = new ServiceBusMessage(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message)))
            {
                CorrelationId = message.Helpdesk,
                Subject = message.MessageText,
                ApplicationProperties =
                {
                    { "id", message.Id },
                    { "MessageText", message.MessageText },
                    { "Helpdesk", message.Helpdesk }
                }
            };
            await sender.SendMessageAsync(sbMessage);

            Console.WriteLine("Sent message with Id={0}, MessageText={1}, Helpdesk={2}", message.Id, message.MessageText, message.Helpdesk);
        }

        async Task ReceiveAllMessageFromSubscription(string connectionString, string subsName)
        {
            var receivedMessages = 0;

            receiver = client.CreateReceiver(TopicName, subsName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

            // Create a receiver from the subscription client and receive all messages.
            Console.WriteLine("\nReceiving messages from subscription {0}.", subsName);

            while (true)
            {
                var receivedMessage = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
                if (receivedMessage != null)
                {
                    foreach (var prop in receivedMessage.ApplicationProperties)
                    {
                        Console.Write("{0}={1},", prop.Key, prop.Value);
                    }
                    Console.WriteLine("CorrelationId={0}", receivedMessage.CorrelationId);
                    receivedMessages++;
                }
                else
                {
                    // No more messages to receive.
                    break;
                }
            }
            Console.WriteLine("Received {0} messages from subscription {1}.", receivedMessages, subsName);
        }

        public static async Task Main()
        {
            try
            {
                Program app = new Program();
                await app.SendAndReceiveTestsAsync(connectionString);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
    }

    public class Message
    {
        public int Id { get; set; }
        public string MessageText { get; set; }
        public string Helpdesk { get; set; }
    }
}
