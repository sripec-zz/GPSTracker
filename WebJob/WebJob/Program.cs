using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.ServiceBus.Messaging;
using Microsoft.Azure.Devices.Common;
using System.Configuration;
using System.Data.SqlClient;
using Dapper;
using DapperExtensions;
using WebJob.Models;
using System.Collections.Generic;
using System.Linq;
using Microsoft.ServiceBus;

namespace WebJob
{
    // To learn more about Microsoft Azure WebJobs SDK, please see http://go.microsoft.com/fwlink/?LinkID=320976
    class Program
    {
        private static int eventHubPartitionsCount;
        CancellationToken ct = new CancellationToken();
        static void Main()
        {
            string activeIoTHubConnectionString = ConfigurationManager.AppSettings["activeIoTHubConnectionString"].ToString();
            string selectedDevice = ConfigurationManager.AppSettings["selectedDevice"].ToString();
            string consumerGroupName = ConfigurationManager.AppSettings["consumerGroupName"].ToString();
            var host = new JobHost();

            MainAsync(activeIoTHubConnectionString, selectedDevice, consumerGroupName).Wait();

            // The following code ensures that the WebJob will be running continuously
            host.RunAndBlock();
        }

        static async Task MainAsync(string activeIoTHubConnectionString, string selectedDevice, string consumerGroupName)
        {
            var ctsForDataMonitoring = new CancellationTokenSource();

            await ReadFromIOTHub(activeIoTHubConnectionString, selectedDevice, DateTime.Now, ctsForDataMonitoring.Token, consumerGroupName);
        }

        private static async Task ReadFromIOTHub(string activeIoTHubConnectionString, string selectedDevice, DateTime startTime, CancellationToken ct, string consumerGroupName)
        {
            EventHubClient eventHubClient = null;
            EventHubReceiver eventHubReceiver = null;
            EventHubReceiver eventHubHistoryReceiver = null;
            string output = "Receiving events...\r\n";

            try
            {
                eventHubClient = EventHubClient.CreateFromConnectionString(activeIoTHubConnectionString, "messages/events");
                eventHubPartitionsCount = eventHubClient.GetRuntimeInformation().PartitionCount;
                string partition = EventHubPartitionKeyResolver.ResolveToPartition(selectedDevice, eventHubPartitionsCount);
                //If inside Firewall, use this below line...
                ServiceBusEnvironment.SystemConnectivity.Mode = ConnectivityMode.Http;

                bool IsHistoryNeeded = Convert.ToBoolean(ConfigurationManager.AppSettings["IsHistoryNeeded"]);
                if (IsHistoryNeeded)
                {
                    //receive the events from startTime until current time in a single call and process them
                    eventHubHistoryReceiver = eventHubClient.GetConsumerGroup(consumerGroupName).CreateReceiver(partition);
                    var events = await eventHubHistoryReceiver.ReceiveAsync(int.MaxValue, TimeSpan.FromSeconds(20));

                    foreach (var eventData in events)
                    {
                        ProcessEvent(eventData);
                    }
                }
                //having already received past events, monitor current events in a loop
                eventHubReceiver = eventHubClient.GetConsumerGroup(consumerGroupName).CreateReceiver(partition, startTime);
                while (true)
                {
                    ct.ThrowIfCancellationRequested();

                    var eventData = await eventHubReceiver.ReceiveAsync(TimeSpan.FromSeconds(1));

                    if (eventData != null)
                    {
                        ProcessEvent(eventData);
                    }
                }
            }
            catch (Exception ex)
            {
                if (ct.IsCancellationRequested)
                {
                    //output += $"Stopped Monitoring events. {ex.Message}\r\n";
                }
                else
                {
                    //Do something
                }
                if (eventHubReceiver != null)
                {
                    eventHubReceiver.Close();
                }
                if (eventHubClient != null)
                {
                    eventHubClient.Close();
                }
            }
        }

        private static void ProcessEvent(EventData eventData)
        {
            var data = Encoding.UTF8.GetString(eventData.GetBytes());
            var enqueuedTime = eventData.EnqueuedTimeUtc.ToLocalTime();
            var connectionDeviceId = eventData.SystemProperties["iothub-connection-device-id"].ToString();

            var rawData = new RawData();
            rawData.EventUTC = enqueuedTime;
            rawData.DeviceId = connectionDeviceId;

            try
            {
                var value = data.Split(',').ToArray();

                rawData.Lat = Convert.ToDouble(value[0]);
                rawData.Lon = Convert.ToDouble(value[1]);

                Console.WriteLine(enqueuedTime + " " + value[0] + " " + value[1]);
                if (rawData.Lat >= -90 && rawData.Lat <= 90 && rawData.Lon >= -180 && rawData.Lon <= 180)
                {
                    try
                    {
                        StoreInDB(rawData);
                    }
                    catch (Exception ex)
                    {
                        //Log the error}
                    }
                }
            }
            catch (Exception ex)
            {
                //Not a GPS data
                //Log the error
            }
        }

        private static void StoreInDB(RawData rawData)
        {
            string ConnectionString = ConfigurationManager.ConnectionStrings["DbConnectionString"].ConnectionString;
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                //serviceProviderDetails = sqlConnection.Query<ServiceProvider>("select Serviceprovider.[Id], Serviceprovider.[Feed], Serviceprovider.[Fleet_id], Serviceprovider.[Url], Serviceprovider.[UserName], Serviceprovider.[Password], Serviceprovider.[FeedSchema_id], Serviceprovider.[Interval], Serviceprovider.[Status],Serviceprovider.[AuthenticationType],Serviceprovider.[QueryString],Serviceprovider.[PagingEnable],Serviceprovider.[FeedNotReportingTimeOut] from Serviceprovider left outer join FeedRun on Serviceprovider.Id=FeedRun.serviceprovider_id where FeedRun.Id=@FeedRunId", new { FeedRunId = inputParameters.FeedRunId }).FirstOrDefault();
                sqlConnection.Insert(rawData);
            }
        }
    }
}
