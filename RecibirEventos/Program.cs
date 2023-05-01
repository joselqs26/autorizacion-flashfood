using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;


using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using System.Data.SqlClient;

namespace SendingToEventHub
{
    class Program
    {

        static async Task Main(string[] args)
        {            
            Console.WriteLine("Starting our Event Hub Receiver");



            string namespaceConnectionString = "Endpoint=sb://eventhub-flashfood.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessPolicy;SharedAccessKey=D7s1PbsGugJeWQaQpiwGJw199QvKqfIcX+AEhFJNPI4=;EntityPath=eventhub-flashfood";
            string eventHubName = "eventhub-flashfood";


            string blobConnectionString = "DefaultEndpointsProtocol=https;AccountName=azurestorageflashfood;AccountKey=Q6jixJ+fue/fsGGnVHn4Q2sVqpFM769twe+DyWMnoayFlvpwLjd0QRQKpoBYwbRdUp94Zo0plf20+AStea8meA==;EndpointSuffix=core.windows.net";
            string containerName = "offsetcontainer";


            BlobContainerClient storageClient = new BlobContainerClient(blobConnectionString, "blobcontainer-flashfood");

            EventProcessorClient processor = new EventProcessorClient(storageClient, "$Default", namespaceConnectionString, eventHubName);

            processor.ProcessEventAsync += Processor_ProcessEventAsync;
            processor.ProcessErrorAsync += Processor_ProcessErrorAsync;

            await processor.StartProcessingAsync();
            Console.WriteLine("Started the processor");


            Console.ReadLine();
            await processor.StopProcessingAsync();
            Console.WriteLine("Started the processor");
        }

        private static Task Processor_ProcessErrorAsync(ProcessErrorEventArgs arg)
        {                       
            Console.WriteLine("Error Received: " + arg.Exception.ToString());
            return Task.CompletedTask;
        }

        private static async Task Processor_ProcessEventAsync(ProcessEventArgs arg)
        {
            //Console.WriteLine($"Event Received from Partition {arg.Partition.PartitionId}: {arg.Data.EventBody.ToString()}");

            string str = arg.Data.EventBody.ToString();
            Console.WriteLine(str);
            string newstr = str.Replace("\"", "");
            

            byte[] decbuff = Convert.FromBase64String(newstr);
            Console.WriteLine(decbuff);
            string descodificar = System.Text.Encoding.UTF8.GetString(decbuff);

            Console.WriteLine(descodificar);

            //------------Deserializar--------

            var ingresar = JsonSerializer.Deserialize<Usuarios>(descodificar);

            Console.WriteLine($"email: {ingresar?.email}");
            Console.WriteLine($"password: {ingresar?.password}");

            await arg.UpdateCheckpointAsync();

            //--------------- conexion -------------------------------------------

            // Best practice is to scope the MySqlConnection to a "using" block
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

            builder.DataSource = "serverdb-flashfood.database.windows.net";
            builder.UserID = "adminServer";
            builder.Password = "FlashFood123*";
            builder.InitialCatalog = "sqlDatabase-FlashFood";

            using (SqlConnection conn = new SqlConnection(builder.ConnectionString))
            {
                // Connect to the database
                conn.Open();

                // Read rows
                SqlCommand selectCommand = new SqlCommand($"SELECT *   FROM [dbo].[Usuarios] WHERE Correo='{ingresar?.email}' AND Contrasenia='{ingresar?.password}'", conn);                
                SqlDataReader reader = selectCommand.ExecuteReader();

                if (reader.HasRows)
                {
                    Console.WriteLine("Inicio de sesión exitoso.");
                }
                else
                {
                    Console.WriteLine("Nombre de usuario o contraseña incorrectos.");
                }
            }

        }       
    }    
    public class Usuarios
    {
        public string email { get; set;}
        public string password { get; set;}
    }
}



