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



using System.Security.Claims;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;




namespace SendingToEventHub
{
    class Program
    {

        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting our Event Hub Receiver");



            string namespaceConnectionString = "Endpoint=sb://eventhub-flash-food.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessPolicy;SharedAccessKey=F15BHDT/eXASYLnB3omw00Li523nmb4CW+AEhDQUIsE=;EntityPath=eventhub-flashfood";
            string eventHubName = "eventhub-flashfood";


            string blobConnectionString = "DefaultEndpointsProtocol=https;AccountName=storageacountflashfood;AccountKey=R//snMT8pKVoyle/2WmtYFG+KJxB2NU4AHkj5LS8nO07CNgWx78k0J+ZlADj7D5AjBdxEN2Y9Ida+AStO6Pxnw==;EndpointSuffix=core.windows.net";
            string containerName = "blobcontainer-flashfood";


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
            try
            {
                var eventoRecibido = JsonSerializer.Deserialize<Evento>(str);
                if (eventoRecibido?.type == "login")
                {
                    string newstr = eventoRecibido.data.Replace("\"", "");


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

                    builder.DataSource = "server-db-sql-flashfood.database.windows.net";
                    builder.UserID = "adminServer";
                    builder.Password = "FlashFood123*";
                    builder.InitialCatalog = "sqlDatabase-FlashFood";

                    using (SqlConnection conn = new SqlConnection(builder.ConnectionString))
                    {
                        // Connect to the database
                        conn.Open();

                        // Read rows
                        SqlCommand selectCommand = new SqlCommand($"SELECT IdUsuario,Correo,Contrasenia,IdTipo   FROM [dbo].[Usuarios] WHERE Correo='{ingresar?.email}' AND Contrasenia='{ingresar?.password}'", conn);
                        SqlDataReader reader = selectCommand.ExecuteReader();

                        int idUsuario = 0;
                        int idTipo = 0;



                        if (reader.HasRows)
                        {


                            reader.Read();
                            idUsuario = reader.GetInt32(0);
                            idTipo = reader.GetInt32(3);
                            Console.WriteLine($"Inicio de sesión exitoso. IdUsuario: {idUsuario}. IdTipo:{idTipo}");

                            // define la clave secreta utilizada para firmar el token
                            var secretKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("LosIngenierisimosPoderosisimos"));

                            // crea una lista de reclamos (claims) que se agregarán al token
                            var claims = new[]
                                                {
                            new Claim("idUser", idUsuario.ToString()),
                            new Claim("idType", idTipo.ToString()),

                            // puedes agregar más reclamos aquí
                        };

                            // crea la información de autenticación del token, incluyendo la clave secreta, el algoritmo de firma y los reclamos
                            var tokenDescriptor = new SecurityTokenDescriptor
                            {
                                Subject = new ClaimsIdentity(claims),
                                Expires = DateTime.UtcNow.AddDays(7),
                                SigningCredentials = new SigningCredentials(secretKey, SecurityAlgorithms.HmacSha256Signature)
                            };

                            // crea un token JWT usando la información de autenticación
                            var tokenHandler = new JwtSecurityTokenHandler();
                            var token = tokenHandler.CreateToken(tokenDescriptor);

                            // convierte el token en una cadena JSON
                            var tokenString = tokenHandler.WriteToken(token);
                            Console.WriteLine(tokenString);

                            ProductorDeEventos producerEvent = new ProductorDeEventos();
                            await producerEvent.sendEventAsync(tokenString);
                        }
                        else
                        {
                            Console.WriteLine("Nombre de usuario o contraseña incorrectos.");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }



        }
    }

    public class Usuarios
    {
        public string email { get; set; }
        public string password { get; set; }



    }
    public class Evento
    {
        public string type { get; set; }
        public string data { get; set; }



    }
    public class ProductorDeEventos
    {
        public async Task sendEventAsync(string tokenString)
        {
            int numOfEvents = 1;

            // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
            // of the application, which is best practice when events are being published or read regularly.
            // TODO: Replace the <EVENT_HUB_NAMESPACE> and <HUB_NAME> placeholder values
            EventHubProducerClient producerClient = new EventHubProducerClient(
                "Endpoint=sb://eventhub-flash-food.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessPolicy;SharedAccessKey=F15BHDT/eXASYLnB3omw00Li523nmb4CW+AEhDQUIsE=;EntityPath=eventhub-flashfood",
                "eventhub-flashfood");


            // Create a batch of events 
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            for (int i = 1; i <= numOfEvents; i++)
            {
                Evento eventoEnviado = new Evento();
                eventoEnviado.type = "send_login";
                eventoEnviado.data = tokenString;
                string jsonString = JsonSerializer.Serialize(eventoEnviado);
                if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonString))))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                }
            }

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"A batch of {numOfEvents} events has been published.");
            }
            finally
            {
                await producerClient.DisposeAsync();
            }
        }
    }

}



