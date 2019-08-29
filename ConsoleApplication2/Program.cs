using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Mime;
using System.Reflection;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Protocol;
using MQTTnet.Server;

namespace ConsoleApplication2
{
    class Program
    {
        static void Main(string[] args)
        {
           

            var optionsBuilder = new MqttServerOptionsBuilder()
                .WithDefaultEndpoint().WithDefaultEndpointPort(1883) // For testing purposes only
                //.WithEncryptedEndpoint().WithEncryptedEndpointPort(config.Port)
                //.WithEncryptionCertificate(certificate.Export(X509ContentType.Pfx))
                //.WithEncryptionSslProtocol(SslProtocols.Tls12)
                .WithConnectionValidator(
                    c =>
                    {
                        Console.WriteLine(c.Username);
                        c.ReasonCode = MqttConnectReasonCode.Success;
                    }).WithSubscriptionInterceptor(
                    c =>
                    {
                        

                        c.AcceptSubscription = false;
                    }).WithApplicationMessageInterceptor(
                    c =>
                    {
                        

                        c.AcceptPublish = false;
                    });

            var mqttServer = new MqttFactory().CreateMqttServer();
            mqttServer.StartAsync(optionsBuilder.Build());
            Console.ReadLine();
        }
    }
}
