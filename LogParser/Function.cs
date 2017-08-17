using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using LogParser.Model;
using Newtonsoft.Json;

namespace LogParser {
    
    //--- Classes ---
    public class Function {
    
        //--- Fields ---
        private readonly string logsBucket = Environment.GetEnvironmentVariable("LOGS_BUCKET");
        private readonly IAmazonS3 _s3Client;
        
        //--- Constructors ---
        public Function() {
            _s3Client = new AmazonS3Client();
        }

        //--- Methods ---
        [LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]
        public void Handler(CloudWatchLogsEvent cloudWatchLogsEvent, ILambdaContext context) {
            
            // Level 1: decode and decompress data
            var logsData = cloudWatchLogsEvent.AwsLogs.Data;
            Console.WriteLine($"THIS IS THE DATA: {logsData}");
            var decompressedData = DecompressLogData(logsData);
            Console.WriteLine($"THIS IS THE DECODED, UNCOMPRESSED DATA: {decompressedData}");
            
            // Level 3: Parse log records
            var athenaFriendlyJson = ParseLog(decompressedData);

            // Level 3: Save data to S3
            PutObject(athenaFriendlyJson);

            // Level 5: Create athena schema to query data
        }
        
        public static string DecompressLogData(string value) {

            using (var streamReader = new GZipStream(new MemoryStream(Convert.FromBase64String(value)), CompressionMode.Decompress, false)){
                const int size = 4096;
                byte[] buffer = new byte[size];

                using (MemoryStream memory = new MemoryStream()) {
                    int count = 0;
                    do
                    {
                        count = streamReader.Read(buffer, 0, size);
                        if (count > 0) {
                            memory.Write(buffer, 0, count);
                        }
                    } while (count > 0);
                    return Encoding.UTF8.GetString(memory.ToArray());
                }
            }

        }

        private static IEnumerable<string> ParseLog(string data) {
            
            throw new NotImplementedException();
        }

        public void PutObject(IEnumerable<string> values) {
            throw new NotImplementedException();
        }
    }
}
