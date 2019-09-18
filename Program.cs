using MongoToElastic.Entities;
using System;
using System.Threading;

namespace MongoToElastic
{
    class Program
    {
        static void Main(string[] args)
        {
            string databaseName = "MONGO_DB_NAME";
            string collectionName = "MONGO_COLLECTION_NAME";
            string indexName = "ELASTIC_INDEX_NAME";

            MongoToElasticObserver<SomeEntity> observer = new MongoToElasticObserver<SomeEntity>(databaseName, collectionName, indexName);
            observer.Start();

            Thread.Sleep(60 * 1000);

            observer.Stop();

            Console.Read();
        }
    }
}
