using Elasticsearch.Net;
using log4net;
using MongoDB.Driver;
using MongoToElastic.Entities;
using Nest;
using Newtonsoft.Json;
using System;
using System.Configuration;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace MongoToElastic
{
    /// <summary>
    /// This class observes Mongo collection changes and syncronizes them with ElasticSearch index.
    /// Synchronization uses Change Streams feature of Mongo, which is available from Mongo version 3.6.
    /// From the Mongo documentation https://docs.mongodb.com/manual/changeStreams/: 
    /// "To open a change stream against specific collection, applications must have privileges that grant changeStream and find actions on the corresponding collection."
    /// </summary>
    /// <typeparam name="T">Type of the entity.</typeparam>
    public sealed class MongoToElasticObserver<T> where T : class, IEntity
    {
        private const int ChangeStreamCheckTimeout = 5000;
        private const int StopTaskTimeout = 5000;
        private static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private string databaseName;
        private string collectionName;
        private string indexName;
        private string mongoConnectionString;
        private string elasticConnectionString;
        private CancellationTokenSource cancellationTokenSource;
        private CancellationToken cancellationToken;
        private Task task;

        /// <summary>
        /// Constructor.
        /// Requires MongoConnectionString and ElasticConnectionString parameters defined in <connectionStrings> configuration section.
        /// </summary>
        /// <param name="databaseName">Mongo database name.</param>
        /// <param name="collectionName">Mongo collection name.</param>
        /// <param name="indexName">ElasticSearch index name.</param>
        public MongoToElasticObserver(string databaseName, string collectionName, string indexName)
        {
            if (String.IsNullOrEmpty(databaseName))
                throw new ArgumentException(nameof(databaseName));

            this.databaseName = databaseName;

            if (String.IsNullOrEmpty(collectionName))
                throw new ArgumentException(nameof(collectionName));

            this.collectionName = collectionName;

            if (String.IsNullOrEmpty(indexName))
                throw new ArgumentException(nameof(indexName));

            this.indexName = indexName;

            this.mongoConnectionString = ConfigurationManager.ConnectionStrings["MongoConnectionString"].ConnectionString;
            if (String.IsNullOrEmpty(mongoConnectionString))
                throw new ConfigurationErrorsException(nameof(mongoConnectionString));

            this.elasticConnectionString = ConfigurationManager.ConnectionStrings["ElasticConnectionString"].ConnectionString;
            if (String.IsNullOrEmpty(elasticConnectionString))
                throw new ConfigurationErrorsException(nameof(elasticConnectionString));
        }

        /// <summary>
        /// Starts handling changes.
        /// </summary>
        public void Start()
        {
            this.cancellationTokenSource = new CancellationTokenSource();
            this.cancellationToken = this.cancellationTokenSource.Token;

            this.task = Task.Run(() =>
            {
                HandleChanges();
            }, this.cancellationToken);
        }

        /// <summary>
        /// Stops handling changes.
        /// </summary>
        public void Stop()
        {
            this.cancellationTokenSource.Cancel();
            this.task.Wait(StopTaskTimeout);
        }

        /// <summary>
        /// Listed to the changes in Mongo collection and synch the changes to ElasticSearch index.
        /// </summary>
        private void HandleChanges()
        {
            log.Info("HandleChanges begin");

            try
            {
                MongoClient mongoClient = new MongoClient(this.mongoConnectionString);
                ElasticClient elasticClient = new ElasticClient(new ConnectionSettings(new Uri(this.elasticConnectionString)));

                IMongoDatabase database = mongoClient.GetDatabase(this.databaseName);
                IMongoCollection<T> collection = database.GetCollection<T>(this.collectionName);

                this.cancellationToken.ThrowIfCancellationRequested();

                EnsureIndexCreated(elasticClient);

                //Get the whole document instead of just the changed portion
                ChangeStreamOptions options = new ChangeStreamOptions() { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup };

                var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<T>>()
                    .Match("{ operationType: { $in: [ 'insert', 'update', 'delete' ] } }");

                using (var stream = collection.Watch(pipeline, options))
                {
                    while (!this.cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            if (stream.MoveNext())
                            {
                                var enumerator = stream.Current.GetEnumerator();
                                while (enumerator.MoveNext())
                                {
                                    HandleChange(elasticClient,
                                        enumerator.Current.OperationType,
                                        enumerator.Current.FullDocument);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            log.Error($"HandleChanges failed.", ex);
                        }

                        Thread.Sleep(ChangeStreamCheckTimeout);

                        this.cancellationToken.ThrowIfCancellationRequested();
                    }
                }
            }
            catch (Exception ex)
            {
                log.Error($"HandleChanges failed.", ex);
            }
        }

        private void HandleChange(ElasticClient elasticClient, ChangeStreamOperationType changeType, T document)
        {
            log.Info($"HandleChange begin, changeType: {changeType}, document.id: {document.id}");

            try
            {
                switch (changeType)
                {
                    case ChangeStreamOperationType.Insert:
                        elasticClient.Index<T>(document, i =>
                            i.Index(this.indexName)
                            .Id(document.id)
                            .Refresh(Refresh.True));
                        break;
                    case ChangeStreamOperationType.Update:
                        // We used .Index() instead of Update() since we are updating all the fields
                        elasticClient.Index<T>(document, i =>
                            i.Index(this.indexName)
                            .Id(document.id)
                            .Refresh(Refresh.True));
                        break;
                    case ChangeStreamOperationType.Delete:
                        elasticClient.Delete<T>(document.id, d => d.Index(this.indexName));
                        break;
                }
            }
            catch (Exception ex)
            {
                log.Error($"HandleChange failed, changeType: {changeType}, document: {JsonConvert.SerializeObject(document)}.", ex);
            }

            log.Info("HandleChange end");
        }

        private void EnsureIndexCreated(ElasticClient elasticClient)
        {
            log.Info("EnsureIndexCreated begin");

            if (!elasticClient.Indices.Exists(this.indexName).Exists)
            {
                elasticClient.Indices.Create(this.indexName);

                log.Info($"EnsureIndexCreated, created a new index: {this.indexName}");
            }

            log.Info("EnsureIndexCreated end");
        }
    }
}
