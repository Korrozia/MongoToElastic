namespace MongoToElastic.Entities
{
    /// <summary>
    /// If each document has an "id" property/field, then this will be used as the "_id" for the document 
    /// which would avoid indexing the same document twice, since a subsequent document with the same id will overwrite the existing document.
    /// </summary>
    public interface IEntity
    {
        string id { get; set; }
    }
}
