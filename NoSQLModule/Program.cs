using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Configuration;
using System.Threading.Tasks;
using System.Security.Cryptography.X509Certificates;
using System.Drawing.Text;

namespace NoSQLModule
{
  class Program
  {
    private static string connectionString = ConfigurationManager.ConnectionStrings["MongoDb"].ConnectionString; 
    private static MongoClient client = new MongoClient(connectionString);
    private static IMongoDatabase database = client.GetDatabase("test");
    private static IMongoCollection<MongoDB.Bson.BsonDocument> collection = database.GetCollection<BsonDocument>("books");
    static void Main(string[] args)
    {

      GetAllBooks().GetAwaiter().GetResult();
      SaveDocs().GetAwaiter().GetResult();
      //Delete(0).GetAwaiter().GetResult();
      FindByCount(1).GetAwaiter().GetResult();
      FindMaxMin().GetAwaiter().GetResult();
      GetAuthorsList().GetAwaiter().GetResult();
      GetBooksWithoutAuthor().GetAwaiter().GetResult();
      UpdateCountBooks().GetAwaiter().GetResult();

      Console.ReadLine();
    }

    private static async Task FindByCount(int count)
    {
      var filter = Builders<BsonDocument>.Filter.AnyGt("Count", count);
      var books = await collection.Find(filter).Sort("{Name:1}").ToListAsync();
      var bookList = books.Take(3);

      foreach (var item in bookList)
      {
        Console.WriteLine(item.Elements.ElementAt(1));
      }

      Console.WriteLine(books.Count);
    }

    private static async Task FindMaxMin()
    {
      
      var maxList = await collection.Find(new BsonDocument()).Sort("{ Count: -1}").ToListAsync();
      Console.WriteLine($"Max count: {maxList.FirstOrDefault()}");

      var minList = await collection.Find(new BsonDocument()).Sort("{ Count: 1}").ToListAsync();
      Console.WriteLine($"Min count: {minList.FirstOrDefault()}");

    }

    private static async Task GetAuthorsList()
    {

      using (var cursor = await collection.DistinctAsync<string>("Author", new BsonDocument()))
      {
        while (await cursor.MoveNextAsync())
        {
          var books = cursor.Current;

          Console.Write("Authors:");

          foreach (var doc in books)
          {
            Console.WriteLine(doc);
          }
        }
      }
    }

    private static async Task GetBooksWithoutAuthor()
    {
      string value = null;
      var filter = Builders<BsonDocument>.Filter.Eq("Author", value);
      var books = await collection.Find(filter).ToListAsync();

      Console.WriteLine("Books without author:");

      foreach (var doc in books)
      {
        Console.WriteLine(doc);
      }

    }

    private static async Task UpdateCountBooks()
    {
      var update = Builders<BsonDocument>.Update.Inc("Count", 1);
      await collection.UpdateManyAsync(new BsonDocument(), update);

      Console.WriteLine("UPDATE:");
      GetAllBooks().GetAwaiter().GetResult();
    }

    public static async Task AddFavoriteColumn()
    {

    }

    private static async Task Delete(int count)
    {
      var filter = Builders<BsonDocument>.Filter.Eq("Count", count);
      await collection.DeleteOneAsync(filter);

      var book = await collection.Find(new BsonDocument()).ToListAsync();

      foreach (var b in book)
      {
        Console.WriteLine(b);
      }
    }

    private static async Task GetAllBooks()
    {

      var filter = new BsonDocument();

      using (var cursor = await collection.FindAsync(filter))
      {
        while (await cursor.MoveNextAsync())
        {
          var book = cursor.Current;

          foreach (var doc in book)
          {
            Console.WriteLine(doc);
          }
        }
      }
    }
    private static async Task SaveDocs()
    {
      var options = new CreateIndexOptions() { Unique = true };
      var field = new StringFieldDefinition<Book>("Name");
      var indexDefinition = new IndexKeysDefinitionBuilder<Book>().Ascending(field);
      await database.GetCollection<Book>("books").Indexes.CreateOneAsync(indexDefinition, options);

      try
      {
        Book book1 = new Book
        {
          Name = "Hobbit",
          Author = "Tolkien",
          Count = 5,
          Genre = new string[] { "Fantasy" },
          Year = 2014
        };
        await collection.InsertOneAsync(book1.ToBsonDocument());

        Book book2 = new Book
        {
          Name = "Lord of the rings",
          Author = "Tolkien",
          Count = 3,
          Genre = new string[] { "Fantasy" },
          Year = 2015
        };
        await collection.InsertOneAsync(book2.ToBsonDocument());

        Book book3 = new Book
        {
          Name = "Kolobok",
          Count = 10,
          Genre = new string[] { "Kids" },
          Year = 2000
        };
        await collection.InsertOneAsync(book3.ToBsonDocument());

        Book book4 = new Book
        {
          Name = "Repka",
          Count = 11,
          Genre = new string[] { "Kids" },
          Year = 2000
        };
        await collection.InsertOneAsync(book4.ToBsonDocument());

        Book book5 = new Book
        {
          Name = "Dyadya Stiopa",
          Author = "Mikhalkov",
          Count = 1,
          Genre = new string[] { "Kids" },
          Year = 2001
        };
        await collection.InsertOneAsync(book5.ToBsonDocument());
      }
       catch
      {
        
      }
      

    }
   
  }
}
