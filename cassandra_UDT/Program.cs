using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data;
using Cassandra.Data.Linq;
using Cassandra.Mapping;

namespace cassandra_UDT
{
    class Program
    {

        //https://github.com/LukeTillman/cqlpoco
        //https://github.com/datastax/csharp-driver

        //http://www.datastax.com/dev/blog/csharp-driver-cassandra-new-mapper-linq-improvements
        //https://docs.datastax.com/en/cql/3.3/cql/cqlIntro.html
        //http://datastax.github.io/csharp-driver/features/udts/


        public class User
        {
            public Guid Id { get; set; }
            public IDictionary<string, Address> Addresses {get; set;}
            public FullName Name { get; set; }
        }

        public class Address
        {
            public string Street { get; set; }
            public string City { get; set; }
            public int ZipCode { get; set; }
            public IEnumerable<string> Phones { get; set; }
        }

        public class FullName
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }

        static void Main(string[] args)
        {
            //var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
            var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();

            ISession session = cluster.Connect("mykeyspace");
            //MappingConfiguration.Global.Define();

            session.UserDefinedTypes.Define(
                    UdtMap.For<FullName>(),
                    UdtMap.For<Address>()
                       .Map(a => a.Street, "street")
                       .Map(a => a.City, "city")
                       .Map(a => a.ZipCode, "zip_code")
                       .Map(a => a.Phones, "phones")
                );
            //BasicCqlOperation(session);
            SelectUsingLinq(session);
            //FetchAll(session);


        }

        private static void BasicCqlOperation(ISession session)
        {
            Row row = session.Execute("SELECT * FROM users WHERE id=62c36092-82a1-3a00-93d1-46196ee77204").First();
            #region works
            //SortedDictionary<string, Address> dictionary = (SortedDictionary<string, Address>)row[1];
            //IDictionary<string, Address> dictionary = (SortedDictionary<string, Address>)row[1];
            #endregion
            IEnumerable<KeyValuePair<string, Address>> dictionary = (SortedDictionary<string, Address>)row[1];

            foreach (var sorteditem in dictionary)
            {
                Console.WriteLine("Sorrted Dictionary Key {0} Values {1}", sorteditem.Key, sorteditem.Value.ZipCode);
            }
        }

        // In a Standard LINQ way.
        public static void SelectUsingLinq(ISession session)
        {
            var users = new Table<User>(session,
                MappingConfiguration.Global.Define(new Map<User>().TableName("users")
                    .Column(c => c.Id, cm => cm.WithName("id"))
                    .Column(c => c.Addresses, cm => cm.WithName("addresses")))
                );

            var user = users.Where(u => u.Id == Guid.Parse("62c36092-82a1-3a00-93d1-46196ee77204")).FirstOrDefault().Execute();
            //Example of paged.
            //var user = users.Where(u => u.Id == Guid.Parse("62c36092-82a1-3a00-93d1-46196ee77204")).SetPageSize(1).ExecutePaged();
            Console.WriteLine(user.Name.FirstName);
        }

        //Using Mapper and CQL
        public static void FetchAll(ISession session)
        {
            IMapper mapper = new Mapper(session);
            var users = mapper.Fetch<User>("SELECT * from users");

            foreach (var item in users)
            {
                Console.WriteLine("{0} {1}", item.Id, item.Name.FirstName);
                foreach(var sd in item.Addresses)
                {
                    Console.WriteLine("\t{0} \t{1}",sd.Key, sd.Value.City );
                }
            }
        }
    }
}

