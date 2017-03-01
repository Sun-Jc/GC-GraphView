GraphView
=========
GraphView is a DLL library that enables users to use SQL Server or Azure SQL Database to manage graphs. It connects to a SQL database locally or in the cloud, stores graph data in tables and queries graphs through a SQL-extended language. It is not an independent database, but a middleware that accepts graph operations and translates them to T-SQL executed in SQL Server or Azure SQL Database. As such, GraphView can be viewed as a special connector to SQL Server/Azure SQL Database. Developers will experience no differences than the default SQL connector provided by the .NET framework (i.e., SqlConnection), only except that this new connector accepts graph-oriented statements.


Use DocumentDB Emulator as storage
-----------
1. Download DocumentDB Emulator at https://aka.ms/documentdb-emulator and then install. (for 64-bit OS)

2. By default, the emulator will start automatically afer installation. If not, find DocumentDB Emulator in your start memu.

3. By default, the port of emulator is https://localhost:8001 and key is "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="

So change corresponding lines in your "Program.cs" into:
```C#
string DOCDB_URL = "https://localhost:8081";

string DOCDB_AUTHKEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

```

Also, set your own database and collection name here:
```C#
string DOCDB_DATABASE =  "YOUR DATABASE NAME";

string DOCDB_COLLECTION = "YOUR COLLECTION NAME";
```

Now you can connect GraphView with your own local database.



***
We will clean collections of  https://iiis-graphview-test2.documents.azure.com:443/ every day in case gas runs out. So please turn to your own emulator as soon as possible.

Get Help
-----------

`GitHub`  The GitHub repository contains a short introduction. You can use Github's issue tracker to report bugs, suggest features and ask questions.

`Email` If you prefer to talk to us in private, write to graphview@microsoft.com


License
--------------
GraphView is under the [MIT license][MIT].

[manual]:http://research.microsoft.com/pubs/259290/GraphView%20User%20Manual.pdf
[Email]:mailto:graphview@microsoft.com
[MIT]:LICENSE
[datatools]:https://msdn.microsoft.com/en-us/library/mt204009.aspx

