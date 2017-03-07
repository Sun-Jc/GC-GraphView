using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Diagnostics;
using GraphView;
using System.Data;
using Microsoft.VisualBasic.FileIO;
using System.Globalization;
using System.Security.Cryptography;
using System.IO;

namespace NetworkScience
{
    class debug
    {
        const bool isDebug = false;
        const int IGNORE_PERIOD = 5000;
        static int count = 0;
        static public void print(string msg)
        {
            if (isDebug || count % IGNORE_PERIOD == 0)
            {
                System.Console.WriteLine(msg);
            }
            count++;
        }
    }

    class UnionFind
    {
        private SortedDictionary<string, string> root = new SortedDictionary<string, string>();
        private SortedDictionary<string, int> rank = new SortedDictionary<string, int>();
        private SortedDictionary<string, int> size = new SortedDictionary<string, int>();
        private SortedDictionary<int, int> numOfSize = new SortedDictionary<int, int>();

        int gcSize = 0;
        int totalSize = 0;
        int S = 0;

        private void updateC(int size)
        {
            if (size > gcSize)
            {
                gcSize = size;
                debug.print("update " + gcSize);
            }
        }

        private void addSize(int size)
        {
            if (numOfSize.ContainsKey(size))
            {
                numOfSize[size] = numOfSize[size] + 1;
            }
            else
            {
                numOfSize[size] = 1;
            }
        }

        private void removeSize(int size)
        {
            numOfSize[size] = numOfSize[size] - 1;
        }

        public double GCRatio()
        {
            if (totalSize > 0)
            {
                return gcSize / (0.0 + totalSize);
            }
            else
            {
                return -1;
            }
        }

        public double sValue()
        {
            if (totalSize > 0)
            {
                return S / (0.0 + totalSize);
            }
            else
            {
                return -1;
            }
        }

        public void Add(string obj)
        {
            if (!root.ContainsKey(obj))
            {
                root[obj] = obj;
                rank[obj] = 0;
                size[obj] = 1;
                totalSize += 1;

                addSize(1);
                updateC(1);
            }
        }

        public bool isConnected(string p, string q)
        {
            return Root(p).Equals(Root(q));
        }

        public void Union(string p, string q)
        {
            if (!root.ContainsKey(p) || !root.ContainsKey(q))
            {
                throw new Exception("Trying to connect node that has not been added");
            }

            if (isConnected(p, q))
                return;

            string rootP = Root(p);
            string rootQ = Root(q);
            int sizeP = size[rootP];
            int sizeQ = size[rootQ];
            int sumSize = sizeP + sizeQ;
            if (rank[rootP] < rank[rootQ])
            {
                root[rootP] = rootQ;
                size[rootQ] = sumSize;
            }
            else if (rank[rootP] > rank[rootQ])
            {
                root[rootQ] = rootP;
                size[rootP] = sumSize;
            }
            else
            {
                root[rootQ] = rootP;
                rank[rootP] += 1;
                size[rootP] = sumSize;
            }

            S += gcSize * gcSize * numOfSize[gcSize];

            S -= sizeP * sizeP;
            S -= sizeQ * sizeQ;
            S += sumSize * sumSize;

            removeSize(sizeP);
            removeSize(sizeQ);
            addSize(sumSize);

            updateC(sumSize);

            S -= gcSize * gcSize * numOfSize[gcSize];
        }

        private string Root(string el)
        {
            if (root[el] != el)
            {
                root[el] = Root(root[el]);
            }
            return root[el];
        }
    }

    class Data
    {
        public SortedDictionary<Tuple<string, string>, double> edges = new SortedDictionary<Tuple<string, string>, double>();

        public void addE(string from, string to, double amount)
        {
            var key = new Tuple<string, string>(from, to);
            if (edges.ContainsKey(key))
            {
                edges[key] = edges[key] + amount;
            }
            else
            {
                edges.Add(key, amount);
            }
        }
    }

    class Program
    {
        // local
        //private const string DOCDB_URL = "https://localhost:8081";
        //private const string DOCDB_AUTHKEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

        // azure
        private const string DOCDB_URL = "https://iiis-graphview-test2.documents.azure.com:443/";
        private const string DOCDB_AUTHKEY = "Rzxzs7fklFYQApb0VWIx2fP3AakbCBDxfuzoQrFg5Ysuh6zlKkOTzOf091fYieteKQ72qtwsdggyAq6tMN6J6w==";

        private const string DOCDB_DATABASE = "NetworkS";
        private const string DOCDB_COLLECTION = "btntest_par";

        private const string NODE_LABEL = "user";
        private const string EDGE_LABEL = "transfer";
        private const string NODE_PROPERTY = "idx";
        private const string EDGE_PROPERTY = "amount";

        private const string CONNECT_EDGE_LABEL = EDGE_LABEL;//"connected";
        private const string CONNECT_EDGE_PROPERTY = EDGE_PROPERTY;//"total amount";

        private const string INPUT_EDGE = @"c:\edges.csv";
        private const string OUTPUT_ADD_FROM_WEAK = @"gc_ADD_FROM_WEAK.csv";
        private const string OUTPUT_ADD_FROM_STRONG = @"gc_ADD_FROM_STRONG.csv";
        private const string OUTPUT_OVERLAP = @"overlap.csv";

        private const int PAR = 1024;
        private const string INTER_NODES = @"i_nodes.csv.";
        private const string INTER_EDGES = @"i_edges.csv.";

        private static Microsoft.Win32.RegistryKey key =
            Microsoft.Win32.Registry.CurrentUser.CreateSubKey("PROCESS_NETWORK_SCIENCE");

        private static string KEY_STAGE = "stage";
        // 0;split;1;create node;2;create edge;3;merge;4;connect;5;query;6
        private static string KEY_PAR_NODE = "node_par_";
        private static string KEY_PAR_EDGE = "edge_par_";


        /* static void split()
         {
             HashSet<string> nodes = new HashSet<string>();

             using (TextFieldParser parser = new TextFieldParser(INPUT_EDGE))
             {
                 parser.TextFieldType = FieldType.Delimited;
                 parser.SetDelimiters(",");
                 while (!parser.EndOfData)
                 {
                     string[] fields = parser.ReadFields();
                     string start = fields[1];
                     string end = fields[2];

                     // remove self circle
                     if (start.Equals(end))
                     {
                         continue;
                     }
                     nodes.Add(start);
                     nodes.Add(end);
                 }
             }

             List<string> nodeList = nodes.ToList();
             int M = (int)Math.Ceiling( (nodeList.Count +0.0) / PAR);

             //debug.print(nodeList.Count+"");

             for (int i = 0; i < PAR; i++)
             {
                 using (System.IO.StreamWriter file =
                     new System.IO.StreamWriter(INTER_NODES + i))
                 {
                     for (int j = M * i; j < Math.Min(M * (i + 1), nodeList.Count); j++)
                     {
                         file.WriteLine(nodeList[j]);
                         //debug.print(j + "");
                     }
                 }
             }

             System.IO.StreamWriter[] files = new System.IO.StreamWriter[PAR];
             for (int i = 0; i < PAR; i++)
             {
                 files[i] = new System.IO.StreamWriter(INTER_EDGES + i);
             }

             using (TextFieldParser parser = new TextFieldParser(INPUT_EDGE))
             {
                 SHA256 mySHA256 = SHA256Managed.Create();

                 parser.TextFieldType = FieldType.Delimited;
                 parser.SetDelimiters(",");
                 while (!parser.EndOfData)
                 {
                     string[] fields = parser.ReadFields();
                     string start = fields[1];
                     string end = fields[2];
                     double amount = double.Parse(fields[4], CultureInfo.InvariantCulture);

                     // remove self circle
                     if (start.Equals(end))
                     {
                         continue;
                     }

                     var x = start.GetHashCode();
                     files[Math.Abs(x) % PAR].WriteLine(start + "," + end + "," + amount);
                 }
             }


             for (int i = 0; i < PAR; i++)
             {
                 files[i].Close();
             }
             System.Console.WriteLine("split");
         }
         */

        static void addNodeAll(int limit)
        {

            //GraphViewCommand graph = new GraphViewCommand(connection);

            Parallel.For(0, PAR, new ParallelOptions { MaxDegreeOfParallelism = limit }, i =>
            {
                System.Console.WriteLine("Adding nodes part " + i);
                //addNodePart(i);

                ProcessStartInfo startInfo = new ProcessStartInfo();
                startInfo.CreateNoWindow = false;
                startInfo.UseShellExecute = false;
                startInfo.FileName = "NetworkScience.exe";
                startInfo.WindowStyle = ProcessWindowStyle.Hidden;
                startInfo.Arguments = "node-par " + i;


                // Start the process with the info we specified.
                // Call WaitForExit and then the using-statement will close.
                using (Process exeProcess = Process.Start(startInfo))
                {
                    exeProcess.WaitForExit();
                }


            });
            System.Console.WriteLine("Nodes added");
        }

        static void addNodePart(int p)
        {
            GraphViewConnection connection =
                new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            GraphViewCommand graph = new GraphViewCommand(connection);

            int process = (int)key.GetValue(KEY_PAR_NODE + p);
            int count = 0;

            using (TextFieldParser parser = new TextFieldParser(INTER_NODES + p))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");
                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();
                    string node = fields[0];
                    if (count > process)
                    {
                        debug.print("[" + p + "]: Node " + node + " adding");
                        graph.g().AddV(NODE_LABEL).Property(NODE_PROPERTY, node).Next();

                        key.SetValue(KEY_PAR_NODE + p, count);

                    }
                    count++;
                }
            }
        }

        static void addEdgesAll(int limit)
        {
            Parallel.For(0, PAR, new ParallelOptions { MaxDegreeOfParallelism = limit }, i =>
            {
                System.Console.WriteLine("Adding edges part " + i);

                //addEdgesPart(i);

                ProcessStartInfo startInfo = new ProcessStartInfo();
                startInfo.CreateNoWindow = false;
                startInfo.UseShellExecute = false;
                startInfo.FileName = "NetworkScience.exe";
                startInfo.WindowStyle = ProcessWindowStyle.Hidden;
                startInfo.Arguments = "edge-par " + i;


                // Start the process with the info we specified.
                // Call WaitForExit and then the using-statement will close.
                using (Process exeProcess = Process.Start(startInfo))
                {
                    exeProcess.WaitForExit();
                }

            });
            System.Console.WriteLine("Edges added");
        }

        static void addEdgesPart(int p)
        {
            GraphViewConnection connection =
                new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            GraphViewCommand graph = new GraphViewCommand(connection);

            int process = (int)key.GetValue(KEY_PAR_EDGE + p);
            int count = 0;

            using (TextFieldParser parser = new TextFieldParser(INTER_EDGES + p))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");
                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();
                    string start = fields[0];
                    string end = fields[1];
                    double amount = double.Parse(fields[2], CultureInfo.InvariantCulture);
                    if (count > process)
                    {
                        // remove self circle
                        if (start.Equals(end))
                        {
                            continue;
                        }

                        debug.print("[" + p + "]: Edge: " + start + "-> " + end + ": " + amount + " adding");

                        graph.g().V().Has(NODE_PROPERTY, start).
                               AddE(EDGE_LABEL).Property(EDGE_PROPERTY, amount).
                               To(graph.g().V().Has(NODE_PROPERTY, end)).Next();

                        key.SetValue(KEY_PAR_EDGE + p, count);

                    }
                    count++;
                }
            }
            //System.Console.WriteLine(count);
        }

        static Tuple<int, int> countVE()
        {
            GraphViewConnection connection =
               new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            GraphViewCommand graph = new GraphViewCommand(connection);

            int countV = 0;
            var res = graph.g().V().Values(NODE_PROPERTY).Next();
            foreach (var i in res)
            {
                //debug.print(i);
                countV++;
            }

            int countE = 0;
            res = graph.g().E().Values(EDGE_PROPERTY).Next();
            foreach (var i in res)
            {
                debug.print(i);
                countE++;
            }
            return new Tuple<int, int>(countV, countE);
        }

        static void initKey()
        {
            key.SetValue(KEY_STAGE, 0);
            for (int i = 0; i < PAR; i++)
            {
                key.SetValue(KEY_PAR_NODE + i, -1);
                key.SetValue(KEY_PAR_EDGE + i, -1);
            }
        }

        static void clear()
        {
            if (key.GetValue(KEY_STAGE) != null)
            {
                key.DeleteValue(KEY_STAGE);

            }

            for (int i = 0; i < PAR; i++)
            {
                if (key.GetValue(KEY_PAR_NODE + i) != null)
                {
                    key.DeleteValue(KEY_PAR_NODE + i);

                }
                if (key.GetValue(KEY_PAR_EDGE + i) != null)
                {
                    key.DeleteValue(KEY_PAR_EDGE + i);

                }
            }
        }

        static void displayKey()
        {
            debug.print("stage: " + key.GetValue(KEY_STAGE));
            for (int i = 0; i < PAR; i++)
            {
                System.Console.WriteLine("par_node[" + i + "]: " + key.GetValue(KEY_PAR_NODE + i));

            }
            for (int i = 0; i < PAR; i++)
            {

                System.Console.WriteLine("par_edge[" + i + "]: " + key.GetValue(KEY_PAR_EDGE + i));
            }
        }

        static bool isInited()
        {
            return !(key.GetValue(KEY_STAGE) == null);
        }

        static void createGraph()
        {
            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            connection.ResetCollection();
            GraphViewCommand graph = new GraphViewCommand(connection);

            HashSet<string> nodes = new HashSet<string>();

            using (TextFieldParser parser = new TextFieldParser(INPUT_EDGE))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");
                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();
                    string start = fields[1];
                    string end = fields[2];
                    double amount = double.Parse(fields[4], CultureInfo.InvariantCulture);

                    // remove self circle
                    if (start.Equals(end))
                    {
                        continue;
                    }

                    if (!nodes.Contains(start))
                    {
                        graph.g().AddV(NODE_LABEL).Property(NODE_PROPERTY, start).Next();
                        debug.print("Node " + start + " added");
                    }
                    if (!nodes.Contains(end))
                    {
                        graph.g().AddV(NODE_LABEL).Property(NODE_PROPERTY, end).Next();
                        debug.print("Node " + end + " added");
                    }
                    nodes.Add(start);
                    nodes.Add(end);

                    graph.g().V().Has(NODE_PROPERTY, start).
                           AddE(EDGE_LABEL).Property(EDGE_PROPERTY, amount).
                           To(graph.g().V().Has(NODE_PROPERTY, end)).Next();
                    debug.print("Edge: " + start + "-> " + end + ": " + amount + " added");
                }
            }
        }

        static private void mergeEdges()
        {
            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            GraphViewCommand graph = new GraphViewCommand(connection);

            var src_res = graph.g().V().Values(NODE_PROPERTY).Next();
            foreach (var src in src_res)
            {
                var dst_res = graph.g().V().Has(NODE_PROPERTY, src).Out().Values(NODE_PROPERTY).Dedup().Next();
                foreach (var dst in dst_res)
                {
                    double sum = 0;
                    var edges_res = graph.g().V().Has(NODE_PROPERTY, src).OutE(EDGE_LABEL).As("e").
                        InV().Has(NODE_PROPERTY, dst).Select("e").Values(EDGE_PROPERTY).Next();
                    foreach (var value in edges_res)
                    {
                        sum += double.Parse(value, CultureInfo.InvariantCulture);
                    }

                    graph.g().V().Has(NODE_PROPERTY, src).OutE(EDGE_LABEL).As("e").
                        InV().Has(NODE_PROPERTY, dst).Select("e").Drop().Next();

                    graph.g().V().Has(NODE_PROPERTY, src).
                          AddE(EDGE_LABEL).Property(EDGE_PROPERTY, sum).
                          To(graph.g().V().Has(NODE_PROPERTY, dst)).Next();

                    debug.print("Merged Edge: " + src + "-> " + dst + ": " + sum + " added");
                }
            }
        }

        static private bool threshold_reached(double amount1, double amount2)
        {
            const double SINGLE_THRESHOLD = 0.05;
            const double SUM_THRESHOLD = 0.1;
            return (amount1 > SINGLE_THRESHOLD && amount2 > SINGLE_THRESHOLD
                && amount1 + amount2 > SUM_THRESHOLD);
        }

        static private void validConnect()
        {
            System.Console.WriteLine("Validating");

            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            GraphViewCommand graph = new GraphViewCommand(connection);

            var src_res = graph.g().V().Values(NODE_PROPERTY).Next();
            foreach (var src in src_res)
            {

                var dst_res = graph.g().V().Has(NODE_PROPERTY, src).Out().Values(NODE_PROPERTY).Dedup().Next();
                foreach (var dst in dst_res)
                {
                    double outAmount = -1;
                    double inAmount = -1;

                    // TODO: bothE not implemented, have so explictly call twice
                    var edges_res = graph.g().V().Has(NODE_PROPERTY, src).OutE(EDGE_LABEL).As("e").
                        InV().Has(NODE_PROPERTY, dst).Select("e").Values(EDGE_PROPERTY).Next();
                    foreach (var value in edges_res)
                    {
                        outAmount = double.Parse(value, CultureInfo.InvariantCulture);
                    }
                    edges_res = graph.g().V().Has(NODE_PROPERTY, dst).OutE(EDGE_LABEL).As("e").
                        InV().Has(NODE_PROPERTY, src).Select("e").Values(EDGE_PROPERTY).Next();
                    foreach (var value in edges_res)
                    {
                        inAmount = double.Parse(value, CultureInfo.InvariantCulture);
                    }

                    graph.g().V().Has(NODE_PROPERTY, src).OutE(EDGE_LABEL).As("e").
                           InV().Has(NODE_PROPERTY, dst).Select("e").Drop().Next();

                    graph.g().V().Has(NODE_PROPERTY, dst).OutE(EDGE_LABEL).As("e").
                        InV().Has(NODE_PROPERTY, src).Select("e").Drop().Next();


                    if (threshold_reached(inAmount, outAmount))
                    {
                        graph.g().V().Has(NODE_PROPERTY, src).
                            AddE(CONNECT_EDGE_LABEL).Property(CONNECT_EDGE_PROPERTY, inAmount + outAmount).
                                To(graph.g().V().Has(NODE_PROPERTY, dst)).Next();
                        debug.print("Add connected edge: " + src + " <-> " + dst + " : " + (inAmount + outAmount));
                    }
                }
            }
        }

        static private List<Tuple<double, double>> connectivityByAddingEdgesOrderly(bool addFromSmall)
        {
            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            GraphViewCommand graph = new GraphViewCommand(connection);

            //TODO: order by method of GraphView not implemented yet, have to sort locally here
            List<Tuple<string, string, double>> edges = new List<Tuple<string, string, double>>();
            HashSet<string> nodes = new HashSet<string>();

            var src_res = graph.g().V().Values(NODE_PROPERTY).Next();
            foreach (var src in src_res)
            {
                var dst_res = graph.g().V().Has(NODE_PROPERTY, src).Out().Values(NODE_PROPERTY).Dedup().Next();
                foreach (var dst in dst_res)
                {
                    var edges_res = graph.g().V().Has(NODE_PROPERTY, src).OutE(CONNECT_EDGE_LABEL).As("e").
                        InV().Has(NODE_PROPERTY, dst).Select("e").Values(CONNECT_EDGE_PROPERTY).Next();

                    double amount = -1;
                    foreach (var value in edges_res)
                    {
                        amount = double.Parse(value, CultureInfo.InvariantCulture);
                    }

                    if (amount < 0)
                    {
                        continue;
                    }

                    edges.Add(new Tuple<string, string, double>(src, dst, amount));
                    nodes.Add(src);
                    nodes.Add(dst);
                }
            }

            edges.Sort((x, y) => { return (addFromSmall ? 1 : -1) * x.Item3.CompareTo(y.Item3); });

            List<Tuple<double, double>> ratiosAndsValues = new List<Tuple<double, double>>();

            UnionFind gc = new UnionFind();

            foreach (var node in nodes)
            {
                gc.Add(node);
            }

            foreach (var edge in edges)
            {
                string start = edge.Item1;
                string end = edge.Item2;

                gc.Union(start, end);
                ratiosAndsValues.Add(new Tuple<double, double>(gc.GCRatio(), gc.sValue()));
                debug.print("Connect " + start + " and " + end + ": " + edge.Item3 + " [" + gc.GCRatio() + " ]");
            }
            return ratiosAndsValues;
        }

        private static List<Tuple<double, double>> overlapBetweenNodes()
        {
            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            GraphViewCommand graph = new GraphViewCommand(connection);

            List<Tuple<double, double>> overlap = new List<Tuple<double, double>>();

            var src_res = graph.g().V().Values(NODE_PROPERTY).Next();
            foreach (var src in src_res)
            {
                var dst_res = graph.g().V().Has(NODE_PROPERTY, src).Out().Values(NODE_PROPERTY).Dedup().Next();
                foreach (var dst in dst_res)
                {
                    var edges_res = graph.g().V().Has(NODE_PROPERTY, src).OutE(CONNECT_EDGE_LABEL).As("e").
                        InV().Has(NODE_PROPERTY, dst).Select("e").Values(CONNECT_EDGE_PROPERTY).Next();

                    double amount = -1;
                    foreach (var value in edges_res)
                    {
                        amount = double.Parse(value, CultureInfo.InvariantCulture);
                    }

                    int sCount = 0;
                    var res = graph.g().V().Has(NODE_PROPERTY, src).Out().Next();
                    foreach (var i in res)
                    {
                        sCount++;
                    }
                    res = graph.g().V().Has(NODE_PROPERTY, src).In().Next();
                    foreach (var i in res)
                    {
                        sCount++;
                    }

                    int dCount = 0;
                    res = graph.g().V().Has(NODE_PROPERTY, dst).Out().Next();
                    foreach (var i in res)
                    {
                        dCount++;
                    }
                    res = graph.g().V().Has(NODE_PROPERTY, dst).In().Next();
                    foreach (var i in res)
                    {
                        dCount++;
                    }

                    int nCount = 0;
                    res = graph.g().V().Has(NODE_PROPERTY, src).In().In().Has(NODE_PROPERTY, dst).Next();
                    foreach (var i in res)
                    {
                        nCount++;
                    }

                    res = graph.g().V().Has(NODE_PROPERTY, src).Out().Out().Has(NODE_PROPERTY, dst).Next();
                    foreach (var i in res)
                    {
                        nCount++;
                    }

                    res = graph.g().V().Has(NODE_PROPERTY, src).In().Out().Has(NODE_PROPERTY, dst).Next();
                    foreach (var i in res)
                    {
                        nCount++;
                    }

                    res = graph.g().V().Has(NODE_PROPERTY, src).Out().In().Has(NODE_PROPERTY, dst).Next();
                    foreach (var i in res)
                    {
                        nCount++;
                    }

                    overlap.Add(new Tuple<double, double>((nCount + 0.0) / (sCount + dCount - 2 - nCount), amount));

                }
            }

            return overlap;
        }

        private static void writeResults(string filename, List<Tuple<double, double>> list)
        {
            using (System.IO.StreamWriter file =
              new System.IO.StreamWriter(filename))
            {
                file.WriteLine("RATIO/OVERLAP,S/STRENGTH");
                foreach (var res in list)
                {
                    file.WriteLine(res.Item1 + "," + res.Item2);
                }
            }
        }

        static void build_local()
        {
            Data data = new Data();
            HashSet<string> nodes = new HashSet<string>();

            using (TextFieldParser parser = new TextFieldParser(INPUT_EDGE))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");
                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();
                    string start = fields[1];
                    string end = fields[2];
                    double amount = double.Parse(fields[4], CultureInfo.InvariantCulture);

                    // remove self circle
                    if (start.Equals(end))
                    {
                        continue;
                    }
                    nodes.Add(start);
                    nodes.Add(end);
                    data.addE(start, end, amount);
                }
            }

            List<string> nodeList = nodes.ToList();
            int M = (int)Math.Ceiling((nodeList.Count + 0.0) / PAR);

            //debug.print(nodeList.Count+"");

            for (int i = 0; i < PAR; i++)
            {
                using (System.IO.StreamWriter file =
                    new System.IO.StreamWriter(INTER_NODES + i))
                {
                    for (int j = M * i; j < Math.Min(M * (i + 1), nodeList.Count); j++)
                    {
                        file.WriteLine(nodeList[j]);
                        //debug.print(j + "");
                    }
                }
            }

            System.IO.StreamWriter[] files = new System.IO.StreamWriter[PAR];
            for (int i = 0; i < PAR; i++)
            {
                files[i] = new System.IO.StreamWriter(INTER_EDGES + i);
            }

            SortedDictionary<Tuple<string, string>, double> edges = new SortedDictionary<Tuple<string, string>, double>();

            foreach (var edge in data.edges)
            {
                var start = edge.Key.Item1;
                var end = edge.Key.Item2;
                var amount = edge.Value;

                var key = edge.Key;
                var key2 = new Tuple<string, string>(end, start);

                // remove self circle
                if (start.Equals(end))
                {
                    continue;
                }

                if (edges.ContainsKey(key) ||
                    edges.ContainsKey(key2))
                {
                    continue;
                }

                
                if (!data.edges.ContainsKey(key2))
                {
                    continue;
                }

                var amount2 = data.edges[key2];
                
                if (!threshold_reached(amount, amount2))
                {
                    continue;
                }

                edges.Add(key, amount2 + amount);
            }

            foreach(var edge in edges)
            {
                var start = edge.Key.Item1;
                var end = edge.Key.Item2;
                var amount = edge.Value;
                var x = edge.GetHashCode();
                files[Math.Abs(x) % PAR].WriteLine(start + "," + end + "," + amount);
            }


            for (int i = 0; i < PAR; i++)
            {
                files[i].Close();
            }
            System.Console.WriteLine("split");
        }





        static void Main(string[] args)
        {
            try
            {
                if (args.Count() >= 1 && args[0].Equals("clean"))
                {
                    System.Console.WriteLine("clear...");
                    clear();
                    return;
                }

                if (args.Count() >= 2 && args[0].Equals("node-par"))
                {
                    //System.Console.WriteLine("Adding nodes part "+ args[1] + " ...");
                    addNodePart(int.Parse(args[1]));
                    return;
                }

                if (args.Count() >= 2 && args[0].Equals("edge-par"))
                {
                    //System.Console.WriteLine("Adding edges part " + args[1] + " ...");
                    addEdgesPart(int.Parse(args[1]));
                    return;
                }

                if (!isInited())
                {
                    System.Console.WriteLine("init...");
                    initKey();
                }

                displayKey();

                //int stage = (int)key.GetValue(KEY_STAGE, -1);

                switch (args[0])
                {
                    case "split": { build_local(); key.SetValue(KEY_STAGE, 1); break; }
                    case "graph":
                        {
                            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
                            connection.ResetCollection();
                            initKey();
                            key.SetValue(KEY_STAGE, 1);
                            break;
                        }
                    case "node": { addNodeAll(PAR/7); key.SetValue(KEY_STAGE, 2); break; }
                    case "edge": { addEdgesAll(PAR/7); key.SetValue(KEY_STAGE, 3); break; }
                    //case "connect": { validConnect(); key.SetValue(KEY_STAGE, 5); break; }
                    case "compute":{
                            Parallel.Invoke(
                                     () => writeResults(OUTPUT_ADD_FROM_WEAK, connectivityByAddingEdgesOrderly(true)),
                                     () => writeResults(OUTPUT_ADD_FROM_STRONG, connectivityByAddingEdgesOrderly(false)),
                                     () => writeResults(OUTPUT_OVERLAP, overlapBetweenNodes())); key.SetValue(KEY_STAGE, 6);
                            break;
                        }
                    case "check":
                        {
                            var resx = countVE();
                            System.Console.WriteLine("nodes: " + resx.Item1 + "; edges: " + resx.Item2);
                            break;
                        }
                }


                /*createGraph();
                System.Console.WriteLine("Graph created\n");

                mergeEdges();
                System.Console.WriteLine("Edge Merged\n");

                validConnect();
                System.Console.WriteLine("Edge Evaluation Done\n");*/

                /*Parallel.Invoke(
                    () => writeResults(OUTPUT_ADD_FROM_WEAK, connectivityByAddingEdgesOrderly(true)),
                    () => writeResults(OUTPUT_ADD_FROM_STRONG, connectivityByAddingEdgesOrderly(false)),
                    () => writeResults(OUTPUT_OVERLAP, overlapBetweenNodes()));*/



                //test();
                //
            }
            catch (Exception e)
            {
                System.Console.WriteLine(e.Message);
            }



            System.Console.Write("Finished ");
            //System.Console.ReadKey();
        }

        static void test()
        {
            GraphViewConnection connection =
                new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            //connection.ResetCollection();
            GraphViewCommand graph = new GraphViewCommand(connection);

            try
            {
                /*graph.g().AddV(NODE_LABEL).Property(NODE_PROPERTY, "1").Next();
                graph.g().AddV(NODE_LABEL).Property(NODE_PROPERTY, "3").Next();
                graph.g().AddV(NODE_LABEL).Property(NODE_PROPERTY, "2").Next();

                graph.g().V().Has(NODE_PROPERTY, "1").
                               AddE(EDGE_LABEL).Property(EDGE_PROPERTY, 1).
                               To(graph.g().V().Has(NODE_PROPERTY, "2")).Next();
                graph.g().V().Has(NODE_PROPERTY, "1").
                              AddE(EDGE_LABEL).Property(EDGE_PROPERTY, -2).
                              To(graph.g().V().Has(NODE_PROPERTY, "3")).Next();
*/              /*  graph.g().V().Has(NODE_PROPERTY, "6242519").
                              AddE(EDGE_LABEL).Property(EDGE_PROPERTY, 1.35183386).
                              To(graph.g().V().Has(NODE_PROPERTY, "343")).Next();
                              */
                //graph.CommandText = "g.E().Order().By("+EDGE_PROPERTY+", incr)";
                //var res= graph.Execute();

                //var res = graph.g().V().Has(NODE_PROPERTY, "12").BothV().Has(NODE_PROPERTY, "2").Values(NODE_PROPERTY).Next();

                //graph.g().V().Has(NODE_PROPERTY, "1").OutE(EDGE_LABEL).As("e").InV().Has(NODE_PROPERTY, "2").Select("e").Drop().Next();

                /*graph.g().V().Has(NODE_PROPERTY, "198036").OutE(EDGE_LABEL).As("e").
                            InV().Has(NODE_PROPERTY, "3888").Select("e").Drop().Next();
                            */
                var res = graph.g().V().Has(NODE_PROPERTY, "198036").OutE(EDGE_LABEL).As("e").
                        InV().Has(NODE_PROPERTY, "3888").Select("e").Next();




                foreach (var x in res)
                {
                    System.Console.WriteLine(x);

                }
                System.Console.WriteLine("");




            }
            finally
            {
                //connection.ResetCollection();
            }
        }
    }
}
