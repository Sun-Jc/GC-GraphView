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

namespace NetworkScience
{
    class debug
    {
        static bool isDebug = true;
        static public void print(string msg)
        {
            if(isDebug)
            {
                System.Console.WriteLine(msg);
            }
        }
    }
    class UnionFind
    {
        private SortedDictionary<string, string> root = new SortedDictionary<string, string>();
        private SortedDictionary<string, int> rank = new SortedDictionary<string, int>();
        private SortedDictionary<string, int> size = new SortedDictionary<string, int>();

        int gcSize = 0;
        int totalSize = 0;

        private void updateC(int size)
        {
            if(size > gcSize)
            {
                gcSize = size;
                debug.print("update " + gcSize);
            }
        }

        public double GCRatio()
        {
            if(totalSize > 0)
            {
                return gcSize / (0.0 + totalSize);
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
            }
        }

        public bool isConnected(string p, string q)
        {
            return Root(p) == Root(q);
        }

        public void Union(string p, string q)
        {
            if(!root.ContainsKey(p) || !root.ContainsKey(q))
            {
                throw new Exception("Trying to connect node that has not been added");
            }

            if (isConnected(p, q))
                return;

            string rootP = Root(p);
            string rootQ = Root(q);
            if (rank[rootP] < rank[rootQ])
            {
                root[rootP] = rootQ;
                size[rootQ] += size[rootP];
            }
            else if(rank[rootP] > rank[rootQ])
            {
                root[rootQ] = rootP;
                size[rootP] += size[rootQ];

            }
            else
            {
                root[rootQ] = rootP;
                rank[rootP] += 1;
                size[rootP] += size[rootQ];
            }
            updateC(size[rootQ]);
            updateC(size[rootP]);
        }

        private string Root(string el)
        {
            if(root[el] != el)
            {
                root[el] = Root(root[el]);
            }
            return root[el];
        }


        /*public double GiantComponentRatio()
        {
            SortedDictionary<string, int> componentCount = new SortedDictionary<string, int>();
            foreach(string k in root.Keys)
            {
                var root = this.root[k];
                if (componentCount.ContainsKey(root))
                {
                    componentCount[root] = componentCount[root] + 1;
                }else
                {
                    componentCount[root] = 1;
                }
            }

            int max = 0;
            int sum = 0;
            foreach(string r in componentCount.Keys)
            {
                var c = componentCount[r];
                sum += c;
                if( c > max)
                {
                    max = c;
                }
            }
            if(sum > 0)
            {
                return max / (sum + 0.0);
            }else
            {
                return -1;
            }
        }*/
    }

    class Program
    {
        // local
        private const string DOCDB_URL = "https://localhost:8081";
        private const string DOCDB_AUTHKEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

        // azure
        //private const string DOCDB_URL = "https://iiis-graphview-test2.documents.azure.com:443/";
        //private const string DOCDB_AUTHKEY = "Rzxzs7fklFYQApb0VWIx2fP3AakbCBDxfuzoQrFg5Ysuh6zlKkOTzOf091fYieteKQ72qtwsdggyAq6tMN6J6w==";

        private const string DOCDB_DATABASE = "NetworkS";
        private const string DOCDB_COLLECTION = "btntest";

        private const string NODE_LABEL = "user";
        private const string EDGE_LABEL = "transfer";
        private const string NODE_PROPERTY = "idx";
        private const string EDGE_PROPERTY = "amount";

        private const string CONNECT_EDGE_LABEL = "connected";
        private const string CONNECT_EDGE_PROPERTY = "total amount";

        private const string INPUT_EDGE = @"c:\edges.csv";
        private const string OUTPUT_RATIOS = @"gc_ratios.csv";
      

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
                    if (start == end)
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
            System.Console.WriteLine("Graph created\n");
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
                    foreach(var value in edges_res)
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
            System.Console.WriteLine("Edge Merged\n");
        }

        static private bool threshold_reached(double amount1, double amount2)
        {
            const double SINGLE_THRESHOLD = 0.05;
            const double SUM_THRESHOLD = 0.1;
            return (amount1 > SINGLE_THRESHOLD && amount2 > SINGLE_THRESHOLD
                && amount1 + amount2 > SUM_THRESHOLD) ;
        }

        static private List<double> dynamicConnectivity()
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

                    if(threshold_reached(inAmount, outAmount))
                    {
                        edges.Add(new Tuple<string,string,double>(src, dst, inAmount + outAmount));
                        nodes.Add(src);
                        nodes.Add(dst);

                        graph.g().V().Has(NODE_PROPERTY, src).
                            AddE(CONNECT_EDGE_LABEL).Property(CONNECT_EDGE_PROPERTY, inAmount + outAmount).
                                To(graph.g().V().Has(NODE_PROPERTY, dst)).Next();

                        graph.g().V().Has(NODE_PROPERTY, src).OutE(EDGE_LABEL).As("e").
                            InV().Has(NODE_PROPERTY, dst).Select("e").Drop().Next();

                        graph.g().V().Has(NODE_PROPERTY, dst).OutE(EDGE_LABEL).As("e").
                            InV().Has(NODE_PROPERTY, src).Select("e").Drop().Next();

                        debug.print("Add connected edge: "+src+" <-> "+dst+" : "+(inAmount+outAmount));
                    }
                }
            }

            edges.Sort((x, y) => { return x.Item3.CompareTo(y.Item3); });

            /*foreach(var i in edges)
            {
                System.Console.WriteLine(i);
            }*/

            List<double> ratios = new List<double>();

            UnionFind gc = new UnionFind();

            foreach(var node in nodes)
            {
                gc.Add(node);
            }

            foreach(var edge in edges)
            {
                string start = edge.Item1;
                string end = edge.Item2;
               
                gc.Union(start, end);
                ratios.Add(gc.GCRatio());

                debug.print("Connect " + start + " and " + end + ": " + gc.GCRatio());
            }
            return ratios;
        }

        private static void writeResults(List<double> list)
        {
            using (System.IO.StreamWriter file =
              new System.IO.StreamWriter(OUTPUT_RATIOS))
            {
                foreach (double res in list)
                {
                    file.WriteLine(res);
                }
            }
        }



        static void Main(string[] args)
        {
            createGraph();
            mergeEdges();

            writeResults( dynamicConnectivity());

            //test();

            System.Console.Write("Finished");
            System.Console.ReadKey();
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
                graph.g().V().Has(NODE_PROPERTY, "1").
                              AddE(EDGE_LABEL).Property(EDGE_PROPERTY, 12).
                              To(graph.g().V().Has(NODE_PROPERTY, "2")).Next();*/

                //graph.CommandText = "g.E().Order().By("+EDGE_PROPERTY+", incr)";
                //var res= graph.Execute();

                var res = graph.g().V().Has(NODE_PROPERTY, "7").OutE(EDGE_LABEL).As("e").
                      InV().Has(NODE_PROPERTY, "5").Select("e").Drop().Next();

                //graph.g().V().Has(NODE_PROPERTY, "1").OutE(EDGE_LABEL).As("e").InV().Has(NODE_PROPERTY, "2").Select("e").Drop().Next();

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
