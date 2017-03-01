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
    class UnionFind
    {
        private SortedDictionary<string, string> ids = new SortedDictionary<string, string>();
        private SortedDictionary<string, int> rank = new SortedDictionary<string, int>();

        public void Add(string obj)
        {
            if (!ids.ContainsKey(obj))
            {
                ids[obj] = obj;
                rank[obj] = 0;
            }
        }

        /// <summary>
        /// Checks whether two provided points are connected
        /// </summary>
        /// <returns>True if elements are connected</returns>
        public bool Connected(string p, string q)
        {
            return Root(p) == Root(q);
        }

        /// <summary>
        /// Makes a connection between first and second element
        /// </summary>
        public void Union(string p, string q)
        {
            Add(p);
            Add(q);

            if (Connected(p, q))
                return;

            string rootP = Root(p);
            string rootQ = Root(q);
            if (rank[rootP] < rank[rootQ])
            {
                ids[rootP] = rootQ;
            }
            else if(rank[rootP] > rank[rootQ])
            {
                ids[rootQ] = rootP;   
            }
            else
            {
                ids[rootQ] = rootP;
                rank[rootP] += 1;
            }
        }

        private string Root(string el)
        {
            if(ids[el] != el)
            {
                ids[el] = Root(ids[el]);
            }
            return ids[el];
        }

        public double GiantComponentRatio()
        {
            SortedDictionary<string, int> componentCount = new SortedDictionary<string, int>();
            foreach(string k in ids.Keys)
            {
                var root = ids[k];
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
        }
    }

    class Program
    {
        private const string DOCDB_URL = "https://iiis-graphview-test2.documents.azure.com:443/";
        private const string DOCDB_AUTHKEY = "Rzxzs7fklFYQApb0VWIx2fP3AakbCBDxfuzoQrFg5Ysuh6zlKkOTzOf091fYieteKQ72qtwsdggyAq6tMN6J6w==";
        private const string DOCDB_DATABASE = "NetworkS";
        private const string DOCDB_COLLECTION = "TEST";

        private const string NODE_LABEL = "user";
        private const string EDGE_LABEL = "transfer";
        private const string NODE_PROPERTY = "idx";
        private const string EDGE_PROPERTY = "amount";

        private const string INPUT_EDGE = @"c:\edges.csv";

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
                        System.Console.WriteLine("Node " + start + " added");
                    }
                    if (!nodes.Contains(end))
                    {
                        graph.g().AddV(NODE_LABEL).Property(NODE_PROPERTY, end).Next();
                        System.Console.WriteLine("Node " + end + " added");
                    }
                    nodes.Add(start);
                    nodes.Add(end);

                    graph.g().V().Has(NODE_PROPERTY, start).
                           AddE(EDGE_LABEL).Property(EDGE_PROPERTY, amount).
                           To(graph.g().V().Has(NODE_PROPERTY, end)).Next();
                    System.Console.WriteLine("Edge: " + start + "-> " + end + ": " + amount + " added");
                }
            }
            System.Console.WriteLine("Graph created");
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

                    System.Console.WriteLine("Merged Edge: " + src + "-> " + dst + ": " + sum + " added");
                }
            }
            System.Console.WriteLine("Edge Merged");
        }


        static private List<double> dynamicConnectivity()
        {
            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            GraphViewCommand graph = new GraphViewCommand(connection);
            UnionFind gc = new UnionFind();

            List<double> ratios = new List<double>();

            var nodes_res = graph.g().V().Values(NODE_PROPERTY).Next();

            foreach(var node in nodes_res)
            {
                gc.Add(node);
            }

            var edges_sorted_res = graph.g().E().Next();

            foreach(var edge in edges_sorted_res)
            {
                string start = "";
                string end = "";
                double startValue = 0;
                double endValue = 0;

                if (true)
                {
                    gc.Connected(start, end);
                    ratios.Add(gc.GiantComponentRatio());
                }
            }

            return ratios;
        }



        static void Main(string[] args)
        {
            createGraph();
            mergeEdges();


            //test();
        }

        static void test()
        {
            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, "tmp");
            connection.ResetCollection();
            GraphViewCommand graph = new GraphViewCommand(connection);

            try
            {
                graph.g().AddV(NODE_LABEL).Property(NODE_PROPERTY, "1").Next();
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
                              To(graph.g().V().Has(NODE_PROPERTY, "2")).Next();

                graph.CommandText = "g.E().Order().By("+EDGE_PROPERTY+", incr)";
                var res= graph.Execute();

                //var res = graph.g().E().Order().By(EDGE_PROPERTY).Values(EDGE_PROPERTY).Next();

                //graph.g().V().Has(NODE_PROPERTY, "1").OutE(EDGE_LABEL).As("e").InV().Has(NODE_PROPERTY, "2").Select("e").Drop().Next();

                foreach (var x in res)
                {
                    System.Console.WriteLine(x);
                }
                System.Console.WriteLine("");

                res = graph.g().V().Has(NODE_PROPERTY, "1").Out().Values(NODE_PROPERTY).Next();

                foreach (var x in res)
                {
                    System.Console.WriteLine(x);
                }
            }
            finally
            {
                connection.ResetCollection();
            }
        }
    }
}
