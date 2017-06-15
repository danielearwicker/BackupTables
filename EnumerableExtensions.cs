using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace BackupTables
{
    public static class EnumerableExtensions
    {
        // http://en.wikipedia.org/wiki/Topological_sorting
        public static IEnumerable<T> TopologicalSort<T>(this IEnumerable<T> source, Func<T, IEnumerable<T>> arrowsFrom)
        {
            var connected = source.ToList();

            Func<T, bool> noIncomingArrows = to => connected.All(node => !arrowsFrom(node).Contains(to));

            var inserted = new HashSet<T>();

            var l = new List<T>();
            var s = new Queue<T>(connected.Where(noIncomingArrows));
            while (s.Count != 0)
            {
                var n = s.Dequeue();
                if (inserted.Add(n))
                    l.Add(n);
                connected.Remove(n);
                foreach (var node in arrowsFrom(n).Where(noIncomingArrows))
                    s.Enqueue(node);
            }

            if (connected.Count != 0)
                throw new SystemException("circular dependencies");

            return l;
        }
    }
}
