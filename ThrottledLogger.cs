using System;

namespace BackupTables
{
    public class ThrottledLogger
    {
        private int _logSecond;

        public void Add(string message)
        {
            var nowSecond = DateTime.UtcNow.Second;
            if (_logSecond != nowSecond)
            {
                _logSecond = nowSecond;
                Console.WriteLine(message);
            }
        }
    }
}
