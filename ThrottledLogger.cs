using System;

namespace BackupTables
{
    public class ThrottledLogger : IDisposable
    {
        private int _logSecond;
        
        private const int maxChars = 77;

        public void Log(string message)
        {
            Clear();
            Console.WriteLine(message);
        }

        private void Clear()
        {
            Console.Write("\r" + new string(' ', maxChars) + "\r");
        }

        public void Update(string message)
        {
            var nowSecond = DateTime.UtcNow.Second;
            if (_logSecond != nowSecond)
            {
                _logSecond = nowSecond;
                
                if (message.Length > maxChars)
                {
                    message = message.Substring(0, maxChars - 3) + "...";
                }

                if (message.Length < maxChars)
                {
                    message += new string(' ', maxChars - message.Length);
                }

                Console.Write("\r" + message);
            }
        }

        public void Dispose()
        {
            Clear();
        }
    }
}
