using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis.Interfaces
{
    /// <summary>
    /// Describes a provider of credentials for connection authentication.
    /// </summary>
    public interface ICredentialsProvider
    {

        /// <summary>
        /// This method returns the user name for the connection.
        /// The returned value can potentially change time, and shouldn't be cached.
        /// </summary>
        string getUser();

        /// <summary>
        /// This method returns the password for the connection.
        /// The returned value can potentially change time, and shouldn't be cached.
        /// </summary>
        string getPassword();
    }
}
