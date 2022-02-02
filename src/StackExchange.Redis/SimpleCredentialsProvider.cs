using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis.Interfaces;

namespace StackExchange.Redis
{
    /// <summary>
    /// A credentials provider with constant password and user.
    /// </summary>
    public class SimpleCredentialsProvider : ICredentialsProvider
    {
        private readonly string user;
        private readonly string password;

        /// <summary>
        /// Returns a new credentials provider with the given user and password.
        /// </summary>
        public SimpleCredentialsProvider(string user, string password)
        {
            this.user = user;
            this.password = password;
        }

        string ICredentialsProvider.getPassword() => this.password;
        string ICredentialsProvider.getUser() => this.user;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;

            if (obj == this)
            {
                return true;
            }

            SimpleCredentialsProvider provider = obj as SimpleCredentialsProvider;

            return provider != null && provider.user == this.user && provider.password == this.password;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return this.user.GetHashCode()
                 ^ this.password.GetHashCode();
        }
    }
}
