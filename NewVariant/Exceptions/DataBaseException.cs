using System;
using System.Runtime.Serialization;

namespace NewVariant.Exceptions {
    [Serializable]
    public class DataBaseException : Exception {
        public DataBaseException() { }
        public DataBaseException(string message) : base(message) { }
        public DataBaseException(string message, Exception inner) : base(message, inner) { }
        protected DataBaseException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }
}