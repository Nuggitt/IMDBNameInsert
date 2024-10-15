using IMDBNameInsert.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBNameInsert
{
    public interface IInserter
    {
       

        void Insert(List<Person> persons, SqlConnection sqlConn, SqlTransaction sqlTransaction, object? table);
    }
}
