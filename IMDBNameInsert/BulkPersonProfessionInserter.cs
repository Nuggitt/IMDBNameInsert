using IMDBNameInsert.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBNameInsert
{
    public class BulkPersonProfessionInserter : IInserter
    {
        public void Insert(List<Person> persons, SqlConnection sqlConn, SqlTransaction sqlTransaction)
        {
            throw new NotImplementedException();
        }
    }
    
}
