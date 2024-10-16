using IMDBNameInsert.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBNameInsert
{
    public class BulkNameInserter : IInserter
    {
        public void Insert(List<Person> persons, SqlConnection sqlConn, SqlTransaction sqlTransaction)
        {
            DataTable personTable = new DataTable();

            DataColumn nconst = new DataColumn("nconst", typeof(string));
            DataColumn primaryName = new DataColumn("primaryName", typeof(string));
            DataColumn birthYear = new DataColumn("birthYear", typeof(int));
            DataColumn deathYear = new DataColumn("deathYear", typeof(int));

            personTable.Columns.Add(nconst);
            personTable.Columns.Add(primaryName);
            personTable.Columns.Add(birthYear);
            personTable.Columns.Add(deathYear);

            foreach(Person person in persons)
            {
                DataRow row = personTable.NewRow();
                FillParameter(row, "nconst", person.Nconst);
                FillParameter(row, "primaryName", person.PrimaryName);
                FillParameter(row, "birthYear", person.BirthYear);
                FillParameter(row, "deathYear", person.DeathYear);
                personTable.Rows.Add(row);
            }

            SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.Default, sqlTransaction);
            bulkCopy.DestinationTableName = "dbo.Persons";
            bulkCopy.WriteToServer(personTable);

        }

        public void FillParameter(DataRow row, string columnName, object? value)
        {
            if (value != null)
            {
                row[columnName] = value;
            }
            else
            {
                row[columnName] = DBNull.Value;
            }
        }
    }

    
}
