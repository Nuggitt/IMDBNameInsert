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
    public class BulkProfessionInserter : IInserter
    {
        public void Insert(List<Person> persons, SqlConnection sqlConn, SqlTransaction sqlTransaction, object? table)
        {
            DataTable professionTable = new DataTable();

            DataColumn professionID = new DataColumn("ProfessionID", typeof(int));
            DataColumn primaryProfession = new DataColumn("Profession", typeof(string));

            professionTable.Columns.Add(professionID);
            professionTable.Columns.Add(primaryProfession);

            HashSet<string> uniqueprofessions = new HashSet<string>();
            int professionsIdCounter = 1;


            foreach (Person person in persons)
            {
                if (!string.IsNullOrWhiteSpace(person.PrimaryProfession))
                {
                    string[] professions = person.PrimaryProfession.Split(',');

                    foreach (string profession in professions.Select(p => p.Trim()).Distinct())
                    {
                        if (!uniqueprofessions.Contains(profession))
                        {
                            DataRow row = professionTable.NewRow();
                            row["ProfessionID"] = professionsIdCounter++;
                            row["Profession"] = profession;
                            professionTable.Rows.Add(row);
                            uniqueprofessions.Add(profession);

                        }
                    }
                }
            }

            SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.Default, sqlTransaction);
            bulkCopy.DestinationTableName = "dbo.Professions";
            bulkCopy.WriteToServer(professionTable);
        }
    }
}
