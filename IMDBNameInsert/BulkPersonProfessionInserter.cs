using IMDBNameInsert.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace IMDBNameInsert
{
    public class BulkPersonProfessionInserter : IInserter
    {
        private Dictionary<string, int> professionMap;

        public BulkPersonProfessionInserter(SqlConnection sqlConn)
        {
            // Load profession names and their IDs into the dictionary
            professionMap = LoadProfessions(sqlConn);
        }

        private Dictionary<string, int> LoadProfessions(SqlConnection sqlConn)
        {
            var map = new Dictionary<string, int>();

            using (SqlCommand cmd = new SqlCommand("SELECT professionID, profession FROM dbo.Professions", sqlConn))
            {
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int professionID = reader.GetInt32(0);
                        string profession = reader.GetString(1);
                        map[profession] = professionID;
                    }
                }
            }

            return map;
        }

        private int GetProfessionIdByName(string professionName)
        {
            // Retrieve the profession ID from the dictionary
            return professionMap.TryGetValue(professionName, out int professionId) ? professionId : -1;
        }

        public void Insert(List<Person> persons, SqlConnection sqlConn, SqlTransaction sqlTransaction, object? table)
        {
            DataTable personProfessionTable = new DataTable();

            DataColumn personProfessionNconstCol = new DataColumn("Nconst", typeof(string));
            DataColumn professionIdCol = new DataColumn("ProfessionID", typeof(int));

            personProfessionTable.Columns.Add(personProfessionNconstCol);
            personProfessionTable.Columns.Add(professionIdCol);

            foreach (Person person in persons)
            {
                foreach (string profession in person.PrimaryProfession.Split(',').Select(p => p.Trim()).Distinct())
                {
                    DataRow row = personProfessionTable.NewRow();
                    FillParameter(row, "Nconst", person.Nconst);

                    // Get the professionID for the current profession
                    int professionId = GetProfessionIdByName(profession);

                    // Now assign the professionId instead of the profession name
                    FillParameter(row, "ProfessionID", professionId);

                    personProfessionTable.Rows.Add(row);
                }
            }

            SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.Default, sqlTransaction);
            bulkCopy.DestinationTableName = "dbo.PersonProfessions";
            bulkCopy.WriteToServer(personProfessionTable);
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
