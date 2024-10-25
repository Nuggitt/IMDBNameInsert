﻿using IMDBNameInsert.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;



namespace IMDBNameInsert
{
    public class BulkPersonTitlesInserter : IInserter
    {
        public void Insert(List<Person> persons, SqlConnection sqlConn, SqlTransaction sqlTransaction)
        {
            DataTable personTitleTable = new DataTable();
            personTitleTable.Columns.Add(new DataColumn("Nconst", typeof(string))); 
            personTitleTable.Columns.Add(new DataColumn("Tconst", typeof(string))); 

            int batchSize = 10000; 
            int totalRecords = 0; 

            for (int i = 0; i < persons.Count; i++)
            {
                foreach (string title in persons[i].KnownForTitles.Split(','))
                {
                    DataRow row = personTitleTable.NewRow();
                    FillParameter(row, "Tconst", title);
                    FillParameter(row, "Nconst", persons[i].Nconst);
                    personTitleTable.Rows.Add(row);
                }

                totalRecords += personTitleTable.Rows.Count;

                
                if (personTitleTable.Rows.Count >= batchSize || i == persons.Count - 1)
                {
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.Default, sqlTransaction))
                    {
                        bulkCopy.DestinationTableName = "dbo.PersonTitles";
                        bulkCopy.BulkCopyTimeout = 600; 
                        bulkCopy.WriteToServer(personTitleTable);
                    }
                    personTitleTable.Clear(); 
                    Console.WriteLine($"Inserted {totalRecords} records so far.");
                }
            }

            Console.WriteLine("Bulk insert completed.");
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

