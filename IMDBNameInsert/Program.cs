
using IMDBNameInsert;
using IMDBNameInsert.Models;
using System.Data.SqlClient;

IInserter inserter;

Console.WriteLine("tast 1 for Persons, \r\n Tast 2 for Profession, \r\n Tast 3 for PersonProfession, \r\n Tast 4 for PersonTitles");

string input = Console.ReadLine();

switch (input)
{
    case "1":
        inserter = new BulkNameInserter();
        break;
    case "2":
        inserter = new BulkProfessionInserter();
        break;
    case "3":
        inserter = new BulkPersonProfessionInserter();
        break;
    case "4":
        inserter = new BulkPersonTitlesInserter();
        break;
    default:
        throw new Exception("Invalid input");
}

int lineCount = 0;
List<Person> persons = new List<Person>();
string filepath = "C:/IMDBData/name.basics.tsv";
foreach (string line in File.ReadLines(filepath).Skip(1)) // Skip header row
{
    if (lineCount == 14000000)
    {
        break;
    }

    string[] splitline = line.Split('\t');

    // Ensure the line has at least 4 columns
    if (splitline.Length < 4)
    {
        throw new Exception("Invalid line: " + line);
    }

    // Now safely access the first four fields
    string nconst = splitline[0];
    string primaryName = splitline[1];
    int? birthYear = ParseInt(splitline[2]);  // Handle possible null values
    int? deathYear = ParseInt(splitline[3]);  // Handle possible null values

    // Check for primaryProfession and knownForTitles only if present
    string primaryProfession = splitline.Length > 4 ? splitline[4] : null;
    string knownForTitles = splitline.Length > 5 ? splitline[5] : null;

    // Create and add a new Person object
    Person newPerson = new Person
    {
        nconst = nconst,
        primaryName = primaryName,
        birthYear = birthYear,
        deathYear = deathYear,
        primaryProfession = primaryProfession,
        knownForTitles = knownForTitles
    };

    persons.Add(newPerson);
    lineCount++;
}


Console.WriteLine("List of person length: " + persons.Count);

SqlConnection sqlConn = new SqlConnection("server=localhost;database=IMDB;" +
    "user id=sa;password=Holger1208!;TrustServerCertificate=True");

sqlConn.Open();

SqlTransaction sqlTransaction = sqlConn.BeginTransaction();

DateTime before = DateTime.Now;

try
{
    inserter.Insert(persons, sqlConn, sqlTransaction);
    sqlTransaction.Commit();
}
catch (SqlException ex)
{
    Console.WriteLine(ex.Message);
    sqlTransaction.Rollback();
}

DateTime after = DateTime.Now;

sqlConn.Close();

Console.WriteLine("milliseconds passed " + (after - before).TotalMilliseconds);

int? ParseInt(string value)
{
    if (value.ToLower() == "\\n") // checks if it is \n
    {
        return null;
    }

    if (int.TryParse(value, out int result))
    {
        return result;
    }
    else
    {
        // Handle the case where the value is not a valid integer
        return null;
    }
}


