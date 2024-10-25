
using IMDBNameInsert;
using IMDBNameInsert.Models;
using System.Data.SqlClient;

IInserter inserter;

Console.WriteLine("tast 1 for Persons, \r\n Tast 2 for Profession, \r\n Tast 3 for PersonProfession, \r\n Tast 4 for PersonTitles");

string input = Console.ReadLine();
SqlConnection sqlConn = new SqlConnection("server=localhost;database=IMDB;" +
    "user id=sa;password=EnterYourPassWordHere!;TrustServerCertificate=True");
sqlConn.Open();

switch (input)
{
    case "1":
        inserter = new BulkNameInserter();
        break;
    case "2":
        inserter = new BulkProfessionInserter();
        break;
    case "3":
        inserter = new BulkPersonProfessionInserter(sqlConn);
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
foreach (string line in File.ReadLines(filepath).Skip(1)) 
{
    if (lineCount == 50000)
    {
        break;
    }

    string[] splitline = line.Split('\t');

    
    if (splitline.Length < 4)
    {
        throw new Exception("Invalid line: " + line);
    }

    
    string nconst = splitline[0];
    string primaryName = splitline[1];
    int? birthYear = ParseInt(splitline[2]);  
    int? deathYear = ParseInt(splitline[3]);  

    
    string primaryProfession = splitline.Length > 4 ? splitline[4] : null;
    string knownForTitles = splitline.Length > 5 ? splitline[5] : null;


    
    Person newPerson = new Person
    {
        Nconst = nconst,
        PrimaryName = primaryName,
        BirthYear = birthYear,
        DeathYear = deathYear,
        PrimaryProfession = primaryProfession,
        KnownForTitles = knownForTitles
    };

    persons.Add(newPerson);
    lineCount++;
}


Console.WriteLine("List of person length: " + persons.Count);





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
    if (value.ToLower() == "\\n") 
    {
        return null;
    }

    if (int.TryParse(value, out int result))
    {
        return result;
    }
    else
    {
        
        return null;
    }
}


