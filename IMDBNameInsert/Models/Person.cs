using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBNameInsert.Models
{
    public class Person
    {
        public string nconst { get; set; }
        public string? primaryName { get; set; }
        public int? birthYear { get; set; }
        public int? deathYear { get; set; }
        public string? primaryProfession { get; set; }
        public string? knownForTitles { get; set; }
    }
}
