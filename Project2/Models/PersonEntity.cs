// Michael O'Keefe
// Cloud Computing - Project 2
// Due 11/9/15
//
// PersonEntity
//
// Constructors plus get and set methods for each customer

using Microsoft.WindowsAzure.Storage.Table;


namespace Project2.Models
{
    public class PersonEntity : TableEntity
    {
        public PersonEntity(string lastName, string firstName)
        {
            this.PartitionKey = lastName;
            this.RowKey = firstName;
            this.Email = "";
            this.PhoneNumber = "";
            this.Gender = "";
            this.City = "";
        }

        public PersonEntity() { }

        public string Email { get; set; }

        public string PhoneNumber { get; set; }

        public string Gender { get; set; }

        public string City { get; set; }

    }
}