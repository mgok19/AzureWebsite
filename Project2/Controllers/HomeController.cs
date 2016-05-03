// Michael O'Keefe
// Cloud Computing - Project 2
// Due 11/9/15
//
// HomeController
//
// In charge of manipulating data in the MVC format. Handles connecting to 
// michaelostorage for access to the blob "sample.txt" and the table "people".
// Loads the data from sample.txt into the table. Returns query results based on
// the partition key (last name), row key (first name), and attribute.
//
// Assumptions:
// - sample.txt exists


using System;
using System.Web.Mvc;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure;
using Project2.Models;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;

namespace Project2.Controllers
{
    public class HomeController : Controller
    {


        public ActionResult Index() // home method
        {
            return View();
        }

        [HttpPost]
        public ActionResult LoadData()  // method called when loading data
        {
            
            // retrieve storage account from connection string for table and blob     
            CloudStorageAccount storageAccount =
            CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // create the table client
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // create the table if it doesn't exist
            CloudTable table = tableClient.GetTableReference("people");
            
            // delete the table if it exits so a new one can be loaded
            bool deleted = table.DeleteIfExists();  

            if (deleted)
                System.Threading.Thread.Sleep(45000); //wait 45 seconds for delete

            // create a new table
            table.Create(); 

            // create the blob client
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // retrieve a reference to a container, expected to already exist
            CloudBlobContainer container = blobClient.GetContainerReference("michaelocontainer");

            // retrieve reference to a blob named "sample.txt"
            CloudBlockBlob blockBlob = container.GetBlockBlobReference("sample.txt");
            
            // retrieve the blob contents in string form
            string text;
            using (var memoryStream = new MemoryStream())
            {
                blockBlob.DownloadToStream(memoryStream);
                text = System.Text.Encoding.UTF8.GetString(memoryStream.ToArray());
            }

            // traverse the string containing the blob contents and create each customer (or person)
            // based on the contents
            using (StringReader reader = new StringReader(text))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    char delimiter = ' ';

                    string[] customer = line.Split(delimiter);

                    //need to have at least a first and last name
                    if (customer.Length >= 2)
                    {
                        PersonEntity person = new PersonEntity(customer[0], customer[1]);
                        for (int i = 2; i < customer.Length; i++)
                        {
                            string attribute = customer[i];
                            if (attribute.Contains("Gender="))
                            {
                                person.Gender = attribute;
                            }
                            else if (attribute.Contains("Phone="))
                            {
                                person.PhoneNumber = attribute;
                            }
                            else if (attribute.Contains("City="))
                            {
                                person.City = attribute;
                            }
                            else if (attribute.Contains("Email="))
                            {
                                person.Email = attribute;
                            }
                            else // If an attribute does not match any of these then it is not a valid attribute
                            {
                                ViewBag.LoadStatus = "Load Failed - one or more customer attributes are invalid (case and format sensitive)";
                                return View();
                            }
                        }
                        // insert the person into the table (insert or replace because no duplicates)
                        TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(person);
                        table.Execute(insertOrReplaceOperation);
                    }
                    else
                    {
                        ViewBag.LoadStatus = "Load Failed - Each customer must have a first and last name";
                    }
                }
            }
            // say that the load was successful and send to view
            ViewBag.LoadStatus = "Data Loaded";
            return View();
        }

        [HttpPost]
        public ActionResult ProcessQuery(string LastName, string FirstName, string Attribute)
        {
            // retrieve the storage account from the connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // create the table client
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // create the CloudTable object that represents the "people" table
            CloudTable table = tableClient.GetTableReference("people");

            

            // retrieving one specific customer (both last and first name provided)
            if (LastName != "" && FirstName != "")
            {
                // create a retrieve operation that takes a customer entity
                TableOperation retrieveOperation = TableOperation.Retrieve<PersonEntity>(LastName, FirstName);

                // execute the retrieve operation
                TableResult retrievedResult = table.Execute(retrieveOperation);

                // store the resulting entity as a string
                if (retrievedResult.Result != null)
                {

                    string result = ((PersonEntity)retrievedResult.Result).PartitionKey +
                        " " + ((PersonEntity)retrievedResult.Result).RowKey;
                    if (Attribute == "")
                    {
                        result += " " + ((PersonEntity)retrievedResult.Result).PhoneNumber +
                            " " + ((PersonEntity)retrievedResult.Result).Email +
                            " " + ((PersonEntity)retrievedResult.Result).Gender +
                            " " + ((PersonEntity)retrievedResult.Result).City;
                    }
                    else
                    {
                        if (Attribute == "Email")
                            result += " " + ((PersonEntity)retrievedResult.Result).Email;
                        else if (Attribute == "Gender")
                            result += " " + ((PersonEntity)retrievedResult.Result).Gender;
                        else if (Attribute == "City")
                            result += " " + ((PersonEntity)retrievedResult.Result).City;
                        else if (Attribute == "Phone")
                            result += " " + ((PersonEntity)retrievedResult.Result).PhoneNumber;
                    }
                    
                    ViewBag.Query = result;
                }
                else
              
                    ViewBag.Query = "No matches";
            }

            // retrieving all customers with same last name
            else if (LastName != "")
            {
                // Construct the query operation for all customer entities where partition key is the given last name
                TableQuery<PersonEntity> query = 
                    new TableQuery<PersonEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, LastName));

                // stores result for each customer with given last name
                string result = "";
                bool first = true; 
                foreach (PersonEntity entity in table.ExecuteQuery(query))
                {
                    if (!first)
                        result += " | ";
                    else
                        first = false;
                    result += entity.PartitionKey + " " + entity.RowKey;

                    // no attribute specified 
                    if (Attribute == "")
                    {
                        result += " " + entity.PhoneNumber + " " + entity.Email +
                            " " + entity.Gender + " " + entity.City;
                    }

                    else
                    {
                        if (Attribute == "Email")
                            result += " " + entity.Email;
                        else if (Attribute == "Gender")
                            result += " " + entity.Gender;
                        else if (Attribute == "City")
                            result += " " + entity.City;
                        else if (Attribute == "Phone")
                            result += " " + entity.PhoneNumber;
                    }
                }
                if (result.Length == 0)
                    result = "No matches";
                ViewBag.Query = result; 
            }

            // retrieving all customers with same first name
            else if (FirstName != "")
            {
                // Construct the query operation for all customer entities where partition key is the given first name
                TableQuery<PersonEntity> query =
                    new TableQuery<PersonEntity>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, FirstName));

                // stores result for each customer with given first name
                string result = "";
                bool first = true;
                foreach (PersonEntity entity in table.ExecuteQuery(query))
                {
                    if (!first)
                        result += " | ";
                    else
                        first = false;
                    result += entity.PartitionKey + " " + entity.RowKey;

                    // no attribute specified
                    if (Attribute == "")
                    {
                        result += " " + entity.PhoneNumber + " " + entity.Email +
                            " " + entity.Gender + " " + entity.City;
                    }

                    else
                    {
                        if (Attribute == "Email")
                            result += " " + entity.Email;
                        else if (Attribute == "Gender")
                            result += " " + entity.Gender;
                        else if (Attribute == "City")
                            result += " " + entity.City;
                        else if (Attribute == "Phone")
                            result += " " + entity.PhoneNumber;
                    }
                }
                if (result.Length == 0)
                    result = "No matches";
                ViewBag.Query = result;
            }
            else
            {
                ViewBag.Query = "Error: Must input either first or last name";
            }
            return View(); // return query to view
        }
    }
}
