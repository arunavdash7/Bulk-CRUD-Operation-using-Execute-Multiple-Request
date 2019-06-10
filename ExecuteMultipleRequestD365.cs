using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Security;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace ExecuteMultipleRequestD365
{
    class ExecuteMultipleRequestD365
    {
       //List<string>  AccountId = new List<string>();
        private static IOrganizationService _orgService;
        private static IOrganizationService _orgServiceTarget;
        //ExecuteMultipleRequest  multipleRequest = null;
        //Entity Account = new Entity("account");
        public static void Main(string[] args)
        {
            ExecuteMultipleRequestD365 obj = new ExecuteMultipleRequestD365();
            ExecuteMultipleRequest multipleRequest = null;

            try
            {
                //Get CRM Configuration Details 
                String connectionString = GetServiceConfiguration();
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                CrmServiceClient conn = new CrmServiceClient(connectionString);
                // Cast the proxy client to the IOrganizationService interface.
                _orgService = (IOrganizationService)conn.OrganizationWebProxyClient != null ? (IOrganizationService)conn.OrganizationWebProxyClient : (IOrganizationService)conn.OrganizationServiceProxy;
                


                var fetchXML = @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                  <entity name='account'>
                                    <attribute name='name' />
                                    <attribute name='telephone1' />
                                    <attribute name='accountid' />
                                    <order attribute='name' descending='false' />
                                  </entity>
                                </fetch>";

                  multipleRequest = new ExecuteMultipleRequest()
                {
                    // Assign settings that define execution behavior: continue on error, return responses.
                    Settings = new ExecuteMultipleSettings()
                    {
                        ContinueOnError = false,
                        ReturnResponses = true
                    },
                    // Create an empty organization request collection.
                    Requests = new OrganizationRequestCollection()
                };

                // get all the records > 5000
                var totalRecords = GetTotalRecordsFetchXML(_orgService, fetchXML);
                
                // split the lst of entity to child list
                // specify the size of the batch i.e. 500 here
                var lstlstEntity = SplitList(totalRecords, 500);
                //connect to target instance///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

                String connectionStringTarget = GetServiceConfiguration();
                CrmServiceClient connTarget = new CrmServiceClient(connectionStringTarget);
                _orgServiceTarget = (IOrganizationService)connTarget.OrganizationWebProxyClient != null ? (IOrganizationService)connTarget.OrganizationWebProxyClient : (IOrganizationService)connTarget.OrganizationServiceProxy;
                //var client2 = new CrmServiceClient("RequireNewInstance=True; Url = https://lti1234567.crm.dynamics.com; Username = arunav@lti1234567.onmicrosoft.com; Password = Qwerty@123;  AuthType = Office365");
                //_orgServiceTarget = (IOrganizationService)client2.OrganizationWebProxyClient != null ? (IOrganizationService)client2.OrganizationWebProxyClient : (IOrganizationService)client2.OrganizationServiceProxy;
                Console.WriteLine("Time started" + DateTime.Now);
                foreach (var lstEntity in lstlstEntity)
                {
                    //BulkUpdate(_orgService, lstEntity);
                    
                    BulkCreate(_orgServiceTarget, lstEntity);
                     //BulkDelete(_orgServiceTarget, lstEntity);
                }
                Console.WriteLine("Time End" + DateTime.Now);
                // loop through each of the list and create the request and add it to Execute Multiple Request
                //foreach (var lstEntity in lstlstEntity)
                //{
                //    multipleRequest.Requests.Clear();
                //    foreach (var entity in lstEntity)
                //    {
                //        UpdateRequest createRequest = new UpdateRequest { Target = entity };
                //        // add the field to be updated
                //        entity.Attributes["sab_referencenumber"] = "1000";
                //        multipleRequest.Requests.Add(createRequest);
                //    }

                //    ExecuteMultipleResponse multipleResponse = (ExecuteMultipleResponse)_orgService.Execute(multipleRequest);
                //}
            }
            catch (FaultException<OrganizationServiceFault> fault)
            {
                // Check if the maximum batch size has been exceeded. The maximum batch size is only included in the fault if it
                // the input request collection count exceeds the maximum batch size.
                if (fault.Detail.ErrorDetails.Contains("MaxBatchSize"))
                {
                    int maxBatchSize = Convert.ToInt32(fault.Detail.ErrorDetails["MaxBatchSize"]);
                    if (maxBatchSize < multipleRequest.Requests.Count)
                    {
                        // Here you could reduce the size of your request collection and re-submit the ExecuteMultiple request.
                        // For this sample, that only issues a few requests per batch, we will just print out some info. However,
                        // this code will never be executed because the default max batch size is 1000.
                        Console.WriteLine("The input request collection contains %0 requests, which exceeds the maximum allowed (%1)",
                            multipleRequest.Requests.Count, maxBatchSize);
                    }
                }
                // Re-throw so Main() can process the fault.
                throw;
            }





        }
        private static String GetServiceConfiguration()
        {
            // Get available connection strings from app.config.
            int count = ConfigurationManager.ConnectionStrings.Count;

            // Create a filter list of connection strings so that we have a list of valid
            // connection strings for Microsoft Dynamics CRM only.
            List<KeyValuePair<String, String>> filteredConnectionStrings =
                new List<KeyValuePair<String, String>>();
            for (int a = 0; a < count; a++)
            {
                if (isValidConnectionString(ConfigurationManager.ConnectionStrings[a].ConnectionString))
                    filteredConnectionStrings.Add
                        (new KeyValuePair<string, string>
                            (ConfigurationManager.ConnectionStrings[a].Name,
                            ConfigurationManager.ConnectionStrings[a].ConnectionString));
            }

            // No valid connections strings found. Write out and error message.
            if (filteredConnectionStrings.Count == 0)
            {
                Console.WriteLine("An app.config file containing at least one valid Microsoft Dynamics CRM " +
                    "connection string configuration must exist in the run-time folder.");
                Console.WriteLine("\nThere are several commented out example connection strings in " +
                    "the provided app.config file. Uncomment one of them and modify the string according " +
                    "to your Microsoft Dynamics CRM installation. Then re-run the sample.");
                return null;
            }

            // If one valid connection string is found, use that.
            if (filteredConnectionStrings.Count == 1)
            {
                return filteredConnectionStrings[0].Value;
            }

            // If more than one valid connection string is found, let the user decide which to use.
            if (filteredConnectionStrings.Count > 1)
            {
                Console.WriteLine("The following connections are available:");
                Console.WriteLine("------------------------------------------------");

                for (int i = 0; i < filteredConnectionStrings.Count; i++)
                {
                    Console.Write("\n({0}) {1}\t",
                    i + 1, filteredConnectionStrings[i].Key);
                }

                Console.WriteLine();

                Console.Write("\nType the number of the connection to use (1-{0}) [{0}] : ",
                    filteredConnectionStrings.Count);
                String input = Console.ReadLine();
                int configNumber;
                if (input == String.Empty) input = filteredConnectionStrings.Count.ToString();
                if (!Int32.TryParse(input, out configNumber) || configNumber > count ||
                    configNumber == 0)
                {
                    Console.WriteLine("Option not valid.");
                    return null;
                }

                return filteredConnectionStrings[configNumber - 1].Value;

            }
            return null;

        }

        /// <summary>
        /// Verifies if a connection string is valid for Microsoft Dynamics CRM.
        /// </summary>
        /// <returns>True for a valid string, otherwise False.</returns>
        private static Boolean isValidConnectionString(String connectionString)
        {
            // At a minimum, a connection string must contain one of these arguments.
            if (connectionString.Contains("Url=") ||
                connectionString.Contains("Server=") ||
                connectionString.Contains("ServiceUri="))
                return true;

            return false;
        }

/// <summary>
/// Function to split the list into child list
/// </summary>
/// <param name="locations"></param>
/// <param name="nSize"></param>
/// <returns></returns>
public static List<List<Entity>> SplitList(List<Entity> locations, int nSize)
{
    var list = new List<List<Entity>>();
    for (int i = 0; i < locations.Count; i += nSize)
    {
        list.Add(locations.GetRange(i, Math.Min(nSize, locations.Count - i)));
    }
    return list;
}



private static List<Entity> GetTotalRecordsFetchXML(IOrganizationService orgProxy, string fetchXML)
{
    XDocument xDocument = XDocument.Parse(fetchXML);
    var fetchXmlEntity = xDocument.Root.Element("entity").ToString();

    EntityCollection entityColl = new EntityCollection();
    List<Entity> lstEntity = new List<Entity>();
    int page = 1;
    do
    {
        entityColl = orgProxy.RetrieveMultiple(new FetchExpression(
        string.Format("<fetch version='1.0' page='{1}' paging-cookie='{0}'>" + fetchXmlEntity + "</fetch>",
        SecurityElement.Escape(entityColl.PagingCookie), page++)));

        lstEntity.AddRange(entityColl.Entities);
    }
    while (entityColl.MoreRecords);

    return lstEntity;
}

        public static void BulkCreate(IOrganizationService service, List<Entity> entities)
        {
            // Create an ExecuteMultipleRequest object.
            var multipleRequest = new ExecuteMultipleRequest()
            {
                // Assign settings that define execution behavior: continue on error, return responses. 
                Settings = new ExecuteMultipleSettings()
                {
                    ContinueOnError = false,
                    ReturnResponses = true
                },
                // Create an empty organization request collection.
                Requests = new OrganizationRequestCollection()
            };

            // Add a CreateRequest for each entity to the request collection.
            foreach (var entity in entities)
            {
                CreateRequest createRequest = new CreateRequest { Target = entity };
                multipleRequest.Requests.Add(createRequest);
            }

            // Execute all the requests in the request collection using a single web method call.
            ExecuteMultipleResponse multipleResponse = (ExecuteMultipleResponse)service.Execute(multipleRequest);
            Console.WriteLine("Records created in Target Organisation");
            foreach (var responseItem in multipleResponse.Responses)
            {
                // A valid response.
                if (responseItem.Response != null)
                  DisplayResponse(multipleRequest.Requests[responseItem.RequestIndex], responseItem.Response);

                // An error has occurred.
                else if (responseItem.Fault != null)
                    DisplayFault(multipleRequest.Requests[responseItem.RequestIndex], responseItem.RequestIndex, responseItem.Fault);

            }

        }
        /// <summary>
        /// Display the fault that resulted from processing an organization message request.
        /// </summary>
        /// <param name="organizationRequest">The organization message request.</param>
        /// <param name="count">nth request number from ExecuteMultiple request</param>
        /// <param name="organizationServiceFault">A WCF fault.</param>
        private static void DisplayFault(OrganizationRequest organizationRequest, int count,
            OrganizationServiceFault organizationServiceFault)
        {
            Console.WriteLine("A fault occurred when processing {1} request, at index {0} in the request collection with a fault message: {2}", count + 1,
                organizationRequest.RequestName,
                organizationServiceFault.Message);
        }
        /// <summary>
        /// Display the response of an organization message request.
        /// </summary>
        /// <param name="organizationRequest">The organization message request.</param>
        /// <param name="organizationResponse">The organization message response.</param>
        private static void DisplayResponse(OrganizationRequest organizationRequest, OrganizationResponse organizationResponse)
        {
            //List<string> AccountId = new List<string>();
            //int recordcount = 0;
            //string id = organizationResponse.Results["accountid"].ToString();
            //AccountId.Add(id);
            Console.WriteLine("organisation response"+ organizationResponse.Results.Values);
            //int increment = ++recordcount;
            //Console.WriteLine("recordcount" +increment);
        }

        /// <summary>
        /// Call this method for bulk update
        /// </summary>
        /// <param name="service">Org Service</param>
        /// <param name="entities">Collection of entities to Update</param>
        public static void BulkUpdate(IOrganizationService service, List<Entity> entities)
        {
            
            // Create an ExecuteMultipleRequest object.
             var multipleRequest = new ExecuteMultipleRequest()
            {
                // Assign settings that define execution behavior: continue on error, return responses. 
                Settings = new ExecuteMultipleSettings()
                {
                    ContinueOnError = false,
                    ReturnResponses = true
                },
                // Create an empty organization request collection.
                Requests = new OrganizationRequestCollection()
            };

            // Add a UpdateRequest for each entity to the request collection.
            foreach (var entity in entities)
            {
                UpdateRequest updateRequest = new UpdateRequest { Target = entity };
                entity.Attributes["numberofemployees"] = 10;
                multipleRequest.Requests.Add(updateRequest);
            }

            // Execute all the requests in the request collection using a single web method call.
            ExecuteMultipleResponse multipleResponse = (ExecuteMultipleResponse)service.Execute(multipleRequest);
            // Execute all the requests in the request collection using a single web method call.
            //ExecuteMultipleResponse multipleResponse = (ExecuteMultipleResponse)service.Execute(multipleRequest);
            foreach (var responseItem in multipleResponse.Responses)
            {
                // A valid response.
                if (responseItem.Response != null)
                    DisplayResponse(multipleRequest.Requests[responseItem.RequestIndex], responseItem.Response);

                // An error has occurred.
                else if (responseItem.Fault != null)
                    DisplayFault(multipleRequest.Requests[responseItem.RequestIndex], responseItem.RequestIndex, responseItem.Fault);

            }
        }

        /// <summary>
        /// Call this method for bulk delete
        /// </summary>
        /// <param name="service">Org Service</param>
        /// <param name="entityReferences">Collection of EntityReferences to Delete</param>
        public static void BulkDelete(IOrganizationService service, List<Entity> GetCollectionOfEntitiesToDelete)
        {
            // Create an ExecuteMultipleRequest object.
            var multipleRequest = new ExecuteMultipleRequest()
            {
                // Assign settings that define execution behavior: continue on error, return responses. 
                Settings = new ExecuteMultipleSettings()
                {
                    ContinueOnError = false,
                    ReturnResponses = true
                },
                // Create an empty organization request collection.
                Requests = new OrganizationRequestCollection()
            };

            // Add a DeleteRequest for each entity to the request collection.
            foreach (var entityRef in GetCollectionOfEntitiesToDelete)
            {
                DeleteRequest deleteRequest = new DeleteRequest { Target = entityRef.ToEntityReference()};
                multipleRequest.Requests.Add(deleteRequest);
            }
            
            // Execute all the requests in the request collection using a single web method call.
            ExecuteMultipleResponse multipleResponse = (ExecuteMultipleResponse)service.Execute(multipleRequest);

            // There should be no responses unless there was an error. Only the first error 
            // should be returned. That is the behavior defined in the settings.
            if (multipleResponse.Responses.Count > 0)
            {
                foreach (var responseItem in multipleResponse.Responses)
                {
                    if (responseItem.Fault != null)
                        DisplayFault(multipleRequest.Requests[responseItem.RequestIndex], responseItem.RequestIndex, responseItem.Fault);

                }
            }
            else
            {
                Console.WriteLine("All account records have been deleted successfully.");
                DateTime startTime = DateTime.Now;
                Console.WriteLine("Started Execution" + startTime);
                DateTime endTime = DateTime.Now;
                Console.WriteLine("Finished Execution" + endTime);
                TimeSpan span = endTime.Subtract(startTime);
                Console.WriteLine("Time Difference (seconds): " + span.Seconds);
                Console.WriteLine("Time Difference (minutes): " + span.Minutes);
                Console.WriteLine("Time Difference (hours): " + span.Hours);
                Console.WriteLine("Time Difference (days): " + span.Days);
            }
        }
    }
}
