use azure_storage_datalake::prelude::*;

use std::collections::HashMap;
use serde_json::Value;
use serde::Serialize;
use warp::Filter;
use azure_storage::StorageCredentials;
use azure_core::auth::Secret;
use azure_security_keyvault::KeyvaultClient;

use std::env;
use tokio::fs::{self, OpenOptions};
use tokio::io;
use std::path::{Path, PathBuf};
use std::net::Ipv4Addr;
use tokio::process::Command;
use tokio::io::{AsyncWriteExt};

fn extract_account_key(connection_string: &str) -> Result<String, io::Error> {
    // Split the connection string by semicolons
    let parts: Vec<&str> = connection_string.split(';').collect();
    // Find the part that starts with "AccountKey="
    for part in parts {
        // if part.starts_with("AccountKey=") {
        //     // Extract and return the AccountKey value
        //     return Ok(part["AccountKey=".len()..].to_string());
        // }

        // Extract and return the AccountKey value
        if let Some(key) = part.strip_prefix("AccountKey=") {
            return Ok(key.to_string())
        }
    }
    Err(io::Error::new(io::ErrorKind::Other, "No AccountKey in environment variable"))
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // Test the environment

    let build_time = env!("BUILD_TIME");
    println!("Build time: {}", build_time);

    // Get the system temporary directory.
    let mut log_path: PathBuf = env::temp_dir();
    // Append the log file name to the temporary directory path.
    log_path.push("azure-rust.log");
    // Run the `date` command to get the current date and time.
    let output = Command::new("date")
        .output()
        .await?;
    let date_output = String::from_utf8_lossy(&output.stdout) ;
    // Open the log file in append mode, create it if it does not exist.
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .await?;

    // Write the date and time to the file, followed by a newline.
    file.write_all(date_output.as_bytes()).await?;
    file.write_all(b"\n").await?;

    // Check if running locally or in the cloud. This ENV variable is only set in the cloud
    let running_in_cloud = if let Ok(website_instance_id) = env::var("AzureWebJobsStorage") {
        println!("Running in Azure Functions. AzureWebJobsStorage={}", website_instance_id);
        // Touch a file in the lakehouse, directly
        // let file_path = "/lakehouse/default/test_if_persists";
        // if Path::new(file_path).exists() {
        //     OpenOptions::new().write(true).open(file_path).await?;
        // } else {
        //     fs::File::create(file_path).await?;
        // }

        // List the $HOME directory
        let home_dir = env::var("HOME").map_err(|e| {
            eprintln!("Failed to get HOME directory: {}", e);
            io::Error::new(io::ErrorKind::Other, e)
        })?;

        println!("Files in home directory {}:", home_dir);
        let mut paths = fs::read_dir(home_dir).await?;

        while let Some(entry) = paths.next_entry().await? {
            println!("{}", entry.path().display());
        }
        true
    } else {
        println!("Running locally.");
        false
    };

    // Set up Data Lake connection
    let account = "stasdpdev";

    // Get the key to the resource group
    let key = if running_in_cloud {
        // Azure uses the credentials provided by the azure_identity::create_credential()
        // function to authenticate your application. This function typically creates a
        // credential object that can automatically use various authentication methods, such as:
        // 1. Environment Variables: If you have environment variables set up for your Azure credentials
        // (like AZURE_CLIENT_ID, AZURE_TENANT_ID, and AZURE_CLIENT_SECRET), the create_credential()
        // function will use these.
        // 2. Managed Identity: If your application is running in an Azure environment that supports
        // Managed Identity (like an Azure VM, App Service, or Azure Kubernetes Service), the create_credential()
        // function can use the managed identity assigned to the resource.
        // 3. Azure CLI: If you are logged into Azure CLI on your local machine, the create_credential() function
        // can use the credentials from your Azure CLI session.
        // 4. Visual Studio Code: If you are logged into Azure through Visual Studio Code,
        // the create_credential() function can use those credentials.

        // In order to access the key vault in the cloud, the Function App must have Secrets Reader access
        // In order to access the key vault in locally, the user (Managed Identity) logged into Azure Cli
        // must have Secrets Reader access

        // Extract the key from a Key Vault
        let vault_url = "https://kv-rust-poc.vault.azure.net/";
        let secret_name = "AZURE-STASDPDEV-KEY";

        let credential = azure_identity::create_credential().unwrap();
        let client = KeyvaultClient::new(vault_url, credential,).unwrap().secret_client();
        // Retrieve the secret from Key Vault
        let secret_response = client.get(secret_name).await.map_err(|e| {
            eprintln!("Failed to get secret {}: {}", secret_name, e);
            io::Error::new(io::ErrorKind::Other, e)
        })?;
        let key = secret_response.value;
        println!("Fetched secret {} from Key Vault", secret_name);
        key


        // let env_var = "AzureWebJobsStorage";
        // let storage_connection_string = env::var(env_var).map_err(|e| {
        //     eprintln!("Failed to get environment variable {}: {}", env_var, e);
        //     io::Error::new(io::ErrorKind::Other, e)
        // })?;
        // extract_account_key(&storage_connection_string)?
    } else {
        // It isn't really necessary to get the key from the env since the Key Vault is accessible locally.
        // However we do this as PoC.
        let env_var = "AZURE_STASDPDEV_KEY";
        env::var(env_var).map_err(|e| {
            eprintln!("Failed to get  environment variable {}: {}", env_var, e);
            io::Error::new(io::ErrorKind::Other, e)
        })?
    };

    let storage_credentials = StorageCredentials::access_key(account, Secret::new(key));
    let client = DataLakeClient::new(account, storage_credentials);

    let file_system = "torsteinlake";
    let file_path = "svgparking/parking.json";
    let file_client = client.file_system_client(file_system).get_file_client(file_path);

    // Test data lake connection
    match file_client.read().await {
        Ok(response) => {
            let mut body = response.data;
            // Remove BOM if present
            if body.starts_with(b"\xef\xbb\xbf") {
                body = body[3..].to_vec().into();
            }
            match serde_json::from_slice::<Value>(&body) {
                Ok(json_data) => {
                    println!("Successfully connected to Data Lake. JSON data: {}", json_data);
                },
                Err(e) => {
                    eprintln!("Failed to parse JSON: {}", e);
                }
            }
        },
        Err(e) => {
            eprintln!("Failed to read file: {}", e);
        }
    }

    // // Create a closure that captures the client
    // let handle_request_with_client = move || {
    //     let client = client.clone();
    //     async move { handle_request(client).await }
    // };

    // Craft a static response
    let mut static_response = HashMap::new();
    static_response.insert("running_in_cloud", format!("{}", running_in_cloud));
    static_response.insert("build_time", build_time.to_string());

    let handle_request_with_known_runtime = move || {
        let static_response_  = static_response.clone();
        async move { handle_request(&static_response_).await }
    };

    // Define routes
    let route = warp::get()
        .and(warp::path("api"))
        .and(warp::path("RustPoc"))
        .and_then(handle_request_with_known_runtime);

    let port_key = "FUNCTIONS_CUSTOMHANDLER_PORT";
    let default_port = 8080;
    // Fetch port from env or set to default if not environment var is not set
    let port: u16 = match env::var(port_key) {
        Ok(val) => val.parse().expect(&format!("Custom Handler '{}' port is not a number!", port_key)),
        Err(_) => default_port,
    };
    println!("Running port {}", port);

    // Start API
    warp::serve(route).run((Ipv4Addr::LOCALHOST, port)).await;

    Ok(())
}

async fn handle_request<K: Serialize, V: Serialize>(static_response: &HashMap<K, V>) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(warp::reply::json(static_response))
}
