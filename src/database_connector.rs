use async_trait::async_trait;
use bson::Document;
use mongodb::Client;
use mongodb::options::{ClientOptions, ServerApi, ServerApiVersion};
use serde::de::DeserializeOwned;


#[async_trait]
pub(crate) trait DocumentDatabaseConnector {
    async fn init(db_uri: String, db_name: String) -> Self;
    async fn find_one_document<T>(
        &self,
        collection_name: String,
        query: impl Into<Option<Document>> + Send,
    ) -> Option<T>
        where
            T: DeserializeOwned + Unpin + Send + Sync;
}
#[derive(Debug, Clone)]
pub(crate) struct MongoDBClient {
    client: Client,
    db_name: String,
}

#[async_trait]
impl DocumentDatabaseConnector for MongoDBClient {
    async fn init(uri: String, name: String) -> Self {
        let mut client_options = ClientOptions::parse(&uri)
            .await
            .expect(format!("Cannot connect to the database on {}", uri).as_str());
        let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
        client_options.server_api = Some(server_api);
        let db_client =
            Client::with_options(client_options).expect("Cannot create a database client");
        MongoDBClient {
            client: db_client,
            db_name: name,
        }
    }

    async fn find_one_document<T>(&self, collection_name: String, query: impl Into<Option<Document>> + Send) -> Option<T> where T: DeserializeOwned + Unpin + Send + Sync {
        let all_collections = self
            .client
            .database(self.db_name.as_str())
            .collection::<T>(&collection_name);
        let result = all_collections.find_one(query, None).await;
        return result.unwrap_or_else(|err| {
            println!("{}", err);
            None
        });
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use super::*;
    fn get_db_connection_uri(&port: &u16) -> String {
        format!("mongodb://{}:{}", "0.0.0.0", port)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn find_a_document_by_version_when_one_exists() {
        // Arrange
        let uri = get_db_connection_uri(&27017);

        let db = MongoDBClient::init(uri, "users".to_string()).await;
        let collection = "profiles".to_string();
        let expected_name = "oliver.bannister".to_string();
        let expected_age = 24;
        let expected_location = "Toronto".to_string();

        // Act
        let document = db.find_one_document::<Document>(collection, doc! { "name": expected_name.clone(), "age": expected_age, "location": expected_location.clone() }).await.unwrap();
        let name = document.get_str("name").unwrap();
        let age = document.get_i32("age").unwrap();
        let location = document.get_str("location").unwrap();

        // Assert
        assert_eq!(name, expected_name);
        assert_eq!(age, expected_age);
        assert_eq!(location, expected_location);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn find_another_document_by_version_when_one_exists() {
        // Arrange
        let uri = get_db_connection_uri(&27017);

        let db = MongoDBClient::init(uri, "users".to_string()).await;
        let collection = "profiles".to_string();
        let expected_name = "john.doe".to_string();
        let expected_age = 35;
        let expected_location = "London".to_string();

        // Act
        let document = db.find_one_document::<Document>(collection, doc! { "name": expected_name.clone(), "age": expected_age, "location": expected_location.clone() }).await.unwrap();
        let name = document.get_str("name").unwrap();
        let age = document.get_i32("age").unwrap();
        let location = document.get_str("location").unwrap();

        // Assert
        assert_eq!(name, expected_name);
        assert_eq!(age, expected_age);
        assert_eq!(location, expected_location);
    }

}