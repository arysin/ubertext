package ua.net.nlp_uk.ubertext

//@Grab(group='org.mongodb', module='mongodb-driver', version='3.12.+')

import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase

class MongoService {
    private MongoClient mongoClient
    private Properties properties
    
    MongoService(Properties properties) {
        this.properties = properties
    }
    
    public MongoClient client() {
        
        mongoClient = mongoClient ?: new MongoClient(properties['db.host'], properties['db.port'] as int)

        return mongoClient
    }

    public MongoCollection collection(collectionName) {
        MongoDatabase db = client().getDatabase(properties['db.name'])

        return db.getCollection(collectionName)
    }
    
    def close() {
        if( mongoClient != null ) {
            mongoClient.close()
        }
    }
}
