#!/bin/env groovy

package ua.net.nlp_uk.ubertext.extract

import groovy.transform.Field
import java.time.Instant
import java.time.LocalDate
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.bson.conversions.Bson
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Updates
import com.mongodb.client.result.UpdateResult

import ua.net.nlp_uk.ubertext.MongoService


@Field
Logger logger = LoggerFactory.getLogger(getClass())

@Field
Properties properties = loadProperties()

@Field
def emptyCountMap = [:].withDefault { 0 } 

def collections = Collections.collections

@Field
MongoService service


private Properties loadProperties() {
    Properties properties = new Properties()
    File propertiesFile = new File('config/extract.properties')
    propertiesFile.withInputStream {
        properties.load(it)
    }
    return properties
}


service = new MongoService(properties)

try {
    
    collections.each { String collectionName, List sources ->
        logger.info "Processing collection: ${collectionName}"
    
        def collection = service.collection(collectionName)
    
        sources.each { source ->
            logger.info "Processing source: ${source}"

            File folder = Collections.getFolder(collectionName, source)
        
            def idFilter = Filters.regex("source_info.slug", ".*${source}.*")
            
            // source == 'news_lugansk_ua' 
            Bson filter = Collections.filters  
                    ? Filters.and(
                        idFilter, Collections.filters
                    )
                    : idFilter 

            def cnts = ['total': 0, 'good': 0, 'exists': 0, 'years': new LinkedHashSet()]
            collection.find(filter) //.limit(1)
                    .each {
                        process(collection, it, cnts, source, folder)
                    }
            println ""
            def yrs = cnts['years'].toSorted()
            logger.info "Files: total ${cnts['total']}, created: ${cnts['good']}, exists: ${cnts['exists']}, years collected: ${yrs}"
        }
    }
}
finally {
    if( emptyCountMap ) {
        def str = emptyCountMap.collect{ k,v -> "$k: $v" }.join("\n")
        logger.warn "empty: $str"
    }
    service.close()
    logger.info "Done processing"
}



def process(MongoCollection collection, record, cnts, String source, File folder) {

    int curr = cnts['total'] + emptyCountMap[source]
    if( curr > 0 && curr % 200 == 0 )
        println "$curr"
    
    String text = record.text
    if( ! text ) {
//        logger.warn "Empty text for ${record._id}"
        print "."
        emptyCountMap[source] += 1
        
//        new File(folder, "empty.lst") << record._id << "\n"
        
        return
    }

    File file = new File(folder, "${record._id}.txt")
    
    cnts['total']++
    if( file.isFile() && file.size() > 0 ) {
//        logger.info "${record._id}.txt exists - skipping"
        print "-"
        cnts['exists']++
        return
    }
        
    //logger.info "Writing ${record._id}.txt"
    print "+"
    
    file.text = ''
    file << "${record.title}\n"
    file << "${record.author}\n"
    file << "${record.date_of_publish}\n"
    file << "---------\n"
    file << record.text
    file << "\n"
    cnts['good']++
    if( record.date_of_publish ) {
        int year = record.date_of_publish.getYear()
        if( year < 1900 ) year += 1900
        cnts['years'] << year
    }
    else {
        cnts['years'] << 'unknown'
    }
}

