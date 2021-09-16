#!/bin/env groovy

package ua.net.nlp_uk.ubertext

import java.time.Instant

import org.bson.conversions.Bson
import org.nlp_uk.other.CleanText
import org.nlp_uk.other.CleanText.CleanOptions
import org.nlp_uk.tools.LemmatizeText
import org.nlp_uk.tools.TokenizeText
import org.nlp_uk.tools.LemmatizeText.Analyzed
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Updates
import com.mongodb.client.result.UpdateResult

import groovy.transform.Field


@Field
Logger logger = LoggerFactory.getLogger(getClass())

@Field
Properties properties = loadProperties()

def collections = properties['db.collections'].split(/[, ]+/)

@Field
MongoService service
@Field
CleanText cleanText = new CleanText(new CleanOptions())
@Field
TokenizeText tokenizeText = new TokenizeText(new TokenizeText.TokenizeOptions())
@Field
LemmatizeText lemmatizeText = new LemmatizeText(new LemmatizeText.LemmatizeOptions(firstLemmaOnly: true))
@Field
List<String> actions = properties['actions'].split(/[, ]+/)
@Field
boolean force = properties['force'] as Boolean
@Field
boolean storeFiles = properties['store.files'] as Boolean

@Field
Map processedStats = [:].withDefault { 0 }

private Properties loadProperties() {
    Properties properties = new Properties()
    File propertiesFile = new File('config/config.properties')
    propertiesFile.withInputStream {
        properties.load(it)
    }
    return properties
}

if( storeFiles ) {
    new File(".files").mkdirs()
}

service = new MongoService(properties)

try {
    def ids = properties['ids'] ? properties['ids'].split(/[ ,]+/) : []
    def limit = -1
    if( ids ) {
        logger.info "IDs to process: $ids"
    }
    else if( 'limit' in properties ) {
        limit = properties['limit'] as int
        logger.info "Record limit is set to: $limit"
    } 

    collections.each { String collectionName ->
        logger.info "Processing collection: ${collectionName}"
        processedStats.clear()
        long tm1 = System.currentTimeMillis()
        int records = 0

        def collection = service.collection(collectionName)

        if( ids ) {
            ids.each { id ->
                Bson idFilter = Filters.eq("_id", id)

                collection.find(idFilter).limit(1).each {
                    process(collection, it)
                    records++
                }
            }
        }
        else if( limit > 0 ) {
            collection.find()
                .limit(limit)
                .each {
                    process(collection, it)
                    records++
                }
        }
        else {
            def filter = Filters.and(
                Filters.exists("nlp", false),
                Filters.ne("text", "")
                )

//            logger.info "Records to process: " + collection.find(filter).estimatedDocumentCount()
//            logger.info "Records to process: " + collection.countDocuments(filter)

            int batchSize = 300
            for(int ii=0; ; ii++) {
                int prevRecords = records
                logger.info "Processing batch $ii of size $batchSize..."

                collection.find(filter)
                  .limit(batchSize)
                  .each {
                    process(collection, it)
                    records++
                    logger.debug "Done with record: $records"

                    if( records % 100 == 0 ) {
                        long tm2 = System.currentTimeMillis()
                        long time = tm2-tm1
                        double speed = (double)records*1000000/time/1000.0
                        logger.info "Intermediate stats: time: $time, records: $records, $speed records/s"
                    }
                }
                if( records - prevRecords < batchSize ) {
                    logger.info "Processed only ${records-prevRecords}, looks like we're done"
                    break; // out of cursor loop
                }
           }
        }

        long tm2 = System.currentTimeMillis()
        long time = tm2-tm1
        double speed = (double)records*1000000/time/1000.0
        logger.info "Done with collection: $collectionName: $processedStats, time: $time, records: $records, $speed records/s"
    }
}
catch(Exception e) {
    logger.error "Exception", e
}
finally {
    service.close()
    logger.info "Done processing"
}



def process(MongoCollection collection, record) {


    String text = record.text    
    if( ! text ) {
        logger.warn "Empty text for ${record._id}"
        return
    }

    if( storeFiles ) {
        new File(".files", "${record._id}_text.txt").text = record.text
    }
    
    if( text.size() > 3000000 ) {
        logger.warn "Text too big for ${record._id}, size: ${record.text.size()}"
        return
    }
    
    Bson idFilter = Filters.eq("_id", record._id)

    if( "clean" in actions ) {
        clean(collection, record)
    }

    if( "tokenize" in actions ) {
        tokenize(collection, record)
    }

    if( "lemmatize" in actions ) {
        lemmatize(collection, record)
    }

}


// do nlp only if not done before or text is cleaned again
boolean needNlp(record) {
    force || ! record.nlp?.time || \
        (record.clean?.text && record.clean?.time \
            && record.clean.time.after(record.nlp.time) )
}

void tokenize(MongoCollection collection, record) {
    if( needNlp(record) ) {
        
        String text = record.clean?.text ?: record.text 

        logger.info "Tokenizing ${record._id}, size: ${text.size()}"

        def tokens = tokenizeText.splitWords(text, true)
        def titleTokens = record.title ? tokenizeText.splitWords(record.title, true) : ""

        if( storeFiles && tokens ) {
            new File(".files", "${record._id}_tokens.txt").text = tokens
        }

        Bson updateOperation = Updates.combine(
            Updates.set("nlp.text.tokens", tokens),
            Updates.set("nlp.title.tokens", titleTokens),
            Updates.set("nlp.time", Instant.now()),
            // remove old fields
            Updates.unset("nlp.tokens"),
            // TEMPORARY: REMOVE: clean fields
            Updates.unset("nlp.text.lemmas"),
            Updates.unset("nlp.title.lemmas")
        )

        Bson idFilter = Filters.eq("_id", record._id)
        UpdateResult updateResult = collection.updateOne(idFilter, updateOperation)

        logger.debug "Updated: {}", updateResult
        processedStats['tokenize']++
    }
    else {
        logger.info "Skipped tokenize for ${record._id}"
        processedStats['tokenize.skip']++
    }
}

void lemmatize(MongoCollection collection, record) {
    if( needNlp(record) ) {
        String text = record.clean?.text ?: record.text 

        logger.info "Lemmatizing ${record._id}, size: ${text.size()}"
        
        Analyzed lemmas = lemmatizeText.analyzeText(text)

        if( storeFiles && lemmas ) {
            new File(".files", "${record._id}_lemmas.txt").text = lemmas
        }

        List updates = []
        
        updates << Updates.set("nlp.text.lemmas", lemmas.tagged)
        updates << Updates.set("nlp.time", Instant.now())
        // remove old fields
        updates << Updates.unset("nlp.lemmas")

        if( record.title ) {
            Analyzed titleLemmas = lemmatizeText.analyzeText(record.title)
            updates << Updates.set("nlp.title.lemmas", titleLemmas.tagged)
        }

        Bson idFilter = Filters.eq("_id", record._id)
        UpdateResult updateResult = collection.updateOne(idFilter, Updates.combine(updates))

        logger.debug "Updated {}", updateResult
        processedStats['lemmatize']++
    }
    else {
        logger.info "Skipped lemmatize for ${record._id}"
        processedStats['lemmatize.skip']++
    }
}

String clean(MongoCollection collection, record) {
    String text = record.text

    if( force || ! record.clean?.time ) {
        logger.info "Cleaning ${record._id}, size: ${text.size()}"

        String cleaned = cleanText.cleanUp(text, new File("/dev/null"), new CleanOptions())
        // normalize for ubertext
        cleaned = cleaned.replaceAll(/[\u2019\u02bc]/, "'")

        if( storeFiles && cleaned && cleaned != text ) {
            new File(".files", "${record._id}_clean.txt").text = cleaned
        }

        float ukRate, ruRate
        (ukRate, ruRate) = cleanText.evalChunk(cleaned)
        
        if( ukRate < ruRate ) {
            ruRate = Math.round(ruRate * 100)/100
        }
        
        List updates = []
        
        if( text != cleaned ) {
            updates << Updates.set("clean.text", cleaned)
            if( record.clean == null ) {
                record.clean = [:]
            }
            record.clean.text = cleaned
        }
        
        if( record.title ) {
            String titleCleaned = cleanText.cleanUp(record.title, new File("/dev/null"), new CleanOptions())
            if( record.title != titleCleaned ) {
                updates << Updates.set("clean.title", titleCleaned)
                if( record.clean == null ) {
                    record.clean = [:]
                }
                record.clean.title = titleCleaned
            }
        }

        
        logger.info "uk: $ukRate / ru: $ruRate"
//        if( ukRate < ruRate ) {
            updates << Updates.set("clean.ru_rate", ruRate)
            updates << Updates.set("clean.uk_rate", ukRate)
//        }
        updates << Updates.set("clean.time", Instant.now())
            
        if( updates ) {
            Bson idFilter = Filters.eq("_id", record._id)
            Bson updateOperation = Updates.combine(updates)
            UpdateResult updateResult = collection.updateOne(idFilter, updateOperation)

            logger.debug "Updated: {}", updateResult
            
            processedStats['clean']++
        }
    }
    else {
        logger.info "Skipped clean for ${record._id}"
        processedStats['clean.skip']++
    }
}
