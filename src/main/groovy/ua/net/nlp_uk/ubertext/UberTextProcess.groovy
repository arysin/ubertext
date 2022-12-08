#!/bin/env groovy

package ua.net.nlp_uk.ubertext

import java.time.Instant
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.Future
import java.util.concurrent.FutureTask

import org.bson.conversions.Bson
import ua.net.nlp.other.CleanText
import ua.net.nlp.other.CleanText.CleanOptions
import ua.net.nlp.tools.tag.TagTextCore
import ua.net.nlp.tools.tag.TagOptions
import ua.net.nlp.tools.tag.TagTextCore.TTR
import ua.net.nlp.tools.tag.TagTextCore.TagResult
import ua.net.nlp.tools.TokenizeText
import ua.net.nlp.tools.tag.TagStats
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Updates
import com.mongodb.client.result.UpdateResult

import groovy.transform.Field
import groovy.transform.TypeChecked


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
TagTextCore tagText = new TagTextCore()
@Field
List<String> actions = properties['actions'].split(/[, ]+/)
@Field
boolean force = properties['force'] as Boolean
@Field
boolean storeFiles = properties['store.files'] as Boolean

@Field
Map processedStats = [:].withDefault { 0 }
@Field
int batchSize = 1000

@Field
static final FIELD_PROCESSING_STATUS="processing_status"
@Field
static final PROCESSING_STATUS_CLEANSED="cleansed"
@Field
static final PROCESSING_STATUS_LANG_RATED="lang_rated"
@Field
static final PROCESSING_STATUS_TOKENIZED="tokenized"
@Field
static final PROCESSING_STATUS_LEMMATIZED="vesum_lemmatized"

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

cleanText.setLogger(logger)

tagText.setOptions(new TagOptions(singleTokenOnly: true))

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

    if( 'batch.size' in properties ) {
        batchSize = properties['batch.szie'] as int
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
            def orFilters = actions
                .collect { String action ->
                    if( action == "clean" ) action = "cleansed"
                    if( action == "tokenize" ) action = "tokenized"
                    Filters.not(Filters.in(FIELD_PROCESSING_STATUS, [action]))
                }

            def filter = Filters.and(
                Filters.or(
                    orFilters
                ),
                Filters.exists("text", true),
                Filters.ne("text", ""),
                )

//            logger.info "Records to process: " + collection.find(filter).estimatedDocumentCount()
//            logger.info "Records to process: " + collection.countDocuments(filter)

            ExecutorService pool = Executors.newWorkStealingPool()
//            ForkJoinPool pool = ForkJoinPool.commonPool()
   
            for(int ii=0; ; ii++) {
                int prevRecords = records
                logger.info "Processing batch $ii of size $batchSize..."

//                def tasks = []
                List<Future> futures = []
                collection.find(filter)
                  .limit(batchSize)
                  .each { record ->
//                    logger.info "Record: $record"
                    
                    if( validateRecord(record) ) {
                        futures << pool.submit({
                            process(collection, record)
                            records++
                        })
                    }
                }
                
//                logger.info "Futures: $futures"
                
                futures.each { it.get() }
//                logger.info "Tasks: ${tasks.size()}"
//                pool.invokeAll(tasks)
                
                long tm2 = System.currentTimeMillis()
                long time = tm2-tm1
                double speed = (double)records*1000000/time/1000.0
                logger.info "Intermediate stats: time: $time, records: $records, $speed records/s"

                if( records - prevRecords < batchSize ) {
                    logger.info "Processed only ${records-prevRecords}, looks like we're done"
                    break // out of cursor loop
                }
           }
        }

    }
}
catch(Exception e) {
    logger.error "Exception", e
}
finally {
    service.close()
    logger.info "Done processing"
}


boolean validateRecord(record) {
    String text = record.text
    if( ! text ) {
        logger.warn "Empty text for ${record._id}"
        return false
    }

    if( storeFiles ) {
        new File(".files", "${record._id}_text.txt").text = record.text
    }
    
    if( text.size() > 3000000 ) {
        logger.warn "Text too big for ${record._id}, size: ${record.text.size()}"
        return false
    }

    return true
}


def process(MongoCollection collection, record) {

    List updates = []

//    logger.info ">>> task"
//    long tm10 = System.currentTimeMillis()
    
    if( "clean" in actions ) {
        clean(collection, record, updates)
    }

    if( "tokenize" in actions ) {
        tokenize(collection, record, updates)
    }

    if( "lemmatize" in actions ) {
        lemmatize(collection, record, updates)
    }

//    long tm20 = System.currentTimeMillis()
//    logger.info "=== processing time: " + (tm20-tm10)
    
    if( updates ) {
        Bson idFilter = Filters.eq("_id", record._id)
        synchronized(service) {
//            long tm1 = System.currentTimeMillis()
            try {
                UpdateResult updateResult = collection.updateOne(idFilter, Updates.combine(updates))
    //            long tm2 = System.currentTimeMillis()
    //            logger.info "Db update time: " + (tm2-tm1)
                logger.debug "Updated: {}", updateResult
            }
            catch(com.mongodb.MongoTimeoutException e) {
                logger.error("Timeout exception, ignoring...: {}", e.getMessage())
            }
        }
    }
//    logger.info "<<< task"
//    logger.debug "Done with record: $records"
}


// do nlp only if not done before or text is cleaned again
boolean needNlp(record) {
    force || ! record.nlp?.time || \
        (record.clean?.text && record.clean?.time \
            && record.clean.time.after(record.nlp.time) )
}

void tokenize(MongoCollection collection, record, List updates) {
    if( needNlp(record) ) {
        
        String text = record.clean?.text ?: record.text 

        logger.info "Tokenizing ${record._id}, size: ${text.size()}"

        def tokens = tokenizeText.splitWords(text, true)
        def titleTokens = record.title ? tokenizeText.splitWords(record.title, true) : ""

        if( storeFiles && tokens ) {
            new File(".files", "${record._id}_tokens.txt").text = tokens
        }

        updates << Updates.set("nlp.text.tokens", tokens)
        updates << Updates.set("nlp.title.tokens", titleTokens)
        updates << Updates.set("nlp.time", Instant.now())
        updates << Updates.addToSet(FIELD_PROCESSING_STATUS, [PROCESSING_STATUS_TOKENIZED])
//                    // remove old fields
//        updates << Updates.unset("nlp.tokens")
//                    // TEMPORARY: REMOVE: clean fields
//        updates << Updates.unset("nlp.text.lemmas")
//        updates << Updates.unset("nlp.title.lemmas")
        processedStats['tokenize']++
    }
    else {
        logger.info "Skipped tokenize for ${record._id}"
        processedStats['tokenize.skip']++
    }
}

void lemmatize(MongoCollection collection, record, List updates) {
    if( needNlp(record) ) {
        String text = record.clean?.text ?: record.text 

        logger.info "Lemmatizing ${record._id}, size: ${text.size()}"
        
        List<List<TTR>> taggedSentences = tagText.tagTextCore(text, null)

        def lemmas = taggedSentences.collect { it.collect { ttr -> ttr.tokens[0].lemma } }.flatten()

        if( storeFiles && lemmas ) {
            new File(".files", "${record._id}_lemmas.txt").text = lemmas
        }

        updates << Updates.set("nlp.text.lemmas", lemmas)
        updates << Updates.set("nlp.time", Instant.now())
        updates << Updates.addToSet(FIELD_PROCESSING_STATUS, [PROCESSING_STATUS_LEMMATIZED])
        // remove old fields
        updates << Updates.unset("nlp.lemmas")

        if( record.title ) {
            List<List<TTR>> taggedSentences2 = tagText.tagTextCore(record.title, null)
            def lemmas2 = taggedSentences2.collect { it.collect { ttr -> ttr.tokens[0].lemma } }.flatten()

            updates << Updates.set("nlp.title.lemmas", lemmas2)
        }

        processedStats['lemmatize']++
    }
    else {
        logger.info "Skipped lemmatize for ${record._id}"
        processedStats['lemmatize.skip']++
    }
}

String clean(MongoCollection collection, record, List updates) {
    String text = record.text

    if( force || ! record.clean?.time ) {
        logger.info "Cleaning ${record._id}, size: ${text.size()}"

        File nullFile = new File("/dev/null")
        String cleaned = cleanText.cleanUp(text, nullFile, new CleanOptions(), nullFile)
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
        
        if( text != cleaned ) {
            updates << Updates.set("clean.text", cleaned)
            if( record.clean == null ) {
                record.clean = [:]
            }
            record.clean.text = cleaned
        }
        
        if( record.title ) {
            String titleCleaned = cleanText.cleanUp(record.title, nullFile, new CleanOptions(), nullFile)
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
        updates << Updates.addToSet(FIELD_PROCESSING_STATUS, [PROCESSING_STATUS_CLEANSED, PROCESSING_STATUS_LANG_RATED])
        
        if( updates ) {
            processedStats['clean']++
        }
    }
    else {
        logger.info "Skipped clean for ${record._id}"
        processedStats['clean.skip']++
    }
}
