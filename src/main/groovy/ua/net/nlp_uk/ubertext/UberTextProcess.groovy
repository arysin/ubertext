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
        logger.info "Record limit is set to: $limit"
        limit = properties['limit'] as int
    } 

    collections.each { String collectionName ->
        logger.info "Processing collection: ${collectionName}"

        def collection = service.collection(collectionName)
    
        if( ids ) {
            ids.each { id ->
                Bson idFilter = Filters.eq("_id", id)

                collection.find(idFilter).limit(1).each {
                    process(collection, it)
                }
            }
        }
        else if( limit ) {
            collection.find()
                .limit(limit)
                .each {
                    process(collection, it)
                }
        }
        else {
            collection.find()
                .each {
                    process(collection, it)
                }
        }
    }
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
    
    
    Bson idFilter = Filters.eq("_id", record._id)

    if( "clean" in actions ) {
        clean(collection, record)
        if( storeFiles && record.clean?.text ) {
            new File(".files", "${record._id}_clean.txt").text = record.clean.text
        }
    }

    if( "tokenize" in actions ) {
        tokenize(collection, record)
        if( storeFiles && record.nlp?.tokens ) {
            new File(".files", "${record._id}_tokens.txt").text = record.nlp.tokens
        }
    }

    if( "lemmatize" in actions ) {
        lemmatize(collection, record)
        if( storeFiles && record.nlp?.lemmas ) {
            new File(".files", "${record._id}_lemmas.txt").text = record.nlp.lemmas
        }
    }

}

void tokenize(MongoCollection collection, record) {
    if( needNlp() ) {
        
        logger.info "Tokenizing ${record._id}"
        
        String text = record.clean?.text ?: record.text 

        def tokens = tokenizeText.splitWords(text, true)

        Bson updateOperation = Updates.combine(
            Updates.set("nlp.tokens", tokens),
            Updates.set("nlp.time", Instant.now()),
        )
        Bson idFilter = Filters.eq("_id", record._id)
        UpdateResult updateResult = collection.updateOne(idFilter, updateOperation)

        logger.debug "Updated: {}", updateResult
    }
}

// do nlp only if not done before or text is cleaned again
boolean needNlp(record) {
    force || ! record.nlp.time || \
        (record.clean.text && record.clean.time && Instant.parse(record.clean.time).after(Instant.parse(record.nlp.time)) )
}

void lemmatize(MongoCollection collection, record) {
    if( needNlp() ) {
        logger.info "Lemmatizing ${record._id}"

        String text = record.clean?.text ?: record.text 
        
        Analyzed lemmas = lemmatizeText.analyzeText(text)

        Bson updateOperation = Updates.combine(
            Updates.set("nlp.lemmas", lemmas.tagged),
            Updates.set("nlp.time", Instant.now()),
        )
        Bson idFilter = Filters.eq("_id", record._id)
        UpdateResult updateResult = collection.updateOne(idFilter, updateOperation)

        logger.debug "Updated {}", updateResult
    }
}

String clean(MongoCollection collection, record) {
    String text = record.text

    if( force || ! record.clean?.time ) {
        logger.info "Cleaning ${record._id}"
        String cleaned = cleanText.cleanUp(text, new File("/dev/null"), new CleanOptions())

        float ukRate, ruRate
        (ukRate, ruRate) = cleanText.evalChunk(cleaned)
        
        if( ukRate < ruRate ) {
            ruRate = Math.round(ruRate * 100)/100
        }
        
        List updates = []
        
        // remove old fields
//        updates << Updates.unset("text_clean")
//        updates << Updates.unset("clean_text")
        
        if( text != cleaned ) {
            updates << Updates.set("clean.text", cleaned)
            if( record.clean == null ) {
                record.clean = [:]
            }
            record.clean.text = cleaned
        }
        println "uk: $ukRate / ru: $ruRate"
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
        }
    }
}
