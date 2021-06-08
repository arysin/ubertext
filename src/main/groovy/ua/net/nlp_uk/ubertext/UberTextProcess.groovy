#!/bin/env groovy

package ua.net.nlp_uk.ubertext

import java.time.Instant

import org.bson.conversions.Bson
import org.nlp_uk.other.CleanText
import org.nlp_uk.other.CleanText.CleanOptions
import org.nlp_uk.tools.LemmatizeText
import org.nlp_uk.tools.TokenizeText
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Updates
import com.mongodb.client.result.UpdateResult

import groovy.transform.Field
import groovy.util.logging.Log


@Field
Logger logger = LoggerFactory.getLogger(getClass())

@Field
Properties properties = loadProperties()

def collections = properties['db.collections'].split(/[, ]+/)

@Field
def service
@Field
CleanText cleanText = new CleanText(new CleanOptions())
@Field
TokenizeText tokenizeText = new TokenizeText(new TokenizeText.TokenizeOptions())
@Field
LemmatizeText lemmatizeText = new LemmatizeText(new LemmatizeText.LemmatizeOptions(firstLemmaOnly: true))
@Field
def actions = properties['actions'].split(/[, ]+/)
@Field
boolean force = properties['force'] as Boolean



private Properties loadProperties() {
    Properties properties = new Properties()
    File propertiesFile = new File('config/config.properties')
    propertiesFile.withInputStream {
        properties.load(it)
    }
    return properties
}

service = new MongoService(properties)

try {
    collections.each { String collectionName ->
        logger.info "Processing collection ${collectionName}"
    
        def collection = service.collection(collectionName)
    
        def id = collectionName == 'fiction' ? "78f15a9926de833faac848d2128e3ba22b2aa288" : "f3cea5a9f0ae2081ef3871e7b50426c05cd51435"
        Bson idFilter = Filters.eq("_id", id)
    
        collection.find(idFilter).limit(1).each {
            process(collection, it)
        }
    }
}
finally {
    service.close()
}

def process(MongoCollection collection, record) {

//    println ":: " + it.text_length
//    new File("text.txt").text = it.text    

    String text = record.text    
    if( ! text ) {
        logger.warn "Empty text for ${record._id}"
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

void tokenize(MongoCollection collection, record) {
    if( force || ! record.nlp.time ) {
        
        logger.info "Tokenizing ${record._id}"
        
        String text = record.clean_text ?: record.text 

        def tokens = tokenizeText.splitWords(text, true)

        Bson updateOperation = Updates.combine(
            Updates.set("nlp.tokens", tokens),
            Updates.set("nlp.time", Instant.now()),
        )
        Bson idFilter = Filters.eq("_id", record._id)
        UpdateResult updateResult = collection.updateOne(idFilter, updateOperation)

        logger.debug updateResult
    }
}

void lemmatize(MongoCollection collection, record) {
    if( force || ! record.nlp.time ) {
        logger.info "Tokenizing ${record._id}"

        String text = record.clean_text ?: record.text 
        
        def tokens = tokenizeText.splitWords(text, true)

        Bson updateOperation = Updates.combine(
            Updates.set("nlp.lemmas", tokens),
            Updates.set("nlp.time", Instant.now()),
        )
        Bson idFilter = Filters.eq("_id", record._id)
        UpdateResult updateResult = collection.updateOne(idFilter, updateOperation)

        logger.debug updateResult
    }
}

String clean(MongoCollection collection, record) {
    String text = record.text

    if( force || ! record.clean.time ) {
        logger.info "Cleaning ${record._id}"
        String cleaned = cleanText.cleanUp(text, new File("/dev/null"), new CleanOptions())

        float ukRate, ruRate
        (ukRate, ruRate) = cleanText.evalChunk(cleaned)
        
        if( ukRate < ruRate ) {
            ruRate = Math.round(ruRate * 100)/100
        }
        
        List updates = []
        if( text != cleaned ) {
            updates << Updates.set("clean.text", cleaned)
            updates << Updates.set("clean.time", Instant.now())
            record.text_clean = cleaned
        }
        println "uk: $ukRate / ru: $ruRate"
        if( ukRate < ruRate ) {
            updates << Updates.set("clean.ru_rate", ruRate)
            updates << Updates.set("clean.uk_rate", ukRate)
        }

        if( updates ) {
            Bson idFilter = Filters.eq("_id", record._id)
            Bson updateOperation = Updates.combine(updates)
            UpdateResult updateResult = collection.updateOne(idFilter, updateOperation)

            logger.debug updateResult
        }
    }
}
