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
final String STORAGE_DIR = "../ubertext_tmp/txt"
@Field
def emptyCountMap = [:].withDefault { 0 } 

def collections = ['news'] //properties['db.collections'].split(/[, ]+/)

@Field
MongoService service


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
    def sources = [
//'hmarochos_kiev_ua',
//'uanews_dp_ua',
//'babel_ua',
//'nv_ua',
//'epravda_com_ua',
// 'mil_in_ua',
//'ye_ua',

//'news_lugansk_ua',
//'zhitomir_info',
//'life_pravda_com_ua',
//'zaxid_net',

//'eurointegration_com_ua',
//'zn_ua',
//'pravda_com_ua',
//'umoloda_kyiv_ua',
//'nashigroshi_org',
//'wz_lviv_ua'

//'news_liga_net', //'ua_news_liga_net',
        
//'tabloid_pravda_com_ua',
//'unian_ua',
//'lb_ua',
//'hromadske_ua',
//'mpz_brovary_org', // dead
//'pik_cn_ua', // dead
//'procherk_info',
//'ua_korrespondent_net', // todo
]
    
    collections.each { String collectionName ->
        logger.info "Processing collection: ${collectionName}"
    
        def collection = service.collection(collectionName)
    
        sources.each { source ->
            logger.info "Processing source: ${source}"
            
            File folder = new File("$STORAGE_DIR/${source}")
            folder.mkdirs()
//            new File(folder, "empty.lst").delete()
        
            def idFilter = Filters.regex("source_info.slug", ".*${source}.*")
            
            Bson filter = source == 'news_lugansk_ua' ? idFilter 
            :
             Filters.and(
                idFilter,
//                Filters.regex('date_of_publish', "202[0-9].*")
//                Filters.regex('{ $year: date_of_publish }', "202[0-9]")
//                Filters.gte('date_of_publish', LocalDate.parse("2001-01-01")),
//                Filters.lt('date_of_publish', LocalDate.parse("2020-01-01"))
                )
                

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

