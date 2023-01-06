package ua.net.nlp_uk.ubertext.extract

import groovy.transform.CompileStatic
import groovy.transform.Field
import org.bson.conversions.Bson

@CompileStatic
class Collections {

    static Map collections = ['news':
        [
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

            // new from news
            //'ua_korrespondent_net',
//            'forbes_ua',
//                    'agravery_com',
//                    'ukrainian_voanews_com',
//                    'bukinfo_com_ua', 'gre4ka_info', 'uanews_kharkiv_ua',
//                    'itc_ua',
//                    'ukrinform_ua',
//                    'censor_net',
//                    'dou_ua'
        ],

//        'fiction': ['ukrlib_com_ua', 'javalibre_com_ua'],
//        'wikipedia': ['uk_wikipedia_org'],
//        'court': ['od_reyestr_court_gov_ua'],
//        'forum': ['dou_ua_forum']
    ]

    static File getFolder(String collectionName, String source) {
        String STORAGE_DIR = "../ubertext_tmp/txt/$collectionName"

        File folder = new File("$STORAGE_DIR/${source}")
        folder.mkdirs()
        return folder
    }

    static Bson filters = null
    // Filters.and(
    //                Filters.regex('date_of_publish', "202[0-9].*")
    //                Filters.regex('{ $year: date_of_publish }', "202[0-9]")
    //                Filters.gte('date_of_publish', LocalDate.parse("2022-06-01")),
    //                Filters.lt('date_of_publish', LocalDate.parse("2020-01-01"))
    //    )

}