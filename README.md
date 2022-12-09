This is a project to use nlp_uk tools to clean, tokenize, and tag/lemmatize texts in ubertext database.
The project is using Groovy.

This project requires JDK >= 17

Configuration for the DB and for actions to perform is in [config/config.properties](config/config.properties)

To run the code:
`./gradlew run`

If you're using snapshot version of nlp_uk you may need to update dependencies occasionally:
`./gradlew --refresh-dependencies run`

For development in Eclipse:
* run `./gradlew eclipse`
* Install Groovy Development Tools from Help/Eclipse Marketplace in Eclipse
* Import as Existing Project in Eclipse IDE
