package net.javamicro.springboot;

import net.javamicro.springboot.entity.WikimediaData;
import net.javamicro.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {

    private WikimediaDataRepository wikimediaDataRepository;
    private static final Logger LOGGER= LoggerFactory.getLogger(KafkaDatabaseConsumer.class);
    public KafkaDatabaseConsumer(WikimediaDataRepository wikimediaDataRepository) {
        this.wikimediaDataRepository = wikimediaDataRepository;
    }
    @KafkaListener(topics = "wikimedia_recentchange", groupId = "myGroup")
    public void consume(String eventMessage) {
        LOGGER.info(String.format("Message received -> %s", eventMessage));
        WikimediaData wikimediaData= new WikimediaData();
        wikimediaData.setWikiEventData(eventMessage.substring(5,30)); //used substring to limit the long of data, so it will not throw exception
        wikimediaDataRepository.save(wikimediaData);
    }


}
