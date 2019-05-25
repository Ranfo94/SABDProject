package producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class Producer {

    public static void runProducer(Dataset<Row> dataset, String topic) {
        org.apache.kafka.clients.producer.Producer<String, String> producer = ProducerCreator.createProducer();

        try {

            List<Row> list = dataset.collectAsList();
            int i = 0;
            while (i<list.size()){
                String row = list.get(i).toString();
                sendMessage(producer,i+"", row, topic);
                ++i;
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    private static void sendMessage(org.apache.kafka.clients.producer.Producer<String, String> producer, String key, String value, String topic){
        try {
            producer.send(
                    new ProducerRecord<String, String>(topic, key, value))
                    .get();
            //System.out.println("Sent message: (" + key + ", " + value + ")");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }


    public static void sendToHbase(List<String> list, String topic) {

        org.apache.kafka.clients.producer.Producer<String, String> producer = ProducerCreator.createProducer();

        try {
            int i;
            for(i=0; i<list.size(); i++){
                String row = list.get(i);
                sendMessage(producer,i+"", row, topic);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
