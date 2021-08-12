import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TradeJsonDeserializer /*implements Deserializer<Trade>*/ {

    private final ObjectMapper objectMapper = new ObjectMapper();

//    @Override
//    public void configure(Map<String, ?> configs, boolean isKey) {
//    }
//
//    @Override
//    public Trade deserialize(String topic, byte[] data) {
//        if (data == null) {
//            return null;
//        }
//
//        try {
//            return objectMapper.readValue(data, Trade.class);
//        }
//        catch (Exception e) {
//            throw new SerializationException(e);
//        }
//    }
//
//    @Override
//    public void close() {
//
//    }

}
