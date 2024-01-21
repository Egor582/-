import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;

import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CrptApi {
    private static final String API_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";

    private final TimeUnit timeUnit;
    private final int requestLimit;
    private final Lock lock;
    private final Condition condition;
    private final AtomicInteger requestsCount;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.timeUnit = timeUnit;
        this.requestLimit = requestLimit;
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
        this.requestsCount = new AtomicInteger(0);
        this.httpClient = HttpClientBuilder.create().build();
        this.objectMapper = new ObjectMapper();
    }

    public void createDocument(Object document, String signature) throws Exception {
        try {
            lock.lock();
            while (requestsCount.get() >= requestLimit) {
                condition.await();
            }
            requestsCount.incrementAndGet();
        } finally {
            lock.unlock();
        }

        try {
            String jsonDocument = objectMapper.writeValueAsString(document);

            HttpPost request = new HttpPost(API_URL);
            request.setEntity(new StringEntity(jsonDocument));

            CloseableHttpResponse response = httpClient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            response.close();

            if (statusCode != 200) {
                throw new Exception("Failed to create document: " + statusCode);
            }
        } finally {
            int count = requestsCount.decrementAndGet();
            if (count == 0) {
                lock.lock();
                try {
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
    }
}