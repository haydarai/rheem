package org.qcri.rheem.debug;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.javanet.NetHttpTransport;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/pause")
public class PauseController {
    static final String[] ips = {
        "10.4.4.43",
        "10.4.4.47",
        "10.4.4.38",
        "10.4.4.42",
        "10.4.4.54",
        "10.4.4.40",
        "10.4.4.45",
        "10.4.4.65",
        "10.4.4.39",
        "10.4.4.44"
    };

    static final String[] port = {
        "8080",
        "8081",
        "8082",
        "8083"
    };

    @RequestMapping(value = "/execute", method = RequestMethod.GET, produces="text/plain")
    @ResponseBody
    public String executePause() {
        long stamp = System.currentTimeMillis();
        return String.valueOf(stamp);
    }

    @RequestMapping(value = "/globalPause", method = RequestMethod.POST, produces="*/*", consumes = "text/plain")
    public String executeGlobal(@RequestBody String payload){
        StringBuffer buffer = new StringBuffer();
        Executor pool = Executors.newFixedThreadPool(ips.length*port.length);

        int maxim = Integer.parseInt(payload);
        int count = 0;
        for(int j = 0; j < port.length; j++) {
            for (int i = 0; i < ips.length; i++) {
                if(count <= maxim) {
                    pool.execute(new RequestRunnable(ips[i], port[j], buffer));
                }
                count++;
            }
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return buffer.toString();
    }

    public class RequestRunnable implements Runnable {

        private String ip, port;
        private StringBuffer buffer;

        public RequestRunnable(String ip, String port, StringBuffer buffer){
            this.ip = ip;
            this.port = port;
            this.buffer = buffer;
        }

        @Override
        public void run() {
            URI uri = URI.create(String.format("http://%s:%s/pause/execute", ip, port));
            try {
                long start_point = System.currentTimeMillis();
                InputStream request = new NetHttpTransport()
                        .createRequestFactory()
                        .buildGetRequest(new GenericUrl(uri))
                        .execute()
                        .getContent();

                String content = new BufferedReader(new InputStreamReader(request)).lines().collect(
                        Collectors.joining(
                                " "
                        )
                );

                long end_point = Long.parseLong(content);
                long duration = end_point - start_point;
                this.buffer
                        .append(
                            String.format(
                                "%d %d %d \n",
                                start_point,
                                end_point,
                                duration
                            )
                        );

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}


