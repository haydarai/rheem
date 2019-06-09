package org.qcri.rheem.debug;

import org.apache.tomcat.util.codec.binary.Base64;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@RestController
@RequestMapping("/debug")
public class DebugController {

    private static String status="pause";
    private static int counter = 0;
    private static Queue<String> store = new ConcurrentLinkedQueue<>();

    @RequestMapping(value = "/", method = RequestMethod.GET, produces="text/plain")
    @ResponseBody
    public String getStatus() {
        //Todo return the status
        return status;
    }


    @RequestMapping(value = "/stop", method = RequestMethod.GET, produces="text/plain")
    public String stopProcessing() {
        status = "stop";
        return status;
    }

    @RequestMapping(value = "/pause", method = RequestMethod.GET, produces="text/plain")
    public String pauseProcessing() {
        status = "pause";
        return status;
    }

    @RequestMapping(value = "/resume", method = RequestMethod.GET, produces="text/plain")
    public String resumeProcessing() {
        status = "resume";
        return status;
    }
//application/octet-stream
    @RequestMapping(value = "/add", method = RequestMethod.POST, produces="application/x-www-form-urlencoded", consumes = "application/x-www-form-urlencoded")
    public String addProcessing() {
        counter++;
        //System.out.println("adding");
        long input_time = System.currentTimeMillis();
       /* printArrayByte(payload, "payload");
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        buffer.putLong(input_time);
        counter++;
        buffer.putLong(System.currentTimeMillis());
        byte[] output = buffer.array();
        printArrayByte(output, "outout");
        return output;*/
        return String.format("%d %d", input_time, System.currentTimeMillis());
    }

    @RequestMapping(value = "/store", method = RequestMethod.POST, produces="*/*", consumes = "application/x-www-form-urlencoded")
    public String storeProcessing(@RequestBody String payload) {
        //System.out.println("storing");
        //store.add(payload);
        return "";
    }

    @RequestMapping(value = "/show", method = RequestMethod.POST, produces="*/*", consumes = "*/*")
    public String showStoreProcessing() {
        StringBuilder builder = new StringBuilder();
        long sum_ida = 0;
        long sum_vuelta = 0;

        int element = 0;
        builder.append(
            "salida_plan llegada_multiplex salida_multiplex llegada_plan tiempo_ida tiempo_vuelta\n"
        );
        while(!store.isEmpty()) {
            /*ByteBuffer buffer = ByteBuffer.wrap(store.poll());
            buffer.asReadOnlyBuffer();
            byte[] a = buffer.array();
            long llegada_plan = buffer.getLong();
            long llegada_multiplex = buffer.getLong();
            long salida_multiplex = buffer.getLong();
            long salida_plan = buffer.getLong();


            long tiempo_ida = (llegada_multiplex - salida_plan);
            long tiempo_vuelta = (llegada_plan - salida_multiplex);

            StringBuilder sb = new StringBuilder(a.length * 2);
            for (int i = 0; i < a.length; i++)
                sb.append(Integer.toBinaryString(a[i])+ "  ");

            builder.append(
                String.format(
                    "%d %d %d %d %d %d %s\n",
                    salida_plan,
                    llegada_multiplex,
                    salida_multiplex,
                    llegada_plan,
                    tiempo_ida,
                    tiempo_vuelta,
                    sb.toString()
                )
            );
            sum_ida += tiempo_ida;
            sum_vuelta += tiempo_vuelta;
            element++;*/
            builder.append(store.poll()+"\n");
        }
        try {
            builder.append(
                    String.format(
                            "Tiempo_ida_avg = %f\nTiempo_vuelta_avg = %f\n Elementos = %d",
                            (double) (sum_ida / element),
                            (double) (sum_vuelta / element),
                            element
                    )
            );
        }catch (Exception e){
            builder.append(e.getMessage());
        }
        return builder.toString();
    }


    @RequestMapping(value = "/reduce", method = RequestMethod.POST, produces="text/plain", consumes = "*/*")
    public String reduceProcessing() {
        return "";
    }


    @RequestMapping(value = "/counter", method = RequestMethod.POST, produces="text/plain", consumes = "*/*")
    public String getCounterProcessing() {
        return String.valueOf(counter);
    }

    @RequestMapping(value = "/counterTo0", method = RequestMethod.POST, produces="text/plain", consumes = "*/*")
    public String setCounterIn0Processing() {
        counter = 0;
        return "";
    }

    private void printArrayByte(byte[] a, String comment){
        StringBuilder sb = new StringBuilder(a.length * 2+ comment.length());
        sb.append(comment+" : ");
        for (int i = 0; i < a.length; i++)
            sb.append("("+i+")"+getByteBinaryString(a[i])+ "  ");
        System.out.println(sb.toString());
    }

    private static String getByteBinaryString(byte b) {
        StringBuilder sb = new StringBuilder();
        for (int i = 7; i >= 0; --i) {
            sb.append(b >>> i & 1);
        }
        return sb.toString();
    }

}
