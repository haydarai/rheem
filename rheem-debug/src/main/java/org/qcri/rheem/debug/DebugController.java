package org.qcri.rheem.debug;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/debug")
public class DebugController {

    private static String status="pause";

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
    @RequestMapping(value = "/add", method = RequestMethod.POST, produces="text/plain", consumes = "*/*")
    public String addProcessing() {
        return "";
    }

    @RequestMapping(value = "/reduce", method = RequestMethod.POST, produces="text/plain", consumes = "*/*")
    public String reduceProcessing() {
        return "";
    }

}
