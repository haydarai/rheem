package org.qcri.rheem.basic.collection;

/**
 * Created by bertty on 12-11-17.
 */
public enum MultiplexStatus {
    //waiting for the it will arrive of the first element
    WAITING,
    //the first element is arrived but is posible that the consume is more fast that the arrived, then
    //if the thread consume all the collection need wait for Time the asked
    RUNNING,
    //the input is finish for futures arrive but, the process is necesary that continues.
    FINISH_INPUT,
    //the interuption is the more fast possible and not data not processing is not important
    KILLED;
    //**Not start the processig, for the continues without delete elements
    //**NOT_PROCESSING,
    //**processing is for delete the element that is ready for all iterator then is possible delete the element;
    //**PROCESSING;
}
