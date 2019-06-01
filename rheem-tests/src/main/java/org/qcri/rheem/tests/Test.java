package org.qcri.rheem.tests;

import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.util.RheemUUID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class Test {


    public static void main(String... args) {

        RheemUUID uuid = RheemUUID.randomUUID();

        System.out.println(uuid);
        System.out.println(Arrays.toString(uuid.tobyte()));

        RheemUUID child = uuid.createChild();
        System.out.println(child);
        System.out.println(Arrays.toString(child.tobyte()));

        RheemUUID child2 = child.createChild();
        System.out.println(child2);
        System.out.println(Arrays.toString(child2.tobyte()));

        RheemUUID child3 = child.createChild();
        System.out.println(child3);
        System.out.println(Arrays.toString(child3.tobyte()));


        RheemUUID child4 = child3.createChild();
        System.out.println(child4);
        System.out.println(Arrays.toString(child4.tobyte()));
    }
}
