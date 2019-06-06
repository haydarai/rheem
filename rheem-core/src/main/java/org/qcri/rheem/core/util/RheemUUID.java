package org.qcri.rheem.core.util;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

public class RheemUUID implements java.io.Serializable, Comparable<RheemUUID> {

    private static int START_SIZE = 1;
    /*
     * The most significant 64 bits of this UUID.
     *
     * @serial
     */
    private final long mostSigBits;

    /*
     * The least significant 64 bits of this UUID.
     *
     * @serial
     */
    private final long leastSigBits;

    private int correlative[] = null;
    private int n_children = 0;
    public transient byte[] bytes = null;

    /*
     * The random number generator used by this class to create random
     * based UUIDs. In a holder class to defer initialization until needed.
     */
    private static class Holder {
        static final Random numberGenerator = new Random();
    }

    private RheemUUID(byte[] data) {
        long msb = 0;
        long lsb = 0;
       // assert data.length == 16 : "data must be 16 bytes in length";
        for (int i=0; i<8; i++)
            msb = (msb << 8) | (data[i] & 0xff);
        for (int i=8; i<16; i++)
            lsb = (lsb << 8) | (data[i] & 0xff);
        this.mostSigBits = msb;
        this.leastSigBits = lsb;
        this.bytes = null;
    }

    public RheemUUID(long mostSigBits, long leastSigBits) {
        this.mostSigBits = mostSigBits;
        this.leastSigBits = leastSigBits;
        this.bytes = null;
    }

    public static RheemUUID randomUUID() {
        Random ng = RheemUUID.Holder.numberGenerator;

        byte[] randomBytes = new byte[16];
        ng.nextBytes(randomBytes);
        randomBytes[6]  &= 0x0f;  /* clear version        */
        randomBytes[6]  |= 0x40;  /* set to version 4     */
        randomBytes[8]  &= 0x3f;  /* clear variant        */
        randomBytes[8]  |= 0x80;  /* set to IETF variant  */
        return new RheemUUID(randomBytes);

//        return new RheemUUID(1, 2);
    }

    public static RheemUUID nameUUIDFromBytes(byte[] name) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException nsae) {
            throw new InternalError("MD5 not supported", nsae);
        }
        byte[] md5Bytes = md.digest(name);
        md5Bytes[6]  &= 0x0f;  /* clear version        */
        md5Bytes[6]  |= 0x30;  /* set to version 3     */
        md5Bytes[8]  &= 0x3f;  /* clear variant        */
        md5Bytes[8]  |= 0x80;  /* set to IETF variant  */
        return new RheemUUID(md5Bytes);
    }

    public static RheemUUID fromString(String name) {
        String[] components = name.split("-");
        if (components.length != 5)
            throw new IllegalArgumentException("Invalid UUID string: "+name);
        for (int i=0; i<5; i++)
            components[i] = "0x"+components[i];

        long mostSigBits = Long.decode(components[0]).longValue();
        mostSigBits <<= 16;
        mostSigBits |= Long.decode(components[1]).longValue();
        mostSigBits <<= 16;
        mostSigBits |= Long.decode(components[2]).longValue();

        long leastSigBits = Long.decode(components[3]).longValue();
        leastSigBits <<= 48;
        leastSigBits |= Long.decode(components[4]).longValue();

        return new RheemUUID(mostSigBits, leastSigBits);
    }

    // Field Accessor Methods

    /**
     * Returns the least significant 64 bits of this UUID's 128 bit value.
     *
     * @return  The least significant 64 bits of this UUID's 128 bit value
     */
    public long getLeastSignificantBits() {
        return leastSigBits;
    }

    /**
     * Returns the most significant 64 bits of this UUID's 128 bit value.
     *
     * @return  The most significant 64 bits of this UUID's 128 bit value
     */
    public long getMostSignificantBits() {
        return mostSigBits;
    }

    /**
     * The version number associated with this {@code UUID}.  The version
     * number describes how this {@code UUID} was generated.
     *
     * The version number has the following meaning:
     * <ul>
     * <li>1    Time-based UUID
     * <li>2    DCE security UUID
     * <li>3    Name-based UUID
     * <li>4    Randomly generated UUID
     * </ul>
     *
     * @return  The version number of this {@code UUID}
     */
    public int version() {
        // Version is bits masked by 0x000000000000F000 in MS long
        return (int)((mostSigBits >> 12) & 0x0f);
    }

    public int variant() {
        // This field is composed of a varying number of bits.
        // 0    -    -    Reserved for NCS backward compatibility
        // 1    0    -    The IETF aka Leach-Salz variant (used by this class)
        // 1    1    0    Reserved, Microsoft backward compatibility
        // 1    1    1    Reserved for future definition.
        return (int) ((leastSigBits >>> (64 - (leastSigBits >>> 62)))
                & (leastSigBits >> 63));
    }

    public long timestamp() {
        if (version() != 1) {
            throw new UnsupportedOperationException("Not a time-based UUID");
        }

        return (mostSigBits & 0x0FFFL) << 48
                | ((mostSigBits >> 16) & 0x0FFFFL) << 32
                | mostSigBits >>> 32;
    }

    /**
     * The clock sequence value associated with this UUID.
     *
     * <p> The 14 bit clock sequence value is constructed from the clock
     * sequence field of this UUID.  The clock sequence field is used to
     * guarantee temporal uniqueness in a time-based UUID.
     *
     * <p> The {@code clockSequence} value is only meaningful in a time-based
     * UUID, which has version type 1.  If this UUID is not a time-based UUID
     * then this method throws UnsupportedOperationException.
     *
     * @return  The clock sequence of this {@code UUID}
     *
     * @throws  UnsupportedOperationException
     *          If this UUID is not a version 1 UUID
     */
    public int clockSequence() {
        if (version() != 1) {
            throw new UnsupportedOperationException("Not a time-based UUID");
        }

        return (int)((leastSigBits & 0x3FFF000000000000L) >>> 48);
    }

    /**
     * The node value associated with this UUID.
     *
     * <p> The 48 bit node value is constructed from the node field of this
     * UUID.  This field is intended to hold the IEEE 802 address of the machine
     * that generated this UUID to guarantee spatial uniqueness.
     *
     * <p> The node value is only meaningful in a time-based UUID, which has
     * version type 1.  If this UUID is not a time-based UUID then this method
     * throws UnsupportedOperationException.
     *
     * @return  The node value of this {@code UUID}
     *
     * @throws  UnsupportedOperationException
     *          If this UUID is not a version 1 UUID
     */
    public long node() {
        if (version() != 1) {
            throw new UnsupportedOperationException("Not a time-based UUID");
        }

        return leastSigBits & 0x0000FFFFFFFFFFFFL;
    }

    // Object Inherited Methods

    /**
     * Returns a {@code String} object representing this {@code UUID}.
     *
     * <p> The UUID string representation is as described by this BNF:
     * <blockquote><pre>
     * {@code
     * UUID                   = <time_low> "-" <time_mid> "-"
     *                          <time_high_and_version> "-"
     *                          <variant_and_sequence> "-"
     *                          <node>
     * time_low               = 4*<hexOctet>
     * time_mid               = 2*<hexOctet>
     * time_high_and_version  = 2*<hexOctet>
     * variant_and_sequence   = 2*<hexOctet>
     * node                   = 6*<hexOctet>
     * hexOctet               = <hexDigit><hexDigit>
     * hexDigit               =
     *       "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
     *       | "a" | "b" | "c" | "d" | "e" | "f"
     *       | "A" | "B" | "C" | "D" | "E" | "F"
     * }</pre></blockquote>
     *
     * @return  A string representation of this {@code UUID}
     */
    public String toString() {
        /*return (digits(mostSigBits >> 32, 8) + "-" +
                digits(mostSigBits >> 16, 4) + "-" +
                digits(mostSigBits, 4) + "-" +
                digits(leastSigBits >> 48, 4) + "-" +
                digits(leastSigBits, 12)) +
                ( (this.correlative != null)?
                    Arrays.stream(this.correlative)
                        .filter(ele -> ele != -1)
                        .mapToObj(ele -> { return "-"+ele;})
                        .collect(Collectors.joining( "" )):
                    ""
                )
                ;*/
        StringBuilder builder = new StringBuilder(50);
        builder.append(this.mostSigBits);
        builder.append("-");
        builder.append(this.leastSigBits);

        if(this.correlative != null) {
            for (int i = 0; i < this.correlative.length; i++) {
                builder.append("-");
                builder.append(this.correlative[i]);
            }
        }
        return builder.toString();
    }

    /** Returns val represented by the specified number of hex digits. */
    private static String digits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }

    /**
     * Returns a hash code for this {@code UUID}.
     *
     * @return  A hash code value for this {@code UUID}
     */
    public int hashCode() {
        long hilo = mostSigBits ^ leastSigBits;
        return ((int)(hilo >> 32)) ^ (int) hilo;
    }

    /**
     * Compares this object to the specified object.  The result is {@code
     * true} if and only if the argument is not {@code null}, is a {@code UUID}
     * object, has the same variant, and contains the same value, bit for bit,
     * as this {@code UUID}.
     *
     * @param  obj
     *         The object to be compared
     *
     * @return  {@code true} if the objects are the same; {@code false}
     *          otherwise
     */
    public boolean equals(Object obj) {
        if ((null == obj) || (obj.getClass() != RheemUUID.class))
            return false;
        RheemUUID id = (RheemUUID)obj;
        return (mostSigBits == id.mostSigBits &&
                leastSigBits == id.leastSigBits);
    }

    // Comparison Operations

    /**
     * Compares this UUID with the specified UUID.
     *
     * <p> The first of two UUIDs is greater than the second if the most
     * significant field in which the UUIDs differ is greater for the first
     * UUID.
     *
     * @param  val
     *         {@code UUID} to which this {@code UUID} is to be compared
     *
     * @return  -1, 0 or 1 as this {@code UUID} is less than, equal to, or
     *          greater than {@code val}
     *
     */
    public int compareTo(RheemUUID val) {
        // The ordering is intentionally set up so that the UUIDs
        // can simply be numerically compared as two numbers
        return (this.mostSigBits < val.mostSigBits ? -1 :
                (this.mostSigBits > val.mostSigBits ? 1 :
                        (this.leastSigBits < val.leastSigBits ? -1 :
                                (this.leastSigBits > val.leastSigBits ? 1 :
                                        0))));
    }

    public RheemUUID createChild(){
        RheemUUID rheemUUID = new RheemUUID(this.mostSigBits, this.leastSigBits);
        if(this.correlative != null) {
            rheemUUID.correlative = new int[this.correlative.length + 1];
            System.arraycopy(this.correlative, 0, rheemUUID.correlative, 0, this.correlative.length);
        }else{
            rheemUUID.correlative = new int[1];
        }
        rheemUUID.correlative[rheemUUID.correlative.length-1] = ++this.n_children;
        return rheemUUID;
    }

    public byte[] tobyte(){
        if(this.bytes == null) {
            //int size = (Long.BYTES * 2) + ((this.correlative != null)?(Integer.BYTES * this.correlative.length): 0) ;
            int size = 100 ;
            byte[] result = new byte[size];

            long tmp = this.mostSigBits;
            result[7] = (byte)((int)(tmp));
            result[6] = (byte)((int)(tmp >>= 8));
            result[5] = (byte)((int)(tmp >>= 8));
            result[4] = (byte)((int)(tmp >>= 8));
            result[3] = (byte)((int)(tmp >>= 8));
            result[2] = (byte)((int)(tmp >>= 8));
            result[1] = (byte)((int)(tmp >>= 8));
            result[0] = (byte)((int)(tmp >> 8));
            tmp = this.leastSigBits;
            result[15] = (byte)((int)(tmp));
            result[14] = (byte)((int)(tmp >>= 8));
            result[13] = (byte)((int)(tmp >>= 8));
            result[12] = (byte)((int)(tmp >>= 8));
            result[11] = (byte)((int)(tmp >>= 8));
            result[10] = (byte)((int)(tmp >>= 8));
            result[9] = (byte)((int)(tmp >>= 8));
            result[8] = (byte)((int)(tmp >> 8));
            //   System.arraycopy(Longs.toByteArray(this.mostSigBits), 0, result, 0, Long.BYTES);
            //  System.arraycopy(Longs.toByteArray(this.leastSigBits), 0, result, Long.BYTES, Long.BYTES);
            if (this.correlative != null) {
                //int position = (Long.BYTES * 2);
                int position = 16;
                for (int i = 0; i < this.correlative.length; i++) {
                    //  System.arraycopy(Ints.toByteArray(this.correlative[i]), 0, result, position, Integer.BYTES);
                    // position += Integer.BYTES;
                    int current = this.correlative[i];
                    result[position+3] = (byte) (current );
                    result[position+2] = (byte) (current >>= 8);
                    result[position+1] = (byte) (current >>= 8);
                    result[position] = (byte) (current >> 8);
                    position += 4;
                }
            }
            this.bytes = result;
        }
        return this.bytes;
    }

}
