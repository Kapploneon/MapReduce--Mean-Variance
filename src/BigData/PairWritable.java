package BigData;

import java.io.*;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


public class PairWritable implements Writable{
    private long num;
    private long numsq;

    public PairWritable(){
    }

    public void write(DataOutput out) throws IOException{
        out.writeLong(num);
        out.writeLong(numsq);
    }

    public void readFields(DataInput in) throws IOException{
        num = in.readLong();
        numsq = in.readLong();
    }

    public long getNum(){
        return num;
    }

    public long getNumsq(){
        return numsq;
    }

    public void set(long num, long numsq){
        this.num = num;
        this.numsq = numsq;
    }
}
