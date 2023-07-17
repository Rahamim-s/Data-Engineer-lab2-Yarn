package com.opstty.job;// Importez les packages nécessaires
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

// Définissez la classe TreeWritable qui contiendra les informations de l'âge et de l'arrondissement
public class TreeWritable implements Writable {
    private int age;
    private int district;

    public TreeWritable() {
    }

    public TreeWritable(int age, int district) {
        this.age = age;
        this.district = district;
    }

    public int getAge() {
        return age;
    }

    public int getDistrict() {
        return district;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(age);
        dataOutput.writeInt(district);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        age = dataInput.readInt();
        district = dataInput.readInt();
    }

    public void setDistrict(int district) {
        this.district = district;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
